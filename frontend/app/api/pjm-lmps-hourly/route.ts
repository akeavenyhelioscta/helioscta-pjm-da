import { NextResponse } from "next/server";
import { query } from "@/lib/db";

export const dynamic = "force-dynamic";

const DATA_SQL = `
  WITH pjm_lmps_hourly AS (
    SELECT DISTINCT
      date,
      hour_ending,
      hub,
      market,
      lmp_total,
      lmp_system_energy_price,
      lmp_congestion_price,
      lmp_marginal_loss_price
    FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_lmps_hourly
    WHERE hub = $1
      AND market = $2
      AND date >= $3::date
      AND date <= $4::date
  )
  SELECT * FROM pjm_lmps_hourly
  ORDER BY date DESC, hour_ending ASC, hub
`;

const HUBS_SQL = `
  SELECT DISTINCT hub
  FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_lmps_hourly
  ORDER BY hub
`;

const MARKETS_SQL = `
  SELECT DISTINCT market
  FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_lmps_hourly
  ORDER BY market
`;

function toISODate(s: string | null): string | null {
  if (!s) return null;
  const d = new Date(s);
  if (isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const hub = searchParams.get("hub") || "WESTERN HUB";
  const market = searchParams.get("market") || "da";

  const tomorrow = new Date();
  tomorrow.setDate(tomorrow.getDate() + 1);
  const defaultEnd = tomorrow.toISOString().slice(0, 10);
  const endDate = toISODate(searchParams.get("end")) ?? defaultEnd;

  let startDate = toISODate(searchParams.get("start"));
  if (!startDate) {
    const daysRaw = parseInt(searchParams.get("days") || "100", 10);
    const days = Number.isFinite(daysRaw) && daysRaw > 0 ? Math.min(daysRaw, 730) : 100;
    const d = new Date(endDate);
    d.setDate(d.getDate() - days);
    startDate = d.toISOString().slice(0, 10);
  }

  try {
    const [dataRes, hubsRes, marketsRes] = await Promise.all([
      query(DATA_SQL, [hub, market, startDate, endDate]),
      query(HUBS_SQL),
      query(MARKETS_SQL),
    ]);

    return NextResponse.json(
      {
        rows: dataRes.rows,
        hubs: hubsRes.rows.map((r) => (r as { hub: string }).hub),
        markets: marketsRes.rows.map((r) => (r as { market: string }).market),
      },
      {
        headers: {
          "Cache-Control": "public, s-maxage=300, stale-while-revalidate=60",
        },
      }
    );
  } catch (error) {
    console.error("[pjm-lmps-hourly] DB query failed:", error);
    return NextResponse.json(
      { error: "Failed to fetch PJM LMP hourly data" },
      { status: 500 }
    );
  }
}
