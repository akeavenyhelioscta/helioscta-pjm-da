import { NextResponse } from "next/server";
import { query } from "@/lib/db";

export const dynamic = "force-dynamic";

const DATA_SQL = `
  WITH pjm_load AS (
    SELECT DISTINCT
      date,
      hour_ending,
      region,
      rt_load_mw
    FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_rt_metered_hourly
    WHERE region = $1
      AND date >= $2::date
      AND date <= $3::date
  )
  SELECT * FROM pjm_load
  ORDER BY date DESC, hour_ending ASC
`;

const REGIONS_SQL = `
  SELECT DISTINCT region
  FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_rt_metered_hourly
  ORDER BY region
`;

function toISODate(s: string | null): string | null {
  if (!s) return null;
  const d = new Date(s);
  if (isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const region = searchParams.get("region") || "RTO";

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
    const [dataRes, regionsRes] = await Promise.all([
      query(DATA_SQL, [region, startDate, endDate]),
      query(REGIONS_SQL),
    ]);

    return NextResponse.json(
      {
        rows: dataRes.rows,
        regions: regionsRes.rows.map((r) => (r as { region: string }).region),
      },
      {
        headers: {
          "Cache-Control": "public, s-maxage=300, stale-while-revalidate=60",
        },
      }
    );
  } catch (error) {
    console.error("[pjm-load-rt-metered-hourly] DB query failed:", error);
    return NextResponse.json(
      { error: "Failed to fetch PJM load data" },
      { status: 500 }
    );
  }
}
