import { NextResponse } from "next/server";
import { query } from "@/lib/db";

export const dynamic = "force-dynamic";

const DATA_SQL = `
  SELECT DISTINCT
    forecast_rank,
    forecast_execution_datetime,
    forecast_execution_date,
    forecast_datetime,
    forecast_date AS date,
    hour_ending,
    region,
    forecast_load_mw
  FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_forecast_hourly
  WHERE region = $1
    AND forecast_execution_date = ANY(string_to_array($2, ',')::date[])
  ORDER BY forecast_datetime ASC
`;

const REGIONS_SQL = `
  SELECT DISTINCT region
  FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_forecast_hourly
  ORDER BY region
`;

const EXEC_DATES_SQL = `
  SELECT DISTINCT forecast_execution_date
  FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_forecast_hourly
  WHERE region = $1
    AND forecast_rank = 1
  ORDER BY forecast_execution_date DESC
  LIMIT 14
`;

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const region = searchParams.get("region") || "RTO_COMBINED";
  const execDatesParam = searchParams.get("exec_dates");

  try {
    const [regionsRes, execDatesRes] = await Promise.all([
      query(REGIONS_SQL),
      query(EXEC_DATES_SQL, [region]),
    ]);

    const availableExecDates = execDatesRes.rows.map((r) => {
      const val = (r as { forecast_execution_date: string }).forecast_execution_date;
      return typeof val === "string" ? val.slice(0, 10) : new Date(val).toISOString().slice(0, 10);
    });

    let execDates: string[];
    if (execDatesParam) {
      execDates = execDatesParam.split(",").map((s) => s.trim()).filter(Boolean);
    } else {
      execDates = availableExecDates.slice(0, 3);
    }

    const dataRes = execDates.length > 0
      ? await query(DATA_SQL, [region, execDates.join(",")])
      : { rows: [] };

    return NextResponse.json(
      {
        rows: dataRes.rows,
        regions: regionsRes.rows.map((r) => (r as { region: string }).region),
        exec_dates: availableExecDates,
      },
      {
        headers: {
          "Cache-Control": "public, s-maxage=300, stale-while-revalidate=60",
        },
      }
    );
  } catch (error) {
    console.error("[pjm-load-forecast-hourly] DB query failed:", error);
    return NextResponse.json(
      { error: "Failed to fetch PJM forecast load data" },
      { status: 500 }
    );
  }
}
