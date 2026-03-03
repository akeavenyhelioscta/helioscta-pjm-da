import { NextResponse } from "next/server";
import { query } from "@/lib/db";

export const dynamic = "force-dynamic";

/* ------------------------------------------------------------------ */
/*  Region mapping: forecast table uses different names than RT prelim */
/* ------------------------------------------------------------------ */

const REGIONS = [
  { key: "RTO", forecast: "RTO_COMBINED", rtPrelim: "RTO", display: "PJM RTO" },
  { key: "MIDATL", forecast: "MID_ATLANTIC_REGION", rtPrelim: "MIDATL", display: "Mid Atlantic" },
  { key: "SOUTH", forecast: "SOUTHERN_REGION", rtPrelim: "SOUTH", display: "South" },
  { key: "WEST", forecast: "WESTERN_REGION", rtPrelim: "WEST", display: "Western" },
];

/* ------------------------------------------------------------------ */
/*  SQL                                                                */
/* ------------------------------------------------------------------ */

const FORECAST_SQL = `
  SELECT DISTINCT
    forecast_execution_datetime,
    forecast_execution_date,
    forecast_date,
    hour_ending,
    region,
    forecast_load_mw,
    forecast_rank
  FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_forecast_hourly
  WHERE forecast_date = $1::date
    AND region = ANY($2::text[])
  ORDER BY forecast_execution_datetime, hour_ending
`;

const ACTUALS_SQL = `
  SELECT DISTINCT
    date,
    hour_ending,
    region,
    rt_load_mw
  FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_rt_prelim_hourly
  WHERE date = $1::date
    AND region = ANY($2::text[])
  ORDER BY hour_ending
`;

const AVAILABLE_DATES_SQL = `
  SELECT DISTINCT forecast_date
  FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_forecast_hourly
  WHERE forecast_rank = 1
    AND region = $1
  ORDER BY forecast_date DESC
  LIMIT 14
`;

/* ------------------------------------------------------------------ */
/*  Handler                                                            */
/* ------------------------------------------------------------------ */

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);

  // Default target date = yesterday
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const defaultDate = yesterday.toISOString().slice(0, 10);
  const targetDate = searchParams.get("target_date") || defaultDate;

  const forecastRegions = REGIONS.map((r) => r.forecast);
  const rtPrelimRegions = REGIONS.map((r) => r.rtPrelim);

  try {
    const [forecastRes, actualsRes, datesRes] = await Promise.all([
      query(FORECAST_SQL, [targetDate, forecastRegions]),
      query(ACTUALS_SQL, [targetDate, rtPrelimRegions]),
      query(AVAILABLE_DATES_SQL, ["RTO_COMBINED"]),
    ]);

    const availableDates = datesRes.rows.map((r) => {
      const val = (r as { forecast_date: string }).forecast_date;
      return typeof val === "string" ? val.slice(0, 10) : new Date(val).toISOString().slice(0, 10);
    });

    return NextResponse.json(
      {
        target_date: targetDate,
        regions: REGIONS,
        forecasts: forecastRes.rows,
        actuals: actualsRes.rows,
        available_dates: availableDates,
      },
      {
        headers: {
          "Cache-Control": "public, s-maxage=300, stale-while-revalidate=60",
        },
      }
    );
  } catch (error) {
    console.error("[pjm-load-forecast-performance] DB query failed:", error);
    return NextResponse.json(
      { error: "Failed to fetch load forecast performance data" },
      { status: 500 }
    );
  }
}
