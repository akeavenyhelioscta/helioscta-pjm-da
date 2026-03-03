import { NextResponse } from "next/server";
import { query } from "@/lib/db";

export const dynamic = "force-dynamic";

/* ------------------------------------------------------------------ */
/*  SQL                                                                */
/* ------------------------------------------------------------------ */

const LOAD_FORECAST_SQL = `
  SELECT DISTINCT
    forecast_execution_datetime,
    forecast_execution_date,
    forecast_date,
    hour_ending,
    forecast_load_mw,
    forecast_rank
  FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_forecast_hourly
  WHERE forecast_date = $1::date
    AND region = 'RTO_COMBINED'
  ORDER BY forecast_execution_datetime, hour_ending
`;

const SOLAR_FORECAST_SQL = `
  SELECT DISTINCT
    forecast_execution_datetime,
    forecast_execution_date,
    forecast_date,
    hour_ending,
    solar_forecast,
    solar_forecast_btm,
    forecast_rank
  FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_solar_forecast_hourly
  WHERE forecast_date = $1::date
  ORDER BY forecast_execution_datetime, hour_ending
`;

const WIND_FORECAST_SQL = `
  SELECT DISTINCT
    forecast_execution_datetime,
    forecast_execution_date,
    forecast_date,
    hour_ending,
    wind_forecast,
    forecast_rank
  FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_wind_forecast_hourly
  WHERE forecast_date = $1::date
  ORDER BY forecast_execution_datetime, hour_ending
`;

const LOAD_ACTUALS_SQL = `
  SELECT DISTINCT
    date,
    hour_ending,
    rt_load_mw
  FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_rt_prelim_hourly
  WHERE date = $1::date
    AND region = 'RTO'
  ORDER BY hour_ending
`;

const FUEL_MIX_ACTUALS_SQL = `
  SELECT DISTINCT
    date,
    hour_ending,
    solar,
    wind
  FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_fuel_mix_hourly
  WHERE date = $1::date
  ORDER BY hour_ending
`;

const AVAILABLE_DATES_SQL = `
  SELECT DISTINCT forecast_date
  FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_forecast_hourly
  WHERE forecast_rank = 1
    AND region = 'RTO_COMBINED'
  ORDER BY forecast_date DESC
  LIMIT 14
`;

/* ------------------------------------------------------------------ */
/*  Handler                                                            */
/* ------------------------------------------------------------------ */

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);

  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const defaultDate = yesterday.toISOString().slice(0, 10);
  const targetDate = searchParams.get("target_date") || defaultDate;

  try {
    const [loadFcstRes, solarFcstRes, windFcstRes, loadActRes, fuelMixRes, datesRes] =
      await Promise.all([
        query(LOAD_FORECAST_SQL, [targetDate]),
        query(SOLAR_FORECAST_SQL, [targetDate]),
        query(WIND_FORECAST_SQL, [targetDate]),
        query(LOAD_ACTUALS_SQL, [targetDate]),
        query(FUEL_MIX_ACTUALS_SQL, [targetDate]),
        query(AVAILABLE_DATES_SQL, ["RTO_COMBINED"]),
      ]);

    const availableDates = datesRes.rows.map((r) => {
      const val = (r as { forecast_date: string }).forecast_date;
      return typeof val === "string" ? val.slice(0, 10) : new Date(val).toISOString().slice(0, 10);
    });

    return NextResponse.json(
      {
        target_date: targetDate,
        load_forecasts: loadFcstRes.rows,
        solar_forecasts: solarFcstRes.rows,
        wind_forecasts: windFcstRes.rows,
        load_actuals: loadActRes.rows,
        fuel_mix_actuals: fuelMixRes.rows,
        available_dates: availableDates,
      },
      {
        headers: {
          "Cache-Control": "public, s-maxage=300, stale-while-revalidate=60",
        },
      }
    );
  } catch (error) {
    console.error("[pjm-net-load-forecast-performance] DB query failed:", error);
    return NextResponse.json(
      { error: "Failed to fetch net load forecast performance data" },
      { status: 500 }
    );
  }
}
