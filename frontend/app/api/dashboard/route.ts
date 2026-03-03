import { NextResponse } from "next/server";
import { query } from "@/lib/db";

export const dynamic = "force-dynamic";

const SCHEMA = "dbt_pjm_v1_2026_feb_19";

const RT_LOAD_LATEST_SQL = `
  SELECT date, hour_ending, rt_load_mw
  FROM ${SCHEMA}.staging_v1_pjm_load_rt_metered_hourly
  WHERE region = 'RTO'
  ORDER BY date DESC, hour_ending DESC
  LIMIT 1
`;

const DA_LMP_LATEST_SQL = `
  SELECT date, hour_ending, lmp_total
  FROM ${SCHEMA}.staging_v1_pjm_lmps_hourly
  WHERE hub = 'WESTERN HUB' AND market = 'da'
  ORDER BY date DESC, hour_ending DESC
  LIMIT 1
`;

const DA_LOAD_LATEST_SQL = `
  SELECT date, hour_ending, da_load_mw
  FROM ${SCHEMA}.staging_v1_pjm_load_da_hourly
  WHERE region = 'RTO'
  ORDER BY date DESC, hour_ending DESC
  LIMIT 1
`;

const FORECAST_LOAD_LATEST_SQL = `
  SELECT forecast_date AS date, hour_ending, forecast_load_mw
  FROM ${SCHEMA}.staging_v1_pjm_load_forecast_hourly
  WHERE region = 'RTO_COMBINED' AND forecast_rank = 1
  ORDER BY forecast_date DESC, hour_ending DESC
  LIMIT 1
`;

const RT_LOAD_TODAY_SQL = `
  SELECT hour_ending, rt_load_mw
  FROM ${SCHEMA}.staging_v1_pjm_load_rt_metered_hourly
  WHERE region = 'RTO'
    AND date = (SELECT MAX(date) FROM ${SCHEMA}.staging_v1_pjm_load_rt_metered_hourly WHERE region = 'RTO')
  ORDER BY hour_ending ASC
`;

const DA_LMP_TODAY_SQL = `
  SELECT hour_ending, lmp_total
  FROM ${SCHEMA}.staging_v1_pjm_lmps_hourly
  WHERE hub = 'WESTERN HUB' AND market = 'da'
    AND date = (SELECT MAX(date) FROM ${SCHEMA}.staging_v1_pjm_lmps_hourly WHERE hub = 'WESTERN HUB' AND market = 'da')
  ORDER BY hour_ending ASC
`;

const FORECAST_TODAY_SQL = `
  SELECT hour_ending, forecast_load_mw
  FROM ${SCHEMA}.staging_v1_pjm_load_forecast_hourly
  WHERE region = 'RTO_COMBINED'
    AND forecast_rank = 1
    AND forecast_date = CURRENT_DATE
  ORDER BY hour_ending ASC
`;

const RT_ACTUAL_TODAY_SQL = `
  SELECT hour_ending, rt_load_mw
  FROM ${SCHEMA}.staging_v1_pjm_load_rt_metered_hourly
  WHERE region = 'RTO' AND date = CURRENT_DATE
  ORDER BY hour_ending ASC
`;

const RT_LOAD_YESTERDAY_SQL = `
  SELECT rt_load_mw
  FROM ${SCHEMA}.staging_v1_pjm_load_rt_metered_hourly
  WHERE region = 'RTO'
    AND date = CURRENT_DATE - INTERVAL '1 day'
    AND hour_ending = $1
  LIMIT 1
`;

const DA_LMP_YESTERDAY_SQL = `
  SELECT lmp_total
  FROM ${SCHEMA}.staging_v1_pjm_lmps_hourly
  WHERE hub = 'WESTERN HUB' AND market = 'da'
    AND date = CURRENT_DATE - INTERVAL '1 day'
    AND hour_ending = $1
  LIMIT 1
`;

const DA_LOAD_YESTERDAY_SQL = `
  SELECT da_load_mw
  FROM ${SCHEMA}.staging_v1_pjm_load_da_hourly
  WHERE region = 'RTO'
    AND date = CURRENT_DATE - INTERVAL '1 day'
    AND hour_ending = $1
  LIMIT 1
`;

const FORECAST_LOAD_YESTERDAY_SQL = `
  SELECT forecast_load_mw
  FROM ${SCHEMA}.staging_v1_pjm_load_forecast_hourly
  WHERE region = 'RTO_COMBINED' AND forecast_rank = 1
    AND forecast_date = CURRENT_DATE - INTERVAL '1 day'
    AND hour_ending = $1
  LIMIT 1
`;

export async function GET() {
  try {
    // Phase 1: All independent queries in parallel
    const [
      rtLatest, daLmpLatest, daLoadLatest, forecastLatest,
      rtToday, daLmpToday,
      forecastToday, rtActualToday,
    ] = await Promise.all([
      query(RT_LOAD_LATEST_SQL),
      query(DA_LMP_LATEST_SQL),
      query(DA_LOAD_LATEST_SQL),
      query(FORECAST_LOAD_LATEST_SQL),
      query(RT_LOAD_TODAY_SQL),
      query(DA_LMP_TODAY_SQL),
      query(FORECAST_TODAY_SQL),
      query(RT_ACTUAL_TODAY_SQL),
    ]);

    // Phase 2: Yesterday values for trend (depends on Phase 1 hour_ending)
    const rtHe = rtLatest.rows[0]?.hour_ending;
    const daLmpHe = daLmpLatest.rows[0]?.hour_ending;
    const daLoadHe = daLoadLatest.rows[0]?.hour_ending;
    const forecastHe = forecastLatest.rows[0]?.hour_ending;

    const [rtYesterday, daLmpYesterday, daLoadYesterday, forecastYesterday] =
      await Promise.all([
        rtHe != null ? query(RT_LOAD_YESTERDAY_SQL, [rtHe]) : Promise.resolve({ rows: [] }),
        daLmpHe != null ? query(DA_LMP_YESTERDAY_SQL, [daLmpHe]) : Promise.resolve({ rows: [] }),
        daLoadHe != null ? query(DA_LOAD_YESTERDAY_SQL, [daLoadHe]) : Promise.resolve({ rows: [] }),
        forecastHe != null ? query(FORECAST_LOAD_YESTERDAY_SQL, [forecastHe]) : Promise.resolve({ rows: [] }),
      ]);

    return NextResponse.json(
      {
        kpis: {
          rt_load: {
            value: rtLatest.rows[0]?.rt_load_mw ?? null,
            date: rtLatest.rows[0]?.date ?? null,
            hour_ending: rtHe ?? null,
            yesterday_value: rtYesterday.rows[0]?.rt_load_mw ?? null,
          },
          da_lmp: {
            value: daLmpLatest.rows[0]?.lmp_total ?? null,
            date: daLmpLatest.rows[0]?.date ?? null,
            hour_ending: daLmpHe ?? null,
            yesterday_value: daLmpYesterday.rows[0]?.lmp_total ?? null,
          },
          da_load: {
            value: daLoadLatest.rows[0]?.da_load_mw ?? null,
            date: daLoadLatest.rows[0]?.date ?? null,
            hour_ending: daLoadHe ?? null,
            yesterday_value: daLoadYesterday.rows[0]?.da_load_mw ?? null,
          },
          forecast_load: {
            value: forecastLatest.rows[0]?.forecast_load_mw ?? null,
            date: forecastLatest.rows[0]?.date ?? null,
            hour_ending: forecastHe ?? null,
            yesterday_value: forecastYesterday.rows[0]?.forecast_load_mw ?? null,
          },
        },
        rt_load_profile: rtToday.rows,
        da_lmp_profile: daLmpToday.rows,
        forecast_vs_actual: {
          forecast: forecastToday.rows,
          actual: rtActualToday.rows,
        },
      },
      {
        headers: {
          "Cache-Control": "public, s-maxage=300, stale-while-revalidate=60",
        },
      }
    );
  } catch (error) {
    console.error("[dashboard] DB query failed:", error);
    return NextResponse.json(
      { error: "Failed to fetch dashboard data" },
      { status: 500 }
    );
  }
}
