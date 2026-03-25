-- DATA_SQL
WITH pjm_load_forecast AS (
  SELECT
    forecast_datetime AS datetime,
    forecast_rank,
    forecast_execution_datetime,
    forecast_execution_date,
    forecast_date,
    hour_ending,
    region,
    forecast_load_mw

  FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_forecast_hourly

  WHERE
    forecast_date >= (CURRENT_TIMESTAMP AT TIME ZONE 'MST' - INTERVAL '7 days')
    AND forecast_date <= (CURRENT_TIMESTAMP AT TIME ZONE 'MST')
)

SELECT * FROM pjm_load_forecast
ORDER BY forecast_date DESC, hour_ending ASC, region;
