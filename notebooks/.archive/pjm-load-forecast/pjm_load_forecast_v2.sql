-- Forecast evolution for current date across all regions, ordered by vintage (rank)
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
  forecast_date = (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date

ORDER BY region, forecast_rank, hour_ending;
