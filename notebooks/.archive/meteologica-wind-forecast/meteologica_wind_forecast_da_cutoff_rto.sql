-- Meteologica Wind Forecast — DA Cutoff Analysis (RTO Only)
WITH forecast_raw AS (
  SELECT
    forecast_execution_datetime,
    forecast_date,
    hour_ending,
    region,
    forecast_generation_mw
  FROM dbt_meteologica_v1_2026_mar_03.staging_v1_meteologica_pjm_generation_forecast_hourly
  WHERE source = 'wind'
    AND region = 'RTO'
    AND forecast_date >= CURRENT_DATE
    AND forecast_date <= CURRENT_DATE + INTERVAL '7 days'
),

cutoff_vintages AS (
  SELECT
    forecast_date,
    region,
    MAX(forecast_execution_datetime) AS cutoff_execution_dt
  FROM forecast_raw
  WHERE forecast_execution_datetime < LEAST(forecast_date + INTERVAL '9 hours', CURRENT_DATE + INTERVAL '9 hours')
  GROUP BY forecast_date, region
),

da_cutoff_forecast AS (
  SELECT
    fr.forecast_date,
    fr.hour_ending,
    fr.region,
    fr.forecast_generation_mw AS cutoff_gen_mw,
    fr.forecast_execution_datetime AS cutoff_execution_dt
  FROM forecast_raw fr
  INNER JOIN cutoff_vintages cv
    ON fr.forecast_date = cv.forecast_date
    AND fr.region = cv.region
    AND fr.forecast_execution_datetime = cv.cutoff_execution_dt
),

lookback_12h_vintage AS (
  SELECT
    cv.forecast_date,
    cv.region,
    MAX(fr.forecast_execution_datetime) AS exec_dt_12h
  FROM cutoff_vintages cv
  INNER JOIN forecast_raw fr
    ON fr.forecast_date = cv.forecast_date
    AND fr.region = cv.region
    AND fr.forecast_execution_datetime <= cv.cutoff_execution_dt - INTERVAL '12 hours'
  GROUP BY cv.forecast_date, cv.region
),

lookback_24h_vintage AS (
  SELECT
    cv.forecast_date,
    cv.region,
    MAX(fr.forecast_execution_datetime) AS exec_dt_24h
  FROM cutoff_vintages cv
  INNER JOIN forecast_raw fr
    ON fr.forecast_date = cv.forecast_date
    AND fr.region = cv.region
    AND fr.forecast_execution_datetime <= cv.cutoff_execution_dt - INTERVAL '24 hours'
  GROUP BY cv.forecast_date, cv.region
),

lookback_48h_vintage AS (
  SELECT
    cv.forecast_date,
    cv.region,
    MAX(fr.forecast_execution_datetime) AS exec_dt_48h
  FROM cutoff_vintages cv
  INNER JOIN forecast_raw fr
    ON fr.forecast_date = cv.forecast_date
    AND fr.region = cv.region
    AND fr.forecast_execution_datetime <= cv.cutoff_execution_dt - INTERVAL '48 hours'
  GROUP BY cv.forecast_date, cv.region
),

combined AS (
  SELECT
    dc.forecast_date,
    dc.hour_ending,
    dc.region,
    dc.cutoff_gen_mw,
    dc.cutoff_execution_dt,
    lb12.exec_dt_12h,
    fr12.forecast_generation_mw AS gen_mw_12h,
    lb24.exec_dt_24h,
    fr24.forecast_generation_mw AS gen_mw_24h,
    lb48.exec_dt_48h,
    fr48.forecast_generation_mw AS gen_mw_48h
  FROM da_cutoff_forecast dc
  LEFT JOIN lookback_12h_vintage lb12
    ON dc.forecast_date = lb12.forecast_date
    AND dc.region = lb12.region
  LEFT JOIN forecast_raw fr12
    ON fr12.forecast_date = dc.forecast_date
    AND fr12.hour_ending = dc.hour_ending
    AND fr12.region = dc.region
    AND fr12.forecast_execution_datetime = lb12.exec_dt_12h
  LEFT JOIN lookback_24h_vintage lb24
    ON dc.forecast_date = lb24.forecast_date
    AND dc.region = lb24.region
  LEFT JOIN forecast_raw fr24
    ON fr24.forecast_date = dc.forecast_date
    AND fr24.hour_ending = dc.hour_ending
    AND fr24.region = dc.region
    AND fr24.forecast_execution_datetime = lb24.exec_dt_24h
  LEFT JOIN lookback_48h_vintage lb48
    ON dc.forecast_date = lb48.forecast_date
    AND dc.region = lb48.region
  LEFT JOIN forecast_raw fr48
    ON fr48.forecast_date = dc.forecast_date
    AND fr48.hour_ending = dc.hour_ending
    AND fr48.region = dc.region
    AND fr48.forecast_execution_datetime = lb48.exec_dt_48h
),

with_deltas AS (
  SELECT
    *,
    cutoff_gen_mw - gen_mw_12h AS delta_12h,
    cutoff_gen_mw - gen_mw_24h AS delta_24h,
    cutoff_gen_mw - gen_mw_48h AS delta_48h
  FROM combined
)

SELECT *
FROM with_deltas
ORDER BY forecast_date DESC, hour_ending