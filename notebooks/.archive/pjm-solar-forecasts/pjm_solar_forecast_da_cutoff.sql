-- PJM Solar Forecast — DA Cutoff Analysis
-- Finds the last forecast vintage before 9am EST (DA bidding cutoff),
-- compares it to 6h/12h/24h earlier vintages, and computes deltas.
-- Restricted to forecast_date >= today (48h forecast horizon).

WITH forecast_raw AS (
  SELECT
    forecast_execution_datetime,
    forecast_date,
    hour_ending,
    solar_forecast,
    solar_forecast_btm
  FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_solar_forecast_hourly
  WHERE forecast_date >= CURRENT_DATE
    AND forecast_date <= CURRENT_DATE + INTERVAL '2 days'
),

-- For each forecast_date, find the last execution before 9am EST
cutoff_vintages AS (
  SELECT
    forecast_date,
    MAX(forecast_execution_datetime) AS cutoff_execution_dt
  FROM forecast_raw
  WHERE forecast_execution_datetime < LEAST(forecast_date + INTERVAL '9 hours', CURRENT_DATE + INTERVAL '9 hours')
  GROUP BY forecast_date
),

-- Full hourly forecast at the cutoff vintage
da_cutoff_forecast AS (
  SELECT
    fr.forecast_date,
    fr.hour_ending,
    fr.solar_forecast AS cutoff_solar_mw,
    fr.solar_forecast_btm AS cutoff_solar_btm_mw,
    fr.forecast_execution_datetime AS cutoff_execution_dt
  FROM forecast_raw fr
  INNER JOIN cutoff_vintages cv
    ON fr.forecast_date = cv.forecast_date
    AND fr.forecast_execution_datetime = cv.cutoff_execution_dt
),

-- 6h lookback: last execution at or before cutoff - 6h
lookback_6h_vintage AS (
  SELECT
    cv.forecast_date,
    MAX(fr.forecast_execution_datetime) AS exec_dt_6h
  FROM cutoff_vintages cv
  INNER JOIN forecast_raw fr
    ON fr.forecast_date = cv.forecast_date
    AND fr.forecast_execution_datetime <= cv.cutoff_execution_dt - INTERVAL '6 hours'
  GROUP BY cv.forecast_date
),

-- 12h lookback
lookback_12h_vintage AS (
  SELECT
    cv.forecast_date,
    MAX(fr.forecast_execution_datetime) AS exec_dt_12h
  FROM cutoff_vintages cv
  INNER JOIN forecast_raw fr
    ON fr.forecast_date = cv.forecast_date
    AND fr.forecast_execution_datetime <= cv.cutoff_execution_dt - INTERVAL '12 hours'
  GROUP BY cv.forecast_date
),

-- 24h lookback
lookback_24h_vintage AS (
  SELECT
    cv.forecast_date,
    MAX(fr.forecast_execution_datetime) AS exec_dt_24h
  FROM cutoff_vintages cv
  INNER JOIN forecast_raw fr
    ON fr.forecast_date = cv.forecast_date
    AND fr.forecast_execution_datetime <= cv.cutoff_execution_dt - INTERVAL '24 hours'
  GROUP BY cv.forecast_date
),

-- Join cutoff forecast with all lookback vintages
combined AS (
  SELECT
    dc.forecast_date,
    dc.hour_ending,
    dc.cutoff_solar_mw,
    dc.cutoff_solar_btm_mw,
    dc.cutoff_execution_dt,
    -- 6h lookback
    lb6.exec_dt_6h,
    fr6.solar_forecast AS solar_mw_6h,
    fr6.solar_forecast_btm AS solar_btm_mw_6h,
    -- 12h lookback
    lb12.exec_dt_12h,
    fr12.solar_forecast AS solar_mw_12h,
    fr12.solar_forecast_btm AS solar_btm_mw_12h,
    -- 24h lookback
    lb24.exec_dt_24h,
    fr24.solar_forecast AS solar_mw_24h,
    fr24.solar_forecast_btm AS solar_btm_mw_24h
  FROM da_cutoff_forecast dc
  LEFT JOIN lookback_6h_vintage lb6
    ON dc.forecast_date = lb6.forecast_date
  LEFT JOIN forecast_raw fr6
    ON fr6.forecast_date = dc.forecast_date
    AND fr6.hour_ending = dc.hour_ending
    AND fr6.forecast_execution_datetime = lb6.exec_dt_6h
  LEFT JOIN lookback_12h_vintage lb12
    ON dc.forecast_date = lb12.forecast_date
  LEFT JOIN forecast_raw fr12
    ON fr12.forecast_date = dc.forecast_date
    AND fr12.hour_ending = dc.hour_ending
    AND fr12.forecast_execution_datetime = lb12.exec_dt_12h
  LEFT JOIN lookback_24h_vintage lb24
    ON dc.forecast_date = lb24.forecast_date
  LEFT JOIN forecast_raw fr24
    ON fr24.forecast_date = dc.forecast_date
    AND fr24.hour_ending = dc.hour_ending
    AND fr24.forecast_execution_datetime = lb24.exec_dt_24h
),

-- Compute revision deltas (positive = upward revision toward cutoff)
with_deltas AS (
  SELECT
    *,
    cutoff_solar_mw - solar_mw_6h AS delta_6h,
    cutoff_solar_mw - solar_mw_12h AS delta_12h,
    cutoff_solar_mw - solar_mw_24h AS delta_24h,
    cutoff_solar_btm_mw - solar_btm_mw_6h AS delta_btm_6h,
    cutoff_solar_btm_mw - solar_btm_mw_12h AS delta_btm_12h,
    cutoff_solar_btm_mw - solar_btm_mw_24h AS delta_btm_24h
  FROM combined
)

SELECT *
FROM with_deltas
ORDER BY forecast_date DESC, hour_ending
