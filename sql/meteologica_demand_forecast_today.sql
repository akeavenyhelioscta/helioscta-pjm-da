-- ============================================================================
-- Meteologica PJM Demand Forecast — Complete Hourly Forecast for Today
-- ============================================================================
-- Combines the deterministic Meteologica load forecast with the ECMWF
-- ensemble forecast (avg, min, max) for PJM RTO, hours 1-24 EPT.
--
-- Strategy: For each hour, use the LATEST available vintage from today's
-- execution runs. This mirrors what the Meteologica dashboard displays —
-- the most recent forecast value for each hour.
--
-- Tables:
--   meteologica_cleaned.meteologica_pjm_demand_forecast_hourly
--   meteologica_cleaned.meteologica_pjm_demand_forecast_ecmwf_ens_hourly
-- ============================================================================

WITH forecast_date_param AS (
    -- Change this date to query a different day
    -- forecast_date is a DATE column (not timestamp)
    SELECT CURRENT_DATE AS target_date
),

-- Deterministic forecast: pick the latest vintage per hour
det_ranked AS (
    SELECT
        d.hour_ending,
        d.forecast_load_mw::numeric AS forecast_load_mw,
        d.forecast_datetime,
        d.forecast_execution_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY d.hour_ending
            ORDER BY d.forecast_execution_datetime DESC
        ) AS rn
    FROM meteologica_cleaned.meteologica_pjm_demand_forecast_hourly d
    CROSS JOIN forecast_date_param p
    WHERE d.forecast_date = p.target_date
      AND d.region = 'RTO'
),

det AS (
    SELECT hour_ending, forecast_load_mw, forecast_datetime, forecast_execution_datetime
    FROM det_ranked
    WHERE rn = 1
),

-- ECMWF ensemble: pick the latest vintage per hour
ens_ranked AS (
    SELECT
        e.hour_ending,
        e.forecast_load_average_mw::numeric AS ens_avg_mw,
        e.forecast_load_bottom_mw::numeric  AS ens_min_mw,
        e.forecast_load_top_mw::numeric     AS ens_max_mw,
        e.forecast_execution_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY e.hour_ending
            ORDER BY e.forecast_execution_datetime DESC
        ) AS rn
    FROM meteologica_cleaned.meteologica_pjm_demand_forecast_ecmwf_ens_hourly e
    CROSS JOIN forecast_date_param p
    WHERE e.forecast_date = p.target_date
      AND e.region = 'RTO'
),

ens AS (
    SELECT hour_ending, ens_avg_mw, ens_min_mw, ens_max_mw, forecast_execution_datetime
    FROM ens_ranked
    WHERE rn = 1
)

SELECT
    d.hour_ending                                       AS he,
    d.forecast_datetime                                 AS forecast_datetime_utc,
    d.forecast_load_mw                                  AS meteologica_load_mw,
    e.ens_avg_mw                                        AS ecmwf_ens_avg_mw,
    e.ens_min_mw                                        AS ecmwf_ens_min_mw,
    e.ens_max_mw                                        AS ecmwf_ens_max_mw,
    (e.ens_max_mw - e.ens_min_mw)                       AS ecmwf_ens_spread_mw,
    d.forecast_execution_datetime                       AS det_vintage_utc,
    e.forecast_execution_datetime                       AS ens_vintage_utc
FROM det d
FULL OUTER JOIN ens e USING (hour_ending)
ORDER BY COALESCE(d.hour_ending, e.hour_ending);
