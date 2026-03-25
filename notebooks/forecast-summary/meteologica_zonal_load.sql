-- Meteologica zonal load forecasts (all PJM transmission zones)
-- Placeholders: {execution_date}, {forecast_date}, {max_execution_hour}
WITH ranked AS (
    SELECT
        forecast_rank,
        hour_ending,
        region,
        forecast_load_mw,
        MIN(forecast_rank) OVER (PARTITION BY region) AS min_forecast_rank
    FROM meteologica_cleaned.meteologica_pjm_demand_forecast_hourly
    WHERE
        forecast_execution_date::DATE = '2026-03-11'
        AND EXTRACT(HOUR FROM forecast_execution_datetime) <= 10
        AND forecast_date::DATE = '2026-03-12'
        -- forecast_execution_date::DATE = '{execution_date}'
        -- AND EXTRACT(HOUR FROM forecast_execution_datetime) <= {max_execution_hour}
        -- AND forecast_date::DATE = '{forecast_date}'
        AND region IN (
            'MIDATL_AE', 'MIDATL_BC', 'MIDATL_DPL', 'MIDATL_JC', 'MIDATL_ME',
            'MIDATL_PE', 'MIDATL_PEP', 'MIDATL_PL', 'MIDATL_PN', 'MIDATL_PS',
            'SOUTH_DOM',
            'WEST_AEP', 'WEST_AP', 'WEST_ATSI', 'WEST_CE', 'WEST_DAY',
            'WEST_DEOK', 'WEST_DUQ'
        )
)
SELECT hour_ending, region, forecast_load_mw
FROM ranked
WHERE forecast_rank = min_forecast_rank
ORDER BY region, hour_ending;
