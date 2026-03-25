-- PJM solar forecast (grid-scale + BTM)
-- Placeholders: {execution_date}, {forecast_date}, {max_execution_hour}
WITH ranked AS (
    SELECT
        forecast_rank,
        hour_ending,
        solar_forecast,
        solar_forecast_btm,
        MIN(forecast_rank) OVER () AS min_forecast_rank
    FROM pjm_cleaned.pjm_solar_forecast_hourly
    WHERE
        forecast_execution_date::DATE = '{execution_date}'
        AND EXTRACT(HOUR FROM forecast_execution_datetime) <= {max_execution_hour}
        AND forecast_date::DATE = '{forecast_date}'
)
SELECT hour_ending, solar_forecast, solar_forecast_btm
FROM ranked
WHERE forecast_rank = min_forecast_rank
ORDER BY hour_ending;
