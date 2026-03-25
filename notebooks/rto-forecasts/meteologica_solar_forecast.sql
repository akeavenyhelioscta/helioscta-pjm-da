-- Meteologica solar generation forecast (RTO)
-- Placeholders: {execution_date}, {forecast_date}, {max_execution_hour}
WITH ranked AS (
    SELECT
        forecast_rank,
        forecast_execution_datetime,
        forecast_execution_date,
        forecast_datetime,
        forecast_date,
        hour_ending,
        source,
        region,
        forecast_generation_mw,
        MIN(forecast_rank) OVER () AS min_forecast_rank
    FROM meteologica_cleaned.meteologica_pjm_generation_forecast_hourly
    WHERE
        forecast_execution_date::DATE = '{execution_date}'
        AND EXTRACT(HOUR FROM forecast_execution_datetime) <= {max_execution_hour}
        AND forecast_date::DATE = '{forecast_date}'
        AND source = 'solar'
        AND region = 'RTO'
)
SELECT
    forecast_rank,
    forecast_execution_datetime,
    forecast_execution_date,
    forecast_datetime,
    forecast_date,
    hour_ending,
    source,
    region,
    forecast_generation_mw
FROM ranked
WHERE forecast_rank = min_forecast_rank
ORDER BY hour_ending;
