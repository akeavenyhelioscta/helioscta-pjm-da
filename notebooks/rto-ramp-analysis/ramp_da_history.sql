-- DA forecast history for a date range, latest execution per forecast_date (RTO only)
-- Placeholders: {history_start}, {history_end}, {max_execution_hour}
WITH ranked AS (
    SELECT
        forecast_rank,
        forecast_execution_datetime,
        forecast_date,
        hour_ending,
        region,
        forecast_load_mw,
        MIN(forecast_rank) OVER (PARTITION BY forecast_date) AS min_forecast_rank
    FROM pjm_cleaned.pjm_load_forecast_hourly
    WHERE
        forecast_date::DATE BETWEEN '{history_start}' AND '{history_end}'
        AND EXTRACT(HOUR FROM forecast_execution_datetime) <= {max_execution_hour}
        AND region = 'RTO'
)
SELECT
    forecast_date,
    hour_ending,
    region,
    forecast_load_mw
FROM ranked
WHERE forecast_rank = min_forecast_rank
ORDER BY forecast_date, hour_ending;
