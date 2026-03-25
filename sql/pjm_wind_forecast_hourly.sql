with ranked as (
    select
        forecast_rank
        ,forecast_execution_datetime
        ,forecast_execution_date
        ,forecast_datetime
        ,forecast_date
        ,hour_ending
        ,wind_forecast
        ,MIN(forecast_rank) OVER () as min_forecast_rank
    from pjm_cleaned.pjm_wind_forecast_hourly
    where
        forecast_execution_date::DATE = (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE
        AND EXTRACT(HOUR FROM forecast_execution_datetime) <= 10
        AND forecast_date::DATE = (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE + 1
)

select
    forecast_rank
    ,forecast_execution_datetime
    ,forecast_execution_date
    ,forecast_datetime
    ,forecast_date
    ,hour_ending
    ,wind_forecast
from ranked
where forecast_rank = min_forecast_rank
