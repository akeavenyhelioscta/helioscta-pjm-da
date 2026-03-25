with ranked as (
    select
        forecast_rank
        ,forecast_execution_datetime
        ,forecast_execution_date
        ,forecast_datetime
        ,forecast_date
        ,hour_ending
        ,date_utc
        ,hour_ending_utc
        ,source
        ,region
        ,forecast_generation_mw
        ,MIN(forecast_rank) OVER () as min_forecast_rank
    from meteologica_cleaned.meteologica_pjm_generation_forecast_hourly
    where
        forecast_execution_date::DATE = (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE
        AND EXTRACT(HOUR FROM forecast_execution_datetime) <= 10
        AND forecast_date::DATE = (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE + 1
        AND region = 'RTO'
        AND source = 'hydro'
)

select
    forecast_rank
    ,forecast_execution_datetime
    ,forecast_execution_date
    ,forecast_datetime
    ,forecast_date
    ,hour_ending
    ,date_utc
    ,hour_ending_utc
    ,source
    ,region
    ,forecast_generation_mw
from ranked
where forecast_rank = min_forecast_rank
