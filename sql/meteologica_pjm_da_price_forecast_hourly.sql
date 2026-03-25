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
        ,hub
        ,forecast_da_price
        ,MIN(forecast_rank) OVER () as min_forecast_rank
    from meteologica_cleaned.meteologica_pjm_da_price_forecast_hourly
    where
        forecast_execution_date::DATE = CURRENT_DATE
        AND EXTRACT(HOUR FROM forecast_execution_datetime) <= 10
        AND forecast_date::DATE = CURRENT_DATE + 1
        AND hub = 'WESTERN'
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
    ,hub
    ,forecast_da_price
from ranked
where forecast_rank = min_forecast_rank
