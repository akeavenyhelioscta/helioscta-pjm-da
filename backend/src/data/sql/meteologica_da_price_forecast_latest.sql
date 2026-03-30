-- Meteologica demand forecast, latest execution per target date.
-- Parameterized by {region} (default RTO).
-- Returns the most recent forecast vintage for each (date, hour_ending)
-- from today onward (forward-looking only).
with params as (
    select
        case
            when left('{hub}', 1) = chr(123) and right('{hub}', 1) = chr(125) then 'SYSTEM'
            else coalesce(nullif('{hub}', ''), 'SYSTEM')
        end::text as hub,
        case
            when left('{timezone}', 1) = chr(123) and right('{timezone}', 1) = chr(125) then 'America/Denver'
            else coalesce(nullif('{timezone}', ''), 'America/Denver')
        end::text as timezone
),
forecast as (
    select
        ROW_NUMBER() over (
            partition by forecast_date, hour_ending
            order by forecast_rank desc
        ) as rn,
        forecast_rank,
        forecast_execution_datetime,
        forecast_execution_date,
        forecast_datetime,
        forecast_date,
        hour_ending,
        hub,
        forecast_da_price
    from meteologica_cleaned.meteologica_pjm_da_price_forecast_hourly
    where hub = (select hub from params)
      and forecast_date >= (CURRENT_TIMESTAMP AT TIME ZONE (select timezone from params))::date
)
select
    forecast_datetime,
    forecast_date,
    hour_ending,
    forecast_rank,
    forecast_execution_datetime,
    forecast_execution_date,
    hub,
    forecast_da_price
from forecast
where rn = 1
order by forecast_datetime, forecast_execution_datetime desc