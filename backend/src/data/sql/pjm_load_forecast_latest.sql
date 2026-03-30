-- PJM official load forecast, latest execution per target date.
-- Parameterized by {region} (default RTO).
-- Returns the most recent forecast vintage for each (date, hour_ending)
-- from today onward (forward-looking only).
with params as (
    select
        case
            when left('{region}', 1) = chr(123) and right('{region}', 1) = chr(125) then 'RTO'
            else coalesce(nullif('{region}', ''), 'RTO')
        end::text as region,
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
        region,
        forecast_load_mw
    from pjm_cleaned.pjm_load_forecast_hourly
    where region = (select region from params)
      and forecast_date >= (CURRENT_TIMESTAMP AT TIME ZONE (select timezone from params))::date
)
select
    forecast_datetime,
    forecast_date,
    hour_ending,
    forecast_rank,
    forecast_execution_datetime,
    forecast_execution_date,
    region,
    forecast_load_mw
from forecast
where rn = 1
order by forecast_datetime, forecast_execution_datetime desc
