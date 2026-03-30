-- PJM official solar forecast (RTO), latest execution per target date.
-- Returns the most recent forecast vintage for each (date, hour_ending)
-- from today onward (forward-looking only).
-- No region filter — PJM solar forecast is RTO-level only.
with params as (
    select
        case
            when left('{timezone}', 1) = chr(123) and right('{timezone}', 1) = chr(125) then 'America/New_York'
            else coalesce(nullif('{timezone}', ''), 'America/New_York')
        end::text as timezone
),
forecast as (
    select
        ROW_NUMBER() over (
            partition by forecast_date, hour_ending
            order by forecast_rank desc, forecast_execution_datetime desc
        ) as rn,
        forecast_rank,
        forecast_execution_datetime,
        forecast_execution_date,
        forecast_datetime,
        forecast_date,
        hour_ending,
        solar_forecast,
        solar_forecast_btm
    from pjm_cleaned.pjm_solar_forecast_hourly
    where forecast_date >= (CURRENT_TIMESTAMP AT TIME ZONE (select timezone from params))::date
)
select
    forecast_datetime,
    forecast_date,
    hour_ending,
    forecast_rank,
    forecast_execution_datetime,
    forecast_execution_date,
    solar_forecast,
    solar_forecast_btm
from forecast
where rn = 1
order by forecast_datetime, forecast_execution_datetime desc
