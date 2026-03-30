-- Meteologica generation forecast (solar/wind), latest execution per target date.
-- Returns the most recent forecast vintage for each (date, hour_ending)
-- from today onward (forward-looking only).
-- Parameterized by {region} (default RTO) and {source} (e.g. 'solar', 'wind').
with params as (
    select
        case
            when left('{region}', 1) = chr(123) and right('{region}', 1) = chr(125) then 'RTO'
            else coalesce(nullif('{region}', ''), 'RTO')
        end::text as region,
        case
            when left('{source}', 1) = chr(123) and right('{source}', 1) = chr(125) then 'solar'
            else coalesce(nullif('{source}', ''), 'solar')
        end::text as source,
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
        source,
        forecast_generation_mw
    from meteologica_cleaned.meteologica_pjm_generation_forecast_hourly
    where region = (select region from params)
      and source = (select source from params)
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
    source,
    forecast_generation_mw
from forecast
where rn = 1
order by forecast_datetime, forecast_execution_datetime desc
