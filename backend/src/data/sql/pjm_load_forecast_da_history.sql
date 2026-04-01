-- PJM official load forecast, full history.
-- For each (forecast_date, hour_ending), picks the highest forecast_rank
-- (latest vintage before DA cutoff). Covers all available dates for backtest.
-- Parameterized by {region} (default RTO).
with params as (
    select
        case
            when left('{region}', 1) = chr(123) and right('{region}', 1) = chr(125) then 'RTO'
            else coalesce(nullif('{region}', ''), 'RTO')
        end::text as region
),
ranked as (
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
from ranked
where rn = 1
order by forecast_date, hour_ending
