-- PJM official load forecast (RTO), latest execution per target date.
-- Returns one row per (date, hour_ending).
-- forecast_rank is DENSE_RANK ascending by forecast_execution_datetime:
--   rank 1 = earliest/oldest, MAX(rank) = latest forecast.
-- ORDER BY forecast_rank DESC → rn=1 picks the most recent vintage for each hour.
with ranked as (
    select
        forecast_date
        ,hour_ending
        ,region
        ,forecast_load_mw
        ,forecast_execution_datetime
        ,ROW_NUMBER() over (
            partition by forecast_date, hour_ending
            order by forecast_rank desc
        ) as rn
    from pjm_cleaned.pjm_load_forecast_hourly
    where region = '{region}'
      and forecast_date >= current_date - interval '{lookback_days} days'
)
select
    forecast_date as date
    ,hour_ending
    ,region
    ,forecast_load_mw
    ,forecast_execution_datetime
from ranked
where rn = 1
order by date, hour_ending
