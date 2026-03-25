-- Latest Meteologica load forecast strip (all forecast dates, RTO).
-- For each (forecast_date, hour_ending), picks the most recent vintage.
-- This handles the case where the latest execution only covers remaining
-- hours of today — earlier hours are filled from the previous vintage.
with ranked as (
    select
        forecast_date
        ,hour_ending
        ,forecast_load_mw
        ,forecast_execution_datetime
        ,ROW_NUMBER() over (
            partition by forecast_date, hour_ending
            order by forecast_rank desc
        ) as rn
    from meteologica_cleaned.meteologica_pjm_demand_forecast_hourly
    where region = '{region}'
      and forecast_date >= current_date
)
select
    forecast_date as date
    ,hour_ending
    ,forecast_load_mw
    ,forecast_execution_datetime
from ranked
where rn = 1
order by forecast_date, hour_ending
