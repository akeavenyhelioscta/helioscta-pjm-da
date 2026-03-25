-- Latest Meteologica ECMWF ensemble load forecast strip (RTO).
-- For each (forecast_date, hour_ending), picks the most recent vintage.
-- Returns avg + top/bottom bounds.
with ranked as (
    select
        forecast_date
        ,hour_ending
        ,forecast_load_average_mw
        ,forecast_load_top_mw
        ,forecast_load_bottom_mw
        ,forecast_execution_datetime
        ,ROW_NUMBER() over (
            partition by forecast_date, hour_ending
            order by forecast_rank desc
        ) as rn
    from meteologica_cleaned.meteologica_pjm_demand_forecast_ecmwf_ens_hourly
    where region = '{region}'
      and forecast_date >= current_date
)
select
    forecast_date as date
    ,hour_ending
    ,forecast_load_average_mw
    ,forecast_load_top_mw
    ,forecast_load_bottom_mw
    ,forecast_execution_datetime
from ranked
where rn = 1
order by forecast_date, hour_ending
