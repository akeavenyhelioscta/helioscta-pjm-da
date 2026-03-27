-- Latest Meteologica ECMWF ensemble load forecast (RTO).
-- For each (forecast_date, hour_ending), picks the most recent vintage.
-- Returns avg + top/bottom bounds from today onward (forward-looking only).
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
ranked as (
    select
        forecast_date
        ,hour_ending
        ,forecast_load_average_mw
        ,forecast_load_top_mw
        ,forecast_load_bottom_mw
        ,forecast_execution_datetime
        ,forecast_datetime
        ,forecast_rank
        ,ROW_NUMBER() over (
            partition by forecast_date, hour_ending
            order by forecast_rank desc
        ) as rn
    from meteologica_cleaned.meteologica_pjm_demand_forecast_ecmwf_ens_hourly
    where region = (select region from params)
      and forecast_date >= (CURRENT_TIMESTAMP AT TIME ZONE (select timezone from params))::date
)
select
    forecast_datetime
    ,forecast_date
    ,hour_ending
    ,forecast_rank
    ,forecast_execution_datetime
    ,forecast_load_average_mw
    ,forecast_load_top_mw
    ,forecast_load_bottom_mw
from ranked
where rn = 1
order by forecast_datetime, forecast_execution_datetime desc
