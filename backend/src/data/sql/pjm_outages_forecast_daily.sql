-- PJM outage forecast daily — 7-day generation outage forecast by type.
-- Pulls from the dbt mart with forecast_rank for vintage ordering.
-- Parameters: region (optional, NULL = all regions), lookback_days for execution window.
with params as (
    select
        case
            when left('{region}', 1) = chr(123) and right('{region}', 1) = chr(125) then null
            else nullif('{region}', '')
        end::text as region,
        coalesce(nullif(regexp_replace('{lookback_days}', '[^0-9-]', '', 'g'), '')::int, 14) as lookback_days
)
select
    f.forecast_rank,
    f.forecast_execution_date,
    f.forecast_date,
    f.forecast_day_number,
    f.region,
    f.total_outages_mw,
    f.planned_outages_mw,
    f.maintenance_outages_mw,
    f.forced_outages_mw
from pjm_cleaned.pjm_outages_forecast_daily f
cross join params p
where (p.region is null or f.region = p.region)
  and f.forecast_execution_date >= current_date - p.lookback_days
order by f.forecast_date, f.forecast_execution_date desc, f.region asc
