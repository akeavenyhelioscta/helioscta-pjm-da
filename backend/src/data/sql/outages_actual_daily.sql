with params as (
    select
        case
            when left('{region}', 1) = chr(123) and right('{region}', 1) = chr(125) then 'RTO'
            else coalesce(nullif('{region}', ''), 'RTO')
        end::text as region,
        nullif(regexp_replace('{start_date}', '[^0-9-]', '', 'g'), '')::date as start_date,
        nullif(regexp_replace('{end_date}', '[^0-9-]', '', 'g'), '')::date as end_date
)
select
    o.date
    ,o.region
    ,o.total_outages_mw
    ,o.planned_outages_mw
    ,o.maintenance_outages_mw
    ,o.forced_outages_mw
from pjm_cleaned.pjm_outages_actual_daily o
cross join params p
where o.region = p.region
  and (p.start_date is null or o.date >= p.start_date)
  and (p.end_date is null or o.date <= p.end_date)
order by o.date
