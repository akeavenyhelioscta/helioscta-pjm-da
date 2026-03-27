with params as (
    select
        nullif(regexp_replace('{start_date}', '[^0-9-]', '', 'g'), '')::date as start_date,
        nullif(regexp_replace('{end_date}', '[^0-9-]', '', 'g'), '')::date as end_date
)
select
    m.interval_start_local::date as date
    ,extract(hour from m.interval_end_local)::int as hour_ending
    ,m.solar
    ,m.wind
    ,m.gas
    ,m.coal
    ,m.nuclear
    ,m.hydro
    ,m.oil
    ,m.storage
    ,m.other
    ,m.other_renewables
    ,m.multiple_fuels
from gridstatus.pjm_fuel_mix_hourly m
cross join params p
where (p.start_date is null or m.interval_start_local::date >= p.start_date)
  and (p.end_date is null or m.interval_start_local::date <= p.end_date)
order by m.interval_start_local
