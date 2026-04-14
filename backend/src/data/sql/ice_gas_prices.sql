with params as (
    select
        coalesce(nullif(regexp_replace('{start_date}', '[^0-9-]', '', 'g'), '')::date, '2022-01-01'::date) as start_date,
        nullif(regexp_replace('{end_date}', '[^0-9-]', '', 'g'), '')::date as end_date
)
select
    g.gas_day as date
    ,g.tetco_m3_cash as gas_m3_price
    ,g.hh_cash as gas_hh_price
    ,g.transco_zone_5_south_cash as gas_z5s_price
    ,g.agt_cash as gas_agt_price
from ice_python_cleaned.ice_python_next_day_gas_daily g
cross join params p
where (p.start_date is null or g.gas_day >= p.start_date)
  and (p.end_date is null or g.gas_day <= p.end_date)
order by g.gas_day
