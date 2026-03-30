with params as (
    select
        coalesce(nullif(regexp_replace('{start_date}', '[^0-9-]', '', 'g'), '')::date, '2022-01-01'::date) as start_date,
        nullif(regexp_replace('{end_date}', '[^0-9-]', '', 'g'), '')::date as end_date
),
base as (
    select
        m.interval_start_local
        ,m.interval_start_local::date as date
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
        ,m.updated_at
    from gridstatus.pjm_fuel_mix_hourly m
    cross join params p
    where (p.start_date is null or m.interval_start_local::date >= p.start_date)
      and (p.end_date is null or m.interval_start_local::date <= p.end_date)
),
ranked as (
    select
        b.date
        ,b.hour_ending
        ,b.solar
        ,b.wind
        ,b.gas
        ,b.coal
        ,b.nuclear
        ,b.hydro
        ,b.oil
        ,b.storage
        ,b.other
        ,b.other_renewables
        ,b.multiple_fuels
        ,row_number() over (
            partition by b.date, b.hour_ending
            order by b.updated_at desc nulls last, b.interval_start_local desc
        ) as rn
    from base b
)
select
    date
    ,hour_ending
    ,solar
    ,wind
    ,gas
    ,coal
    ,nuclear
    ,hydro
    ,oil
    ,storage
    ,other
    ,other_renewables
    ,multiple_fuels
from ranked
where rn = 1
order by date, hour_ending
