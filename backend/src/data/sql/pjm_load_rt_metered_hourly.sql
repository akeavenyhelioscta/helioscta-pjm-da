with params as (
    select
        case
            when left('{region}', 1) = chr(123) and right('{region}', 1) = chr(125) then null
            else nullif('{region}', '')
        end::text as region,
        coalesce(nullif(regexp_replace('{start_date}', '[^0-9-]', '', 'g'), '')::date, '2022-01-01'::date) as start_date,
        nullif(regexp_replace('{end_date}', '[^0-9-]', '', 'g'), '')::date as end_date
),
ranked as (
    select
        l.date
        ,l.hour_ending
        ,l.region
        ,l.rt_load_mw
        ,row_number() over (
            partition by l.date, l.hour_ending, l.region
            order by l.date desc
        ) as rn
    from {schema}.pjm_load_rt_metered_hourly l
    cross join params p
    where (p.region is null or l.region = p.region)
      and (p.start_date is null or l.date >= p.start_date)
      and (p.end_date is null or l.date <= p.end_date)
)
select
    date
    ,hour_ending
    ,region
    ,rt_load_mw
from ranked
where rn = 1
order by date, hour_ending
