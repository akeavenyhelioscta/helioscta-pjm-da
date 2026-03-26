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
    l.date
    ,l.hour_ending
    ,l.region
    ,l.rt_load_mw
from pjm_cleaned.pjm_load_rt_metered_hourly l
cross join params p
where l.region = p.region
  and (p.start_date is null or l.date >= p.start_date)
  and (p.end_date is null or l.date <= p.end_date)
order by l.date, l.hour_ending
