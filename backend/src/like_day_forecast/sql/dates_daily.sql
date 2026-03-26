with params as (
    select
        nullif(regexp_replace('{start_date}', '[^0-9-]', '', 'g'), '')::date as start_date,
        nullif(regexp_replace('{end_date}', '[^0-9-]', '', 'g'), '')::date as end_date
)
select
    d.date
    ,d.day_of_week_number
    ,d.is_weekend
    ,d.is_nerc_holiday
    ,d.summer_winter
from pjm_cleaned.pjm_dates_daily d
cross join params p
where (p.start_date is null or d.date >= p.start_date)
  and (p.end_date is null or d.date <= p.end_date)
order by d.date
