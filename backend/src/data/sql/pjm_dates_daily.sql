with params as (
    select
        coalesce(nullif(regexp_replace('{start_date}', '[^0-9-]', '', 'g'), '')::date, '2022-01-01'::date) as start_date,
        nullif(regexp_replace('{end_date}', '[^0-9-]', '', 'g'), '')::date as end_date
),
ranked as (
    select
        d.date
        ,d.day_of_week_number
        ,d.is_weekend
        ,d.is_nerc_holiday
        ,d.summer_winter
        ,row_number() over (partition by d.date order by d.date desc) as rn
    from {schema}.pjm_dates_daily d
    cross join params p
    where (p.start_date is null or d.date >= p.start_date)
      and (p.end_date is null or d.date <= p.end_date)
)
select
    date
    ,day_of_week_number
    ,is_weekend
    ,is_nerc_holiday
    ,summer_winter
from ranked
where rn = 1
order by date
