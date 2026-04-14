with params as (
    select
        coalesce(nullif(regexp_replace('{start_date}', '[^0-9-]', '', 'g'), '')::date, '2022-01-01'::date) as start_date,
        nullif(regexp_replace('{end_date}', '[^0-9-]', '', 'g'), '')::date as end_date
),
base as (
    select
        w.interval_start_local::date as date
        ,case
            when extract(hour from w.interval_end_local)::int = 0 then 24
            else extract(hour from w.interval_end_local)::int
         end as hour_ending
        ,w.publish_time_local
        ,w.updated_at
        ,w.wind_forecast
    from gridstatus.pjm_wind_forecast_hourly w
    cross join params p
    where w.publish_time_local::date = w.interval_start_local::date - interval '1 day'
      and (p.start_date is null or w.interval_start_local::date >= p.start_date)
      and (p.end_date is null or w.interval_start_local::date <= p.end_date)
),
ranked as (
    select
        b.date
        ,b.hour_ending
        ,b.wind_forecast
        ,row_number() over (
            partition by b.date, b.hour_ending
            order by b.publish_time_local desc, b.updated_at desc nulls last
        ) as rn
    from base b
)
select
    date
    ,hour_ending
    ,wind_forecast
from ranked
where rn = 1
order by date, hour_ending
