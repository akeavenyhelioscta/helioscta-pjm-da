with params as (
    select
        coalesce(nullif(regexp_replace('{start_date}', '[^0-9-]', '', 'g'), '')::date, '2022-01-01'::date) as start_date,
        nullif(regexp_replace('{end_date}', '[^0-9-]', '', 'g'), '')::date as end_date
),
base as (
    select
        s.interval_start_local::date as date
        ,extract(hour from s.interval_end_local)::int as hour_ending
        ,s.publish_time_local
        ,s.updated_at
        ,s.solar_forecast
        ,s.solar_forecast_btm
    from gridstatus.pjm_solar_forecast_hourly s
    cross join params p
    where s.publish_time_local::date = s.interval_start_local::date - interval '1 day'
      and (p.start_date is null or s.interval_start_local::date >= p.start_date)
      and (p.end_date is null or s.interval_start_local::date <= p.end_date)
),
ranked as (
    select
        b.date
        ,b.hour_ending
        ,b.solar_forecast
        ,b.solar_forecast_btm
        ,row_number() over (
            partition by b.date, b.hour_ending
            order by b.publish_time_local desc, b.updated_at desc nulls last
        ) as rn
    from base b
)
select
    date
    ,hour_ending
    ,solar_forecast
    ,solar_forecast_btm
from ranked
where rn = 1
order by date, hour_ending
