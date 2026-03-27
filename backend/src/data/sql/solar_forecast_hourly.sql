with params as (
    select
        nullif(regexp_replace('{start_date}', '[^0-9-]', '', 'g'), '')::date as start_date,
        nullif(regexp_replace('{end_date}', '[^0-9-]', '', 'g'), '')::date as end_date
)
select
    s.interval_start_local::date as date
    ,extract(hour from s.interval_end_local)::int as hour_ending
    ,s.solar_forecast
    ,s.solar_forecast_btm
from gridstatus.pjm_solar_forecast_hourly s
cross join params p
where s.publish_time_local::date = s.interval_start_local::date - interval '1 day'
  and (p.start_date is null or s.interval_start_local::date >= p.start_date)
  and (p.end_date is null or s.interval_start_local::date <= p.end_date)
order by s.interval_start_local
