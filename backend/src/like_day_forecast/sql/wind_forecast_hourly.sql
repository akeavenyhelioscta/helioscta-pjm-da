with params as (
    select
        nullif(regexp_replace('{start_date}', '[^0-9-]', '', 'g'), '')::date as start_date,
        nullif(regexp_replace('{end_date}', '[^0-9-]', '', 'g'), '')::date as end_date
)
select
    w.interval_start_local::date as date
    ,extract(hour from w.interval_end_local)::int as hour_ending
    ,w.wind_forecast
from gridstatus.pjm_wind_forecast_hourly w
cross join params p
where w.publish_time_local::date = w.interval_start_local::date - interval '1 day'
  and (p.start_date is null or w.interval_start_local::date >= p.start_date)
  and (p.end_date is null or w.interval_start_local::date <= p.end_date)
order by w.interval_start_local
