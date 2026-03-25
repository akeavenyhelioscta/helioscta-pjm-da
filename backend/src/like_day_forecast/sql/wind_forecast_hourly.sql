select
    interval_start_local::date as date
    ,extract(hour from interval_end_local)::int as hour_ending
    ,wind_forecast
from gridstatus.pjm_wind_forecast_hourly
where publish_time_local::date = interval_start_local::date - interval '1 day'
order by interval_start_local
