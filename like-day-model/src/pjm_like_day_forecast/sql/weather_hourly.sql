select
    date
    ,hour_ending
    ,station_name
    ,temp
    ,feels_like_temp
    ,dew_point_temp
    ,wind_speed_mph
    ,relative_humidity
    ,cloud_cover_pct
from {schema}.source_v1_hourly_observed_temp
where region = '{region}'
    and station_name = '{station}'
order by date, hour_ending
