select
    date
    ,hour_ending
    ,region
    ,rt_load_mw
from {schema}.staging_v1_pjm_load_rt_metered_hourly
where region = '{region}'
order by date, hour_ending
