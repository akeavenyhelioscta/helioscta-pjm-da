select
    date
    ,hour_ending
    ,region
    ,da_load_mw
from {schema}.pjm_load_da_hourly
where region = '{region}'
order by date, hour_ending
