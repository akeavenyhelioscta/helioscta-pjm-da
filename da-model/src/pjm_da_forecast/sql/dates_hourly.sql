select
    date
    ,hour_ending
    ,day_of_week_number
    ,is_weekend
    ,is_nerc_holiday
    ,summer_winter
from {schema}.utils_v1_pjm_dates_hourly
order by date, hour_ending
