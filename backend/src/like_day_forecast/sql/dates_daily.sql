select
    date
    ,day_of_week_number
    ,is_weekend
    ,is_nerc_holiday
    ,summer_winter
from {schema}.pjm_dates_daily
order by date
