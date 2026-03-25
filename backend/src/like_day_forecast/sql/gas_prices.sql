select
    gas_day as date
    ,tetco_m3_cash as gas_m3_price
    ,hh_cash as gas_hh_price
from ice_python_cleaned.ice_python_next_day_gas_daily
order by gas_day
