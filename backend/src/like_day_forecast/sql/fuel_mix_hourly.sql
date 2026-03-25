select
    interval_start_local::date as date
    ,extract(hour from interval_end_local)::int as hour_ending
    ,solar
    ,wind
    ,gas
    ,coal
    ,nuclear
    ,hydro
    ,oil
    ,storage
    ,other
    ,other_renewables
    ,multiple_fuels
from gridstatus.pjm_fuel_mix_hourly
order by interval_start_local
