select
    date
    ,hub
    ,value as price
from ice.next_day_gas
where hub in ({hubs})
    and data_type = 'VWAP Close'
order by date, hub
