select
    date
    ,hub
    ,market
    ,period
    ,lmp_total
    ,lmp_system_energy_price
    ,lmp_congestion_price
    ,lmp_marginal_loss_price
from {schema}.staging_v1_pjm_lmps_daily
where hub = '{hub}'
    and market = '{market}'
order by date, period
