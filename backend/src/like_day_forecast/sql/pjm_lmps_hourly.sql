select
    date
    ,hour_ending
    ,hub
    ,market
    ,lmp_total
    ,lmp_system_energy_price
    ,lmp_congestion_price
    ,lmp_marginal_loss_price
from {schema}.pjm_lmps_hourly

WHERE
    hub = '{hub}'
    AND market = '{market}'