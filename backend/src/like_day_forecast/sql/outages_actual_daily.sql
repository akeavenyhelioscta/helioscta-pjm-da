select
    date
    ,region
    ,total_outages_mw
    ,planned_outages_mw
    ,maintenance_outages_mw
    ,forced_outages_mw
from {schema}.pjm_outages_actual_daily
where region = '{region}'
order by date
