with params as (
    select
        case
            when left('{hub}', 1) = chr(123) and right('{hub}', 1) = chr(125) then 'WESTERN HUB'
            else coalesce(nullif('{hub}', ''), 'WESTERN HUB')
        end::text as hub,
        case
            when left('{market}', 1) = chr(123) and right('{market}', 1) = chr(125) then 'da'
            else coalesce(nullif('{market}', ''), 'da')
        end::text as market,
        nullif(regexp_replace('{start_date}', '[^0-9-]', '', 'g'), '')::date as start_date,
        nullif(regexp_replace('{end_date}', '[^0-9-]', '', 'g'), '')::date as end_date
)
select
    l.date
    ,l.hour_ending
    ,l.hub
    ,l.market
    ,l.lmp_total
    ,l.lmp_system_energy_price
    ,l.lmp_congestion_price
    ,l.lmp_marginal_loss_price
from pjm_cleaned.pjm_lmps_hourly l
cross join params p
where l.hub = p.hub
  and l.market = p.market
  and (p.start_date is null or l.date >= p.start_date)
  and (p.end_date is null or l.date <= p.end_date)
order by l.date, l.hour_ending
