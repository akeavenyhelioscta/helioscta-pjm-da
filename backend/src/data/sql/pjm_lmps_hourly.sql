with params as (
    select
        case
            when left('{hub}', 1) = chr(123) and right('{hub}', 1) = chr(125) then null
            else nullif('{hub}', '')
        end::text as hub,
        case
            when left('{market}', 1) = chr(123) and right('{market}', 1) = chr(125) then 'da'
            else coalesce(nullif('{market}', ''), 'da')
        end::text as market,
        coalesce(nullif(regexp_replace('{start_date}', '[^0-9-]', '', 'g'), '')::date, '2022-01-01'::date) as start_date,
        nullif(regexp_replace('{end_date}', '[^0-9-]', '', 'g'), '')::date as end_date
),
ranked as (
    select
        l.date
        ,l.hour_ending
        ,l.hub
        ,l.market
        ,l.lmp_total
        ,l.lmp_system_energy_price
        ,l.lmp_congestion_price
        ,l.lmp_marginal_loss_price
        ,row_number() over (
            partition by l.date, l.hour_ending, l.hub, l.market
            order by l.datetime desc
        ) as rn
    from {schema}.pjm_lmps_hourly l
    cross join params p
    where (p.hub is null or l.hub = p.hub)
      and l.market = p.market
      and (p.start_date is null or l.date >= p.start_date)
      and (p.end_date is null or l.date <= p.end_date)
)
select
    date
    ,hour_ending
    ,hub
    ,market
    ,lmp_total
    ,lmp_system_energy_price
    ,lmp_congestion_price
    ,lmp_marginal_loss_price
from ranked
where rn = 1
order by date, hour_ending
