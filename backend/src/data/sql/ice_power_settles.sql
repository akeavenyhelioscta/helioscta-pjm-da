-- ice_power_settles.sql
-- Daily settlement prices for ICE PJM power products.
-- Parameters: {lookback_days}, {symbols}

with raw as (
    select
        iq.trade_date::date                    as trade_date,
        iq.symbol,
        iq.snapshot_at,
        iq.data_type,
        iq.value
    from ice_python.intraday_quotes iq
    where iq.trade_date >= current_date - coalesce(nullif('{lookback_days}', '')::int, 30)
      and ('{symbols}' = '' or iq.symbol = any(string_to_array('{symbols}', ',')))
),

-- Get the official Settle value per (trade_date, symbol) if it exists
settles as (
    select
        trade_date,
        symbol,
        max(value) as settle
    from raw
    where data_type = 'Settle'
    group by trade_date, symbol
),

-- Get prior day's settlement (carried on every snapshot as "Recent Settlement")
prior_settles as (
    select distinct on (trade_date, symbol)
        trade_date,
        symbol,
        value as prior_settle
    from raw
    where data_type = 'Recent Settlement'
    order by trade_date, symbol, snapshot_at desc
),

-- Last snapshot's VWAP as fallback when no Settle row exists
last_vwap as (
    select distinct on (trade_date, symbol)
        trade_date,
        symbol,
        value as last_vwap
    from raw
    where data_type = 'VWAP'
    order by trade_date, symbol, snapshot_at desc
),

-- End-of-day summary stats from the last snapshot
eod_stats as (
    select
        trade_date,
        symbol,
        max(case when data_type = 'High'   then value end) as high,
        max(case when data_type = 'Low'    then value end) as low,
        max(case when data_type = 'Volume' then value end) as volume
    from raw
    where data_type in ('High', 'Low', 'Volume')
    group by trade_date, symbol
)

select
    td.trade_date,
    td.symbol,
    coalesce(s.settle, lv.last_vwap)                                      as settle,
    ps.prior_settle,
    lv.last_vwap                                                           as vwap,
    es.high,
    es.low,
    es.volume,
    round((coalesce(s.settle, lv.last_vwap) - ps.prior_settle)::numeric, 2) as settle_vs_prior
from (
    select distinct trade_date, symbol from raw
) td
left join settles       s  on s.trade_date  = td.trade_date and s.symbol  = td.symbol
left join prior_settles ps on ps.trade_date = td.trade_date and ps.symbol = td.symbol
left join last_vwap     lv on lv.trade_date = td.trade_date and lv.symbol = td.symbol
left join eod_stats     es on es.trade_date = td.trade_date and es.symbol = td.symbol
order by td.trade_date, td.symbol
