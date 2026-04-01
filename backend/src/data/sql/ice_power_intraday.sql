-- ice_power_intraday.sql
-- Full intraday snapshot tape for ICE PJM power products.
-- Parameters: {lookback_days}, {symbols}

with tape as (
    select
        iq.trade_date::date                                           as trade_date,
        iq.symbol,
        iq.snapshot_at,
        (iq.snapshot_at at time zone 'America/New_York')::time        as time_et,
        iq.data_type,
        iq.value
    from ice_python.intraday_quotes iq
    where iq.trade_date >= current_date - coalesce(nullif('{lookback_days}', '')::int, 7)
      and ('{symbols}' = '' or iq.symbol = any(string_to_array('{symbols}', ',')))
      and iq.data_type not in ('Settle', 'Recent Settlement')
),

pivoted as (
    select
        trade_date,
        symbol,
        snapshot_at,
        time_et,
        max(case when data_type = 'Bid'    then value end) as bid,
        max(case when data_type = 'Ask'    then value end) as ask,
        max(case when data_type = 'Last'   then value end) as last_px,
        max(case when data_type = 'VWAP'   then value end) as vwap,
        max(case when data_type = 'Volume' then value end) as volume,
        max(case when data_type = 'High'   then value end) as high,
        max(case when data_type = 'Low'    then value end) as low,
        max(case when data_type = 'Open'   then value end) as open_px
    from tape
    group by trade_date, symbol, snapshot_at, time_et
)

select
    trade_date,
    symbol,
    snapshot_at,
    time_et,
    bid,
    ask,
    round((ask - bid)::numeric, 2)                                                                       as spread,
    last_px,
    vwap,
    high,
    low,
    open_px,
    volume,
    round((last_px - lag(last_px) over (partition by symbol, trade_date order by snapshot_at))::numeric, 2) as last_chg
from pivoted
order by symbol, trade_date, snapshot_at
