-- DATA_SQL
WITH pjm_lmps_hourly AS (
  SELECT DISTINCT
    date,
    hour_ending,
    hub,
    market,
    lmp_total,
    lmp_system_energy_price,
    lmp_congestion_price,
    lmp_marginal_loss_price
  FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_lmps_hourly
  WHERE hub = $1
    AND market = $2
    AND date >= $3::date
    AND date <= $4::date
)
SELECT * FROM pjm_lmps_hourly
ORDER BY date DESC, hour_ending ASC, hub;

-- HUBS_SQL
SELECT DISTINCT hub
FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_lmps_hourly
ORDER BY hub;

-- MARKETS_SQL
SELECT DISTINCT market
FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_lmps_hourly
ORDER BY market;
