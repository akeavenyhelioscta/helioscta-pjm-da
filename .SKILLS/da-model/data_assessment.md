# Data Assessment for DA LMP Probabilistic Forecasting Model -- PJM Western Hub

**Date:** 2026-02-25
**Target:** Day-ahead hourly LMP price forecasting for PJM Western Hub with probabilistic output (quantiles/intervals)
**Database:** Azure PostgreSQL (`helioscta-test-pg.postgres.database.azure.com`)

---

## 1. Current Data Inventory -- `dbt_pjm_v1_2026_feb_19` Schema

This schema contains 12 views: 3 utility views, 2 LMP views, and 7 load views.

### 1.1 LMP Views (PRIMARY TARGET)

| View | Rows | Date Range | Distinct Dates | Grain | Notes |
|---|---|---|---|---|---|
| `staging_v1_pjm_lmps_hourly` | 3,835,968 | 2014-01-01 to 2026-02-25 | 4,439 | date x hour_ending x hub x market | 12 hubs, 3 markets (da/rt/dart) |
| `staging_v1_pjm_lmps_daily` | 479,340 | 2014-01-01 to 2026-02-25 | -- | date x hub x market x period | Periods: flat/onpeak/offpeak |

**Western Hub DA LMP Summary by Year:**

| Year | Rows | Dates | Null LMPs | Avg LMP | Min LMP | Max LMP |
|---|---|---|---|---|---|---|
| 2014 | 8,760 | 365 | 0 | $51.02 | $5.00 | $949.08 |
| 2015 | 8,760 | 365 | 0 | $35.82 | $3.85 | $415.00 |
| 2016 | 8,784 | 366 | 0 | $29.22 | $3.50 | $121.70 |
| 2017 | 8,760 | 365 | 0 | $29.73 | $9.61 | $225.71 |
| 2018 | 8,760 | 365 | 0 | $36.45 | $7.00 | $331.30 |
| 2019 | 8,784 | 365 | 0 | $26.74 | $9.81 | $154.82 |
| 2020 | 8,784 | 366 | 0 | $20.95 | $6.80 | $83.89 |
| 2021 | 8,760 | 365 | 0 | $38.92 | $14.62 | $236.02 |
| 2022 | 8,760 | 365 | 0 | $73.09 | $15.43 | $469.10 |
| 2023 | 8,760 | 365 | 0 | $33.07 | $9.03 | $315.87 |
| 2024 | 8,784 | 366 | 0 | $33.83 | $9.45 | $314.66 |
| 2025 | 8,784 | 365 | 0 | $50.35 | $11.85 | $550.23 |
| 2026 | 1,344 | 56 | 0 | $128.22 | $19.55 | $2,323.34 |

**Data quality:** Excellent. Zero nulls across 12+ years. Hourly completeness is 24 hours/day for all but 12 DST-transition days (23 hours). Hub name is `WESTERN HUB` (not `WESTERN`).

**Columns:** `date`, `hour_ending`, `hub`, `market`, `lmp_total`, `lmp_system_energy_price`, `lmp_congestion_price`, `lmp_marginal_loss_price`

### 1.2 Load Views

| View | Rows | Date Range | Distinct Dates | Grain | Region Dim |
|---|---|---|---|---|---|
| `staging_v1_pjm_load_da_hourly` | 208,364 | 2020-01-01 to 2026-02-25 | 2,245 | date x hour x region | RTO, WEST, MIDATL, SOUTH |
| `staging_v1_pjm_load_da_daily` | 34,728 | 2020-01-01 to 2026-02-25 | 2,245 | date x region | Same |
| `staging_v1_pjm_load_rt_metered_hourly` | 425,904 | 2014-01-01 to 2026-02-23 | 4,437 | date x hour x region | Same |
| `staging_v1_pjm_load_rt_instantaneous_hourly` | 716 | 2026-02-18 to 2026-02-25 | 8 | date x hour x region | (very recent only) |
| `staging_v1_pjm_load_rt_prelim_hourly` | 672 | 2026-02-18 to 2026-02-24 | 7 | date x hour x region | (very recent only) |
| `staging_v1_pjm_load_forecast_hourly` | 1,396,944 | 2026-02-18 to 2026-03-03 | 14 | date x hour x region | Multiple forecasts per hour |
| `staging_v1_gridstatus_pjm_load_forecast_hourly` | 109,344 | 2026-02-18 to 2026-03-03 | 14 | date x hour x region | GridStatus source |

**DA Load (RTO) completeness:** 24 hours/day for all but 6 DST days (23 hours). Zero nulls. Full coverage from 2020 to present.

**Key limitation:** RT metered load has deep history (2014+), but DA load only starts 2020. The load forecast views have only ~2 weeks of data (recent snapshot).

### 1.3 Utility Views

| View | Rows | Date Range | Notes |
|---|---|---|---|
| `utils_v1_pjm_dates_daily` | 6,205 | 2010-01-01 to 2026-12-31 | Calendar features: day_of_week, is_weekend, is_nerc_holiday, summer_winter, etc. |
| `utils_v1_pjm_dates_hourly` | -- | Same + hour_ending | Hourly grain version |
| `utils_v1_pjm_load_regions` | -- | -- | Reference/lookup for PJM load regions |

---

## 2. Source Schema Inventory -- `pjm` Schema

The `pjm` schema has 18 base tables. These are the raw/source tables from which the staging views are derived, plus additional data domains not yet promoted to staging.

### 2.1 Already Surfaced in Staging

| Table | Rows | Date Range | Staging Equivalent |
|---|---|---|---|
| `da_hrl_lmps` | 1,279,008 | 2014-01-01 to 2026-02-25 | `staging_v1_pjm_lmps_hourly` (DA portion) |
| `rt_settlements_verified_hourly_lmps` | 1,285,632 | 2014-01-01 to 2026-02-25 | `staging_v1_pjm_lmps_hourly` (RT portion) |
| `rt_unverified_hourly_lmps` | 147,756 | 2024-09-29 to 2026-02-25 | Supplement to RT verified LMPs |
| `hourly_load_metered` | 3,234,860 | 2014-01-01 to 2026-02-24 | `staging_v1_pjm_load_rt_metered_hourly` |
| `hourly_load_prelim` | 1,253,337 | 2011-08-23 to 2026-02-25 | `staging_v1_pjm_load_rt_prelim_hourly` |
| `seven_day_load_forecast` | 10,788,825 | 2024-10-16 to 2025-07-15 | `staging_v1_pjm_load_forecast_hourly` |

### 2.2 NOT Yet in Staging (High-Value for Forecasting)

| Table | Rows | Date Range | Grain | Description |
|---|---|---|---|---|
| `wind_gen` | 283,194 | 2020-01-01 to 2025-10-01 | hourly x area | Actual wind generation (MW) by area (RTO, WEST, MIDATL, SOUTH, RFC, OTHER) |
| `solar_gen` | 301,026 | 2020-01-01 to 2025-10-01 | hourly x area | Actual solar generation (MW) by area |
| `hrl_dmd_bids` | 154,490 | 2020-01-01 to 2026-02-25 | hourly x area | Hourly DA demand bids (MW). Areas: PJM_RTO, WESTERN_REGION, MID_ATLANTIC_REGION |
| `seven_day_outage_forecast` | 47,217 | 2020-01-01 to 2026-03-03 | daily x region | Outage forecast: total, planned, maintenance, forced (MW). Regions: PJM RTO, Western, Mid Atlantic |
| `long_term_outages` | 204,295 | 2020-01-01 to 2026-05-27 | daily | Gen outage forecast by RTO/West/Other regions |
| `five_min_tie_flows` | 1,029,963 | 2025-03-31 to 2026-02-25 | 5-min x tie | Net interchange flows (actual + scheduled MW). ~20 tie names including PJM RTO, PJM MISO, NYIS |
| `dispatched_reserves` | 85,375 | 2025-07-15 to 2026-02-22 | sub-hourly x area | DA dispatched reserves: Sync, Primary, 30-Min (quantity, requirement, clearing price) |
| `operational_reserves` | 100,000 | 2025-08-13 to 2026-02-22 | 5-min x reserve | RT operational reserve levels (MW) |
| `real_time_dispatched_reserves` | 83,520 | 2025-07-14 to 2026-02-20 | 5-min x area | RT dispatched reserves with deficit tracking |
| `instantaneous_load` | 739,650 | 2025-07-15 to 2025-11-14 | 5-min | RT instantaneous load (limited range) |
| `five_min_instantaneous_load_v1_2025_oct_15` | 996,525 | 2025-10-10 to 2026-02-25 | 5-min x area | Newer version of instantaneous load |

---

## 3. Adjacent Schemas with Valuable Data

### 3.1 Natural Gas Prices -- `ice` Schema

| Table | Key Hubs for PJM | Rows | Date Range | Grain |
|---|---|---|---|---|
| `next_day_gas` | M3, Transco Z6 NY, HH, Dom, Z5, Dominion North | 127,742 | 2010-01-01 to 2026-02-25 | daily x hub |

**Key hub details (VWAP Close):**

| Hub | Records | From | To | Avg Price |
|---|---|---|---|---|
| M3 (TETCO) | 5,901 | 2010-01-01 | 2026-02-25 | $3.44 |
| HH (Henry Hub) | 5,901 | 2010-01-01 | 2026-02-25 | $3.29 |
| Transco Z6 NY | 5,901 | 2010-01-01 | 2026-02-25 | $3.86 |
| Z5 (Transco Zone 5) | 5,901 | 2010-01-01 | 2026-02-25 | $3.65 |
| Dom (Dominion SP) | 4,803 | 2013-01-01 | 2026-02-25 | $2.55 |

**Relevance:** M3 is the benchmark gas hub for PJM Western Hub power pricing. Gas is the marginal fuel ~50% of hours, making it the single most important exogenous feature for LMP forecasting.

### 3.2 Fuel Mix -- `gridstatus` Schema

| Table | Rows | Date Range | Grain | Fuel Types |
|---|---|---|---|---|
| `pjm_fuel_mix_hourly` | 53,882 | 2020-01-01 to 2026-02-25 | hourly | coal, gas, hydro, multiple_fuels, nuclear, oil, solar, storage, wind, other, other_renewables |

**Data quality:** Zero nulls across all fuel columns. Near-complete hourly coverage (8,700+ rows/yr).

### 3.3 Weather -- `wsi` Schema

| Table | PJM Rows | Date Range | Grain | Key Measures |
|---|---|---|---|---|
| `daily_observed_temp_v3_2025_09_08` | 398,548 | 1993-01-01 to 2026-02-24 | daily x station | min, max, avg temp, CDD, HDD, precip |
| `hourly_observed_temp_v2_20250722` | 9,076,236 | 1995-01-01 to 2026-02-25 | hourly x station | temp, dew point, wind chill, heat index, wind speed, cloud cover, precip |
| `weighted_temp_daily_forecast_iso_wsi_v2_2026_jan_12` | 9,960 (PJM) | 2026-01-13 to 2026-03-11 | daily x region | min/max temp, HDD, CDD, heat index forecasts |

**PJM regions available:** PJM, PJM EAST, PJM WEST, PJM SOUTH

**Relevance:** Weather drives load, which drives LMP. Temperature (especially extremes) is a critical feature for capturing seasonal and intra-day price dynamics.

### 3.4 GridStatus PJM Renewables Forecasts

| Table | Rows | Date Range | Grain | Measures |
|---|---|---|---|---|
| `pjm_solar_forecast_hourly` | 341,282 | 2025-04-16 to 2026-02-27 | hourly | solar_forecast, solar_forecast_btm |
| `pjm_wind_forecast_hourly` | 1,787,270 | 2025-04-16 to 2026-02-27 | hourly | wind_forecast |

### 3.5 GridStatus PJM Load

| Table | Rows | Date Range | Grain |
|---|---|---|---|
| `pjm_load` | 317,140 | 2023-02-07 to 2026-02-25 | 5-min, with zonal breakdown (30+ columns) |
| `pjm_load_forecast` | 771,399 | 2025-04-16 to 2026-03-04 | hourly, with zonal breakdown |

---

## 4. Data Gaps -- Critical Missing Data

### 4.1 Entirely Missing from Database

| Data Source | Impact | Notes |
|---|---|---|
| **PJM capacity/generation offer data** | High | Public offer data from PJM could reveal supply stack positioning |
| **NYMEX/ICE power futures (on-peak/off-peak)** | Medium | Forward curve provides market expectations; useful for basis modeling |
| **Hourly gas prices (intraday)** | Medium | Only daily next-day gas available; intraday gas moves affect RT LMP |
| **Demand response availability** | Low-Med | DR participation reduces peak loads and suppresses LMP spikes |
| **Transmission constraint data** | Medium | Congestion component of LMP driven by specific constraints |

### 4.2 Present but with Limited History

| Data Source | Available Range | Ideal Range | Gap |
|---|---|---|---|
| DA load (staging) | 2020-01-01+ | 2014-01-01+ | 6 years missing (need to backfill from `pjm.hrl_dmd_bids` or other source) |
| Wind/Solar gen (pjm schema) | 2020-01-01 to 2025-10-01 | Through present | 5 months of recent data missing |
| Fuel mix (gridstatus) | 2020-01-01+ | 2014-01-01+ | 6 years missing (lower priority -- gas prices proxy for fuel dynamics) |
| Tie flows | 2025-03-31+ | 2020-01-01+ | Very limited history |
| Reserves | 2025-07-15+ | 2020-01-01+ | Very limited history |
| Load forecast (staging) | 2026-02-18+ (2 wks) | 2020-01-01+ | Only a rolling snapshot -- not historical |
| Solar/Wind forecasts (gridstatus) | 2025-04-16+ | 2020-01-01+ | ~1 year of history only |
| WSI temp forecast (weighted) | 2026-01-13+ | 2020-01-01+ | Only 6 weeks of history |

---

## 5. Recommended Data Pipeline -- Priority Order

### P0: Must Have (Core Features for MVP Model)

| Source | Schema.Table | Date Range | Action |
|---|---|---|---|
| **DA LMP hourly (target)** | `dbt_pjm_v1_2026_feb_19.staging_v1_pjm_lmps_hourly` | 2014-01-01 to present | Already in staging. Filter `hub='WESTERN HUB'`, `market='da'`. |
| **Calendar features** | `dbt_pjm_v1_2026_feb_19.utils_v1_pjm_dates_daily` | 2010-2026 | Already in staging. |
| **DA load hourly (RTO)** | `dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_da_hourly` | 2020-01-01 to present | Already in staging. History starts 2020 only. |
| **Next-day gas price (M3)** | `ice.next_day_gas` | 2010-01-01 to present | **Promote to staging view.** Filter `hub='M3'`, `data_type='VWAP Close'`. Daily grain -- broadcast to all hours. |
| **RT metered load** | `dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_rt_metered_hourly` | 2014-01-01 to present | Already in staging. Useful for load actuals / lag features. |

### P1: High Value (Significant Predictive Power)

| Source | Schema.Table | Date Range | Action |
|---|---|---|---|
| **Fuel mix hourly** | `gridstatus.pjm_fuel_mix_hourly` | 2020-01-01 to present | **Promote to staging view.** Gas share of generation is a key price signal. |
| **Hourly demand bids** | `pjm.hrl_dmd_bids` | 2020-01-01 to present | **Promote to staging view.** DA demand bid levels reveal market expectations. |
| **Outage forecast (7-day)** | `pjm.seven_day_outage_forecast` | 2020-01-01 to present | **Promote to staging view.** Total/forced/planned outage MW drives supply scarcity. |
| **Wind generation actual** | `pjm.wind_gen` | 2020-01-01 to 2025-10-01 | **Promote to staging view.** Renewables suppress LMP; wind has large hourly variability. |
| **Solar generation actual** | `pjm.solar_gen` | 2020-01-01 to 2025-10-01 | **Promote to staging view.** Solar suppresses midday LMP. |
| **Additional gas hubs** | `ice.next_day_gas` | 2010+ | **Promote to staging view.** Add HH and Transco Z6 NY for basis spread features. |

### P2: Nice to Have (Incremental Improvement)

| Source | Schema.Table | Date Range | Action |
|---|---|---|---|
| **Weather actuals (PJM weighted daily)** | `wsi.daily_observed_temp_v3_2025_09_08` | 1993-01-01 to present | **Promote to staging view.** Temperature drives load (esp. HDD/CDD). |
| **Weather actuals (hourly)** | `wsi.hourly_observed_temp_v2_20250722` | 1995-01-01 to present | Deep hourly history; valuable for temperature-load-price chain. |
| **Tie flows (interchange)** | `pjm.five_min_tie_flows` | 2025-03-31+ | Limited history. Aggregate to hourly. Net imports reduce LMP. |
| **RT LMPs** | `dbt_pjm_v1_2026_feb_19.staging_v1_pjm_lmps_hourly` (rt market) | 2014-01-01 to present | Already in staging. RT-DA spread (DART) as a feature. |
| **Long-term outages** | `pjm.long_term_outages` | 2020-01-01 to 2026-05-27 | Daily gen outage forecast MW by RTO/West/Other. |
| **Solar/Wind forecast** | `gridstatus.pjm_solar_forecast_hourly`, `pjm_wind_forecast_hourly` | 2025-04-16+ | Limited history but forward-looking for operational model. |
| **Reserves** | `pjm.dispatched_reserves`, `pjm.operational_reserves` | 2025-07+ | Very limited history; may be useful once more data accumulates. |

---

## 6. Feature Engineering Opportunities

### 6.1 From LMP History (Target Variable Lags)

| Feature | Description | Rationale |
|---|---|---|
| `lmp_lag_24h` | LMP same hour yesterday | Strong autocorrelation |
| `lmp_lag_168h` | LMP same hour same weekday last week | Weekly seasonality |
| `lmp_lag_8760h` | LMP same hour same date last year | Annual seasonality |
| `lmp_rolling_7d_mean` | 7-day rolling mean of daily avg LMP | Trend capture |
| `lmp_rolling_7d_std` | 7-day rolling std dev | Volatility regime |
| `lmp_rolling_30d_mean` | 30-day rolling mean | Seasonal trend |
| `lmp_same_hour_7d_mean` | Mean of same hour over past 7 days | Hour-specific trend |
| `dart_spread_lag_24h` | DA-RT spread same hour yesterday | Market imbalance signal |
| `lmp_congestion_share` | congestion_price / lmp_total | Congestion regime indicator |
| `lmp_energy_share` | system_energy_price / lmp_total | Marginal fuel cost dominance |

### 6.2 From Calendar / Temporal

| Feature | Description | Rationale |
|---|---|---|
| `hour_ending` | Hour of day (1-24) | Strong intra-day pattern |
| `day_of_week_number` | Day of week (0=Mon, 6=Sun) | Weekday vs weekend effect |
| `is_weekend` | Boolean | Load drops 10-15% on weekends |
| `is_nerc_holiday` | Boolean | Holiday effect similar to weekend |
| `month` | Month (1-12) | Seasonal proxy |
| `summer_winter` | Season flag | Heating vs cooling demand |
| `hour_sin`, `hour_cos` | Cyclical encoding of hour | Smooth hour transitions for models |
| `month_sin`, `month_cos` | Cyclical encoding of month | Smooth seasonal transitions |
| `is_peak_hour` | Hours 7-23 on non-holiday weekdays | On-peak / off-peak regime |

### 6.3 From Load Data

| Feature | Description | Rationale |
|---|---|---|
| `da_load_mw` | DA load forecast for the target hour | Demand is primary LMP driver |
| `da_load_daily_avg` | Daily average DA load | Overall demand level |
| `da_load_peak_ratio` | Hour load / daily peak load | Position in daily load shape |
| `load_ramp` | da_load[h] - da_load[h-1] | Ramp needs drive scarcity |
| `load_forecast_error_lag` | RT actual - DA forecast (lagged) | Systematic forecast bias |
| `load_vs_7d_avg` | Current DA load vs 7-day rolling avg | Demand anomaly |

### 6.4 From Gas Prices

| Feature | Description | Rationale |
|---|---|---|
| `gas_m3_price` | TETCO M3 next-day gas price | Primary marginal fuel cost for PJM |
| `gas_hh_price` | Henry Hub price | National gas benchmark |
| `gas_m3_hh_spread` | M3 minus HH | Regional gas basis |
| `gas_m3_7d_change` | M3 price change over 7 days | Gas price momentum |
| `gas_m3_30d_mean` | 30-day rolling mean M3 | Gas trend |
| `implied_heat_rate` | LMP / gas_price | Market-implied efficiency; mean-reverting |

### 6.5 From Generation / Fuel Mix

| Feature | Description | Rationale |
|---|---|---|
| `gas_gen_share` | gas / total generation | Gas on margin fraction |
| `wind_gen_mw` | Wind generation (RTO) | Wind suppresses LMP |
| `solar_gen_mw` | Solar generation (RTO) | Solar suppresses midday LMP |
| `renewable_share` | (wind + solar) / total | Net renewables penetration |
| `coal_gen_share` | coal / total | Coal vs gas switching signal |

### 6.6 From Outages

| Feature | Description | Rationale |
|---|---|---|
| `total_outage_mw` | Total gen outages (PJM RTO) | Supply reduction -> price increase |
| `forced_outage_mw` | Forced outages only | Unexpected supply loss -> price spikes |
| `outage_change_7d` | Change in outage MW over 7 days | Outage trend |

### 6.7 From Demand Bids

| Feature | Description | Rationale |
|---|---|---|
| `da_demand_bid_mw` | Hourly DA demand bid (PJM RTO) | Realized demand in DA market |
| `bid_vs_forecast` | Demand bid / DA load forecast | Market expectation vs ISO forecast |

---

## 7. Summary of Data Readiness

### What is ready now (MVP can train immediately):

- **12+ years** of hourly DA LMP for Western Hub (2014-2026), zero nulls
- **6+ years** of hourly DA load for RTO (2020-2026)
- **12+ years** of hourly RT metered load for RTO (2014-2026)
- **Full calendar features** (weekday, holiday, season)
- **16+ years** of daily gas prices at M3, HH, Transco Z6 (2010-2026)

### What needs a staging view but is available in source schema:

- Fuel mix hourly (6 years, gridstatus)
- Hourly demand bids (6 years, pjm schema)
- Outage forecasts (6 years, pjm schema)
- Wind/Solar actual generation (5.7 years, pjm schema -- stops Oct 2025)
- Weather actuals (30+ years of daily, hourly available)

### Critical gaps that limit model accuracy:

1. **No historical load forecast data** -- the load forecast views only have ~2 weeks of data. For backtesting, we must use DA load as a proxy (which is the cleared DA schedule, not the forecast).
2. **Wind/Solar actuals stop at Oct 2025** -- 5 months of missing recent data. GridStatus fuel mix covers this gap partially.
3. **No weather forecast history** -- WSI temp forecasts only available from Jan 2026. For backtesting, use observed weather (available) as a proxy for "perfect forecast" scenario.
4. **No transmission constraint data** -- congestion component of LMP is driven by specific flowgate constraints, which are not in the database.

### Recommended MVP feature set (available today, no new data needed):

1. LMP lags and rolling statistics (from staging LMP view)
2. Calendar features (from utility view)
3. DA load and load-shape features (from staging load view, 2020+)
4. Gas price and spread features (from ice.next_day_gas, 2014+)
5. RT load lags (from staging RT metered view, 2014+)

**Effective training window:** 2020-01-01 to 2026-02-25 (~6 years, limited by DA load history)
**Extended training window (LMP + gas + calendar only):** 2014-01-01 to 2026-02-25 (~12 years)
