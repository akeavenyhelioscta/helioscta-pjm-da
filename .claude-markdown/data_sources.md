# Data Sources

## Schema: `dbt_pjm_v1_2026_feb_19`

All staging views live in a single dbt schema and are **independently queryable**. Each view owns one data domain. The backend composes features by joining only the sources it needs.

---

### Utility Views

| View | Grain | Key Columns |
|---|---|---|
| `utils_v1_pjm_dates_daily` | 1 row per calendar date | `date`, `year`, `year_month`, `summer_winter`, `month`, `day_of_week`, `day_of_week_number`, `is_weekend`, `is_nerc_holiday`, `eia_storage_week` |
| `utils_v1_pjm_dates_hourly` | 1 row per calendar date x hour | _(same as daily + `hour_ending`)_ |
| `utils_v1_pjm_load_regions` | reference / lookup | PJM load region definitions |

---

### LMP Views

| View | Grain | Dimensions | Measures |
|---|---|---|---|
| `staging_v1_pjm_lmps_daily` | date x hub x market x period | `hub` (12 hubs), `market` (da/rt/dart), `period` (flat/onpeak/offpeak) | `lmp_total`, `lmp_system_energy_price`, `lmp_congestion_price`, `lmp_marginal_loss_price` |
| `staging_v1_pjm_lmps_hourly` | date x hour_ending x hub x market | `hub`, `market` | same 4 LMP measures |

**Hubs:** AEP-DAYTON, AEP GEN, ATSI GEN, CHICAGO GEN, CHICAGO, DOMINION, EASTERN, NEW JERSEY, N ILLINOIS, OHIO, WESTERN, WEST INT

**Date range (current):** 2025-09-22 to 2026-02-23 (~155 distinct dates, ~8,100 daily rows)

---

### Load Views

| View | Grain | Notes |
|---|---|---|
| `staging_v1_pjm_load_da_daily` | date | Day-ahead load forecast (daily avg) |
| `staging_v1_pjm_load_da_hourly` | date x hour_ending | Day-ahead load forecast (hourly) |
| `staging_v1_pjm_load_rt_metered_hourly` | date x hour_ending | Real-time metered load |
| `staging_v1_pjm_load_rt_instantaneous_hourly` | date x hour_ending | Real-time instantaneous load |
| `staging_v1_pjm_load_rt_prelim_hourly` | date x hour_ending | Real-time preliminary load |
| `staging_v1_pjm_load_forecast_hourly` | date x hour_ending | PJM published load forecast |
| `staging_v1_gridstatus_pjm_load_forecast_hourly` | date x hour_ending | GridStatus load forecast |

---

## Design Principles

1. **Sources stay separate.** Each view is one data domain. No monolithic "everything" query.
2. **Backend composes.** The Python backend picks which sources to join based on what the analysis needs.
3. **Incremental complexity.** Start with LMP only. Add load, gas, generation, outages as separate feature modules.
4. **Schema versioning.** The schema name (`dbt_pjm_v1_2026_feb_19`) pins the dbt build. A new build produces a new schema.

---

## Comparison to v0 (archived)

| | v0 (`pjm_v0_2025_nov_08`) | v1 (`dbt_pjm_v1_2026_feb_19`) |
|---|---|---|
| LMPs | Baked into `marts_western_hub_hourly` (Western Hub only) | Separate view, all 12 hubs, daily + hourly |
| Load | Baked into marts | Separate views per source (DA, RT metered, RT instantaneous, forecast) |
| Gas | In marts | _Not yet available_ |
| Generation | In marts | _Not yet available_ |
| Outages | In marts | _Not yet available_ |
| Dates | Generated inline via CTE | Dedicated utility views |
| Query pattern | One 382-line SQL query joining everything | Compose small queries per source |
