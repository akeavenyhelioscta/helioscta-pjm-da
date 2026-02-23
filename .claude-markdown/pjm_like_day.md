# PJM Like Day - Redesign

## Overview

Find historically similar ("like") days for a target date based on PJM market data, starting with LMP prices only. Uses nearest-neighbor similarity on daily feature vectors.

**Target variable:** DA LMP Total, Western Hub (flat average)

---

## What Changed from v2 (archived)

| Aspect | v2 (`v2_2025_dec_17`) | Redesign |
|---|---|---|
| Data query | Single 382-line SQL joining LMPs, gas, load, gen, outages, forecasts | One query per data source, composed in Python |
| Schema | `pjm_v0_2025_nov_08` (marts tables) | `dbt_pjm_v1_2026_feb_19` (staging views) |
| Features | 6 mixed features (load, solar, wind, outages, gas M3, gas Z5) | Start with LMP-derived features only, add sources incrementally |
| Hub coverage | Western Hub only | All 12 hubs available, start with Western Hub |
| Daily reshape | Hourly → flatten 24h x N features into single row | Use `staging_v1_pjm_lmps_daily` directly (already daily) |
| Caching | Parquet file cache | _TBD_ |

---

## Phase 1: LMP Only

### Data Source

```
dbt_pjm_v1_2026_feb_19.staging_v1_pjm_lmps_daily
```

Filtered to: `hub = 'WESTERN HUB'`, `period = 'flat'`

### Feature Set

From this single table, pivot the `market` dimension (da / rt / dart) into columns:

| Feature | Source Column | Market Filter | Description |
|---|---|---|---|
| `da_lmp_total` | `lmp_total` | `market = 'da'` | **Target** - Day-ahead total LMP |
| `rt_lmp_total` | `lmp_total` | `market = 'rt'` | Real-time total LMP |
| `dart_lmp_total` | `lmp_total` | `market = 'dart'` | DA-RT spread |
| `da_congestion` | `lmp_congestion_price` | `market = 'da'` | DA congestion component |
| `da_marginal_loss` | `lmp_marginal_loss_price` | `market = 'da'` | DA marginal loss component |
| `rt_congestion` | `lmp_congestion_price` | `market = 'rt'` | RT congestion component |

Join with `utils_v1_pjm_dates_daily` for temporal context:

| Feature | Source Column | Description |
|---|---|---|
| `day_of_week_number` | `day_of_week_number` | 0=Sun .. 6=Sat |
| `is_weekend` | `is_weekend` | Weekend flag |
| `month` | `month` | Month of year |

### SQL Shape (Phase 1)

```sql
SELECT
    d.date,
    d.day_of_week,
    d.day_of_week_number,
    d.is_weekend,
    d.month,
    d.summer_winter,

    -- pivot markets into columns
    da.lmp_total              AS da_lmp_total,
    da.lmp_system_energy_price AS da_system_energy,
    da.lmp_congestion_price   AS da_congestion,
    da.lmp_marginal_loss_price AS da_marginal_loss,

    rt.lmp_total              AS rt_lmp_total,
    rt.lmp_congestion_price   AS rt_congestion,

    dart.lmp_total            AS dart_lmp_total

FROM dbt_pjm_v1_2026_feb_19.utils_v1_pjm_dates_daily d

LEFT JOIN dbt_pjm_v1_2026_feb_19.staging_v1_pjm_lmps_daily da
    ON d.date = da.date AND da.hub = 'WESTERN HUB' AND da.market = 'da' AND da.period = 'flat'

LEFT JOIN dbt_pjm_v1_2026_feb_19.staging_v1_pjm_lmps_daily rt
    ON d.date = rt.date AND rt.hub = 'WESTERN HUB' AND rt.market = 'rt' AND rt.period = 'flat'

LEFT JOIN dbt_pjm_v1_2026_feb_19.staging_v1_pjm_lmps_daily dart
    ON d.date = dart.date AND dart.hub = 'WESTERN HUB' AND dart.market = 'dart' AND dart.period = 'flat'

WHERE da.lmp_total IS NOT NULL
ORDER BY d.date
```

### Pipeline Steps

1. **Pull** - Execute SQL above, return a DataFrame with 1 row per date
2. **Split** - Separate target date row from historical rows
3. **Scale** - StandardScaler on selected feature columns
4. **Similarity** - NearestNeighbors (cosine/euclidean/manhattan) to rank historical days
5. **Output** - Return top-N like days with distance + similarity scores

### Backend Module Structure

```
backend/
  pjm_like_day/
    __init__.py
    configs.py          # target col, feature cols, hub, schema
    data/
      __init__.py
      lmps.py           # pull LMP data (Phase 1)
      # load.py          # Phase 2
      # gas.py           # Phase 3
      # generation.py    # Phase 4
    like_day.py          # split, scale, nearest neighbors
    pipeline.py          # orchestrate pull → like_day → output
```

Each file in `data/` owns one data source. `like_day.py` doesn't care where features came from - it just receives a DataFrame with a date column and feature columns.

---

## Future Phases (not yet)

| Phase | Data Source | New Features |
|---|---|---|
| 2 - Load | `staging_v1_pjm_load_da_daily`, `staging_v1_pjm_load_rt_metered_hourly` | DA load, RT load, net load |
| 3 - Gas | _TBD (not yet in dbt)_ | Next-day gas M3, Z5, basis |
| 4 - Generation | _TBD_ | Solar, wind, thermal mix |
| 5 - Outages | _TBD_ | Total outages MW |
| 6 - Multi-hub | Same LMP views, different hub filters | Cross-hub spreads |

Each phase adds a new file in `data/`, a new feature set in `configs.py`, and a join in `pipeline.py`. The core like-day algorithm (`like_day.py`) doesn't change.

---

## Open Questions

- [ ] What distance metric(s) to support? (v2 had cosine, euclidean, manhattan)
- [ ] Should the dashboard (Dash app) be rebuilt or is CLI/notebook output sufficient for now?
- [ ] Do we want hourly like-day matching (using `staging_v1_pjm_lmps_hourly`) or is daily sufficient?
- [ ] How to handle the limited date range (~155 days)? Is that enough history for meaningful similarity?
