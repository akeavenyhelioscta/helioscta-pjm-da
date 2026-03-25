# Current Like-Day Model Pipelines

Two implementations exist: a **backend simple pipeline** (API-facing) and the **research extended pipeline** (full-featured forecasting).

---

## 1. Backend Simple Pipeline

**Location:** `backend/src/pjm_like_day/`
**Entry point:** `pipeline.py → run()`
**Purpose:** Fast API responses for the `/api/like-day` endpoint

### Files

| File | Role |
|------|------|
| `pipeline.py` | Orchestration: pull, merge, filter, rank |
| `like_day.py` | Core KNN similarity algorithm |
| `configs.py` | Defaults (schema, hub, feature columns) |
| `data/lmps.py` | LMP data source (SQL query) |

### Data Flow

1. **Pull & Prefix** — Query DA/RT LMP hourly from Azure PostgreSQL, prefix columns by market (`da_lmp_total`, `rt_lmp_total`, etc.)
2. **Merge** — Inner join across markets on (date, hour_ending)
3. **Filter** — Sequential: hours → date range → day-of-week → month. Split into target date vs historical candidates.
4. **Feature Vectors** — Each day = 24 hourly LMP values. Feature weights dict maps `{market_lmp_col: weight}`.
5. **KNN Ranking** (`find_like_days`):
   - Per-feature raw distance (MAE, RMSE, Euclidean, or Cosine)
   - Z-score normalize across the historical pool
   - Weighted blend: `final_dist = sum(weight * z_norm) / sum(weights)`
   - Select top-N, compute similarity: `1 - (dist - min) / (max - min)`
6. **Output** — Top-N dates with rank/distance/similarity + full hourly LMP profiles for all markets

### Limitations

- LMP-only (no load, gas, weather, calendar features)
- No probabilistic output
- No evaluation metrics
- ~300 lines of code

---

## 2. Research Extended Pipeline

**Location:** `like-day-model/src/pjm_like_day_forecast/`
**Entry point:** `pipelines/forecast.py → run(forecast_date, n_analogs=30, weight_method="inverse_distance")`
**Purpose:** Full-featured probabilistic DA LMP forecasting

### Directory Structure

```
like-day-model/src/pjm_like_day_forecast/
├── pipelines/forecast.py           # Main entry point
├── features/
│   ├── builder.py                  # Orchestrator: pull all data, build & merge features
│   ├── lmp_features.py             # LMP profile, level, volatility, rolling stats
│   ├── load_features.py            # Load level, shape, rolling stats
│   ├── gas_features.py             # Gas prices, spreads, momentum
│   ├── weather_features.py         # Temperature, HDD/CDD, wind
│   ├── calendar_features.py        # Cyclical DOW/season, one-hot DOW, flags
│   ├── composite.py                # Cross-domain: implied heat rate, LMP/load ratio
│   ├── target_load_features.py     # D+1 load (shifted back to reference date)
│   ├── target_weather_features.py  # D+1 weather (shifted back to reference date)
│   └── preprocessing.py            # asinh variance stabilizing transform
├── similarity/
│   ├── engine.py                   # Analog selection: filter → normalize → rank
│   ├── metrics.py                  # Distance functions + weighted blend
│   └── filtering.py               # Pre-filtering: calendar + regime
├── data/
│   ├── lmps_hourly.py              # DA/RT LMP
│   ├── gas_prices.py               # M3, HH, Transco Z6
│   ├── load_da_hourly.py           # DA load forecast (2020+)
│   ├── load_rt_metered_hourly.py   # RT metered load (2014+)
│   ├── weather_hourly.py           # PJM observed weather (1995+)
│   └── dates.py                    # Calendar data
├── evaluation/metrics.py           # MAE, RMSE, MAPE, CRPS, coverage, sharpness
├── utils/
│   ├── azure_postgresql.py         # DB connection pool
│   └── logging_utils.py            # PipelineLogger
├── sql/                            # SQL templates
├── configs.py                      # Feature group weights + defaults
└── settings.py                     # Environment initialization
```

### Pipeline Stages

#### Stage 1: Build Daily Feature Matrix

**`features/builder.py → build_daily_features()`**

Produces one row per date with 100+ features across 8 domains. All feature modules merge on `date` via left join.

| Feature Group | Module | Key Features |
|---------------|--------|-------------|
| **LMP Profile** | `lmp_features.py` | `lmp_profile_h1..h24` (asinh-transformed hourly DA LMP) |
| **LMP Level** | `lmp_features.py` | `lmp_daily_flat`, `lmp_onpeak_avg`, `lmp_offpeak_avg`, `lmp_peak_ratio` |
| **LMP Volatility** | `lmp_features.py` | `lmp_intraday_std`, `lmp_intraday_range`, 7d/30d rolling mean/std, `dart_spread_daily` |
| **Load Level** | `load_features.py` | `load_daily_avg`, `load_daily_peak`, `load_daily_valley`, 7d rolling mean |
| **Load Shape** | `load_features.py` | `load_peak_ratio`, `load_ramp_max` |
| **Gas Price** | `gas_features.py` | `gas_m3_price`, `gas_hh_price`, `gas_m3_hh_spread` |
| **Gas Momentum** | `gas_features.py` | `gas_m3_7d_change`, `gas_m3_30d_mean` |
| **Calendar DOW** | `calendar_features.py` | `dow_sin/cos` (cyclical), `dow_0..6` (one-hot), `is_weekend`, `is_nerc_holiday` |
| **Calendar Season** | `calendar_features.py` | `month_sin/cos`, `day_of_year_sin/cos`, `summer_winter` |
| **Weather Level** | `weather_features.py` | `temp_daily_avg/max/min`, `feels_like_daily_avg`, `wind_speed_daily_avg` |
| **Weather HDD/CDD** | `weather_features.py` | `hdd`, `cdd` (base 65F), `temp_7d_rolling_mean`, `temp_daily_change` |
| **Composite** | `composite.py` | `implied_heat_rate` (sinh(LMP)/gas), `lmp_per_load` (sinh(LMP)/load) |
| **Target Load** | `target_load_features.py` | D+1 load aggregates shifted to date D: `tgt_load_daily_avg/peak/valley`, `tgt_load_change_vs_ref` |
| **Target Weather** | `target_weather_features.py` | D+1 weather shifted to date D: `tgt_temp_daily_avg`, `tgt_hdd/cdd`, `tgt_temp_change_vs_ref` |

**asinh transform:** Compresses price spikes (e.g., $2,300 → ~8.4) so high-level differences don't dominate distance calculations. Applied to all LMP features. Reversed (`sinh`) in composite features before division.

**Date range:** Filtered to `EXTENDED_FEATURE_START` (2021-01-01), with rolling-window warmup NaNs dropped.

#### Stage 2: Analog Selection

**`similarity/engine.py → find_analogs()`**

**Step 1 — Calendar Pre-filter** (`filtering.py`):
- Exclude target date and future dates
- Match DOW group (weekday / Saturday / Sunday)
- Season window: ±60 days from target day-of-year (wraps around year boundary)

**Step 2 — Regime Pre-filter** (`filtering.py`):
- Compute z-score of target's `lmp_daily_flat` and `gas_m3_price` in the candidate pool
- Keep candidates within ±1.5 std of target's z-score (rejects extreme spikes/crashes)
- Fallback: if pool < 20 candidates, relax to date-proximity sort

**Step 3 — Feature Normalization** (`engine.py`):
- Z-score normalize each feature group independently across the filtered pool
- Prevents any single domain from dominating

**Step 4 — Per-Group Distance** (`metrics.py`):
- Euclidean (default), cosine, MAE, or pattern (level-invariant shape)
- Each feature group computed separately

**Step 5 — Weighted Blend** (`metrics.py`):
- `final_dist = sum(group_weight * group_dist) / sum(group_weights)`
- 16 feature groups with expert-tuned weights (see configs below)

**Step 6 — Rank & Weight**:
- Select top-N by lowest blended distance
- Compute analog weights: inverse_distance (default), softmax, rank, or uniform
- Compute similarity score: `1 - (dist - min) / (max - min)`

**Feature Group Weights** (from `configs.py`):

```
lmp_profile:          3.0    weather_hdd_cdd:      3.0
lmp_level:            2.0    weather_level:        2.5
lmp_volatility:       1.0    weather_wind:         0.5
load_level:           2.0    composite_heat_rate:  2.0
load_shape:           1.0    target_load_level:    2.5
gas_price:            2.0    target_load_shape:    1.0
gas_momentum:         0.5    target_weather_level: 2.0
calendar_dow:         1.5    target_weather_hdd_cdd: 2.5
calendar_season:      1.0
```

**NaN-group exclusion:** If the target row has all-NaN for a feature group (e.g., D+1 weather in production), that group is silently skipped from the similarity computation.

#### Stage 3: Probabilistic Forecast

**`pipelines/forecast.py`**

For each of the N analog dates, fetch the **next day's** actual DA LMP hourly profile. Then for each hour (1-24):

- **Point forecast:** Weighted average of analog next-day LMPs
- **Quantiles:** Weighted quantile interpolation at q = [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]

#### Stage 4: Evaluation

**`evaluation/metrics.py`** (when actuals are available)

| Metric | Description |
|--------|-------------|
| MAE | Mean absolute error ($/MWh) |
| RMSE | Root mean squared error ($/MWh) |
| MAPE | Mean absolute percentage error (%) |
| rMAE | Relative MAE vs naive (same day last week); < 1.0 = model wins |
| Mean Pinball Loss | Average pinball loss across all quantiles |
| CRPS | Continuous ranked probability score |
| Coverage 80/90/98% | % of actuals within P10-P90 / P05-P95 / P01-P99 |
| Sharpness 90% | Average width of P05-P95 interval ($/MWh) |

#### Stage 5: Output Format

Pivoted table: `Date | Type | HE1-24 | OnPeak | OffPeak | Flat`

- **Type rows:** Actual (if available), Forecast, Error (if available)
- **Quantiles table:** Same format with Type = P01, P05, ..., P99
- **Analogs:** DataFrame with date, rank, distance, similarity, weight
- **Metrics dict:** All evaluation metrics

**On-peak hours:** HE 8-23. **Off-peak hours:** HE 1-7, 24. **Flat:** All 24 hours.

---

## Data Sources

| Source | Table | Key Columns | Coverage |
|--------|-------|-------------|----------|
| DA LMP Hourly | `f_lmps_da_hourly` | lmp_total, system_energy, congestion, marginal_loss | 2009+ |
| RT LMP Hourly | `f_lmps_rt_hourly` | Same as DA | 2008+ |
| Gas Prices | `f_gas_prices` | M3, HH, Transco Z6 (next-day ICE settlement) | 1990+ |
| RT Metered Load | `f_load_rt_metered_hourly` | load_mw (RTO) | 2014+ |
| DA Load Forecast | `f_load_da_hourly` | load_mw (RTO) | 2020+ |
| Weather (Observed) | `f_hourly_temps` (WSI schema) | temp, dew_point, wind, cloud cover (PJM aggregate) | 1995+ |
| Calendar | `f_dates` | DOW, holiday, season | Static |

All tables in schema `dbt_pjm_v1_2026_feb_19` (weather in `dbt_wsi_temps_v1_2026_feb_25`).

---

## Backend vs Research Pipeline Comparison

| Aspect | Backend Simple | Research Extended |
|--------|---------------|-------------------|
| Data sources | LMP only | LMP + load + gas + weather + calendar |
| Features | 4 LMP components x hours | 100+ across 8 groups |
| Pre-filtering | Hour, date range, DOW, month | Calendar (DOW group, season) + regime (z-score) |
| Distance metrics | MAE, RMSE, Euclidean, cosine | Same + pattern (level-invariant) |
| Feature weighting | Uniform or custom per-column | 16 expert-tuned group weights |
| Normalization | Z-score across pool | Z-score per group independently |
| Output | Hourly profiles + similarity scores | Point forecast + 9 quantiles + evaluation metrics |
| Lines of code | ~300 | ~5,000+ |
