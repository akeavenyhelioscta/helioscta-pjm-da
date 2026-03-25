# DA Model Implementation Plan

## Overview

Day-ahead probabilistic LMP price forecasting model for PJM Western Hub. Outputs quantile predictions (1st, 5th, 10th, 25th, 50th, 75th, 90th, 95th, 99th percentiles) for 24 hourly prices.

**Data foundation:** 12+ years of hourly LMP history (2014–2026, 3.8M rows, zero nulls) with 16 years of daily gas prices, 6+ years of DA load, and 12+ years of RT metered load. See `.skills/da-model/data_assessment.md` for full inventory.

**Research basis:** Lago et al. (2021) epftoolbox benchmark, Uniejewski (2018) LASSO/PJM methods, Dudek (2015) GBM quantile approach from GEFCom2014, Polson (2018) DL-EVT on PJM data, Peng (2024) Transformer virtual bidding on ERCOT, Hong (2016) GEFCom2014 overview. See `.skills/da-model/research_summary.md` for full summaries.

**Target accuracy:** rMAE < 0.75 (LightGBM), < 0.70 (QRA ensemble). Literature reports achievable MAPE of 6–7% for DA LMP forecasting in PJM with well-tuned ML models.

**Key design decisions:**
- Forecast total LMP (standard practice) — PJM LMP = energy + congestion + loss components
- asinh variance stabilizing transformation (Uniejewski 2018) — critical for price spikes ($2,323 max in 2026)
- Multi-window calibration averaging (Lago 2021) — consistently improves accuracy across markets
- 3-stage probabilistic pipeline: Direct Quantile → QRA Ensemble → Conformal Calibration

---

## 1. Directory Structure

```
da-model/
  research/                          # (existing) PDFs and reference repos

  src/
    pjm_da_forecast/
      __init__.py
      configs.py                     # Schema, hub, feature lists, quantiles, hyperparams, paths

      data/                          # One module per data source (mirrors backend/src/pjm_like_day/data/)
        __init__.py
        lmps_hourly.py               # Pull hourly LMPs (DA, RT, DART) for target + neighboring hubs
        lmps_daily.py                # Pull daily LMPs (flat/onpeak/offpeak averages)
        load_da_hourly.py            # Pull DA load forecast (2020+)
        load_rt_metered_hourly.py    # Pull RT metered load (2014+, for lag features)
        gas_prices.py                # Pull M3, HH, Transco Z6 from ice.next_day_gas (2010+)
        fuel_mix.py                  # Pull gridstatus.pjm_fuel_mix_hourly (2020+)
        weather.py                   # Pull wsi.daily_observed_temp / hourly (1993+)
        wind_solar.py                # Pull pjm.wind_gen, pjm.solar_gen (2020-2025)
        outages.py                   # Pull pjm.seven_day_outage_forecast (2020+)
        demand_bids.py               # Pull pjm.hrl_dmd_bids (2020+)
        dates.py                     # Pull calendar utility views

      features/                      # Feature engineering pipeline
        __init__.py
        builder.py                   # Orchestrator: calls feature modules, merges, applies preprocessing
        preprocessing.py             # Variance stabilizing transforms (asinh), inverse transforms
        lmp_features.py              # Price lags (d-1/d-2/d-3/d-7/d-14 all 24h), rolling stats, shape, spreads, cross-hub
        load_features.py             # DA load, RT load lags, load shape, ramp, net load, forecast error proxy
        gas_features.py              # M3/HH prices, basis spread, momentum, implied heat rate
        calendar_features.py         # Cyclical hour/dow/month, Fourier terms, is_weekend, is_holiday, season, dow_group
        weather_features.py          # Temperature, HDD/CDD, wind speed (when available)
        generation_features.py       # Fuel mix shares, wind/solar gen, renewable penetration
        outage_features.py           # Total/forced outage MW, outage trends
        demand_bid_features.py       # DA demand bid MW, bid vs forecast ratio

      models/                        # Model training and persistence
        __init__.py
        base.py                      # Abstract base class: QuantileForecaster
        lightgbm_quantile.py         # LightGBM quantile regression (primary)
        lear.py                      # LEAR baseline via epftoolbox (Lago 2021)
        qra.py                       # Quantile Regression Averaging ensemble (Phase 3)
        registry.py                  # Load/save model artifacts by name+date

      evaluation/                    # Evaluation and backtesting
        __init__.py
        metrics.py                   # Pinball loss, rMAE, coverage, sharpness, CRPS, DM test
        backtester.py                # Walk-forward validation: train/predict loop over dates
        reports.py                   # Generate evaluation summary DataFrames/dicts
        shap_analysis.py             # SHAP feature importance per quantile

      pipelines/                     # Orchestration
        __init__.py
        train.py                     # pull data -> preprocess -> features -> split -> train -> evaluate -> save
        predict.py                   # pull latest -> preprocess -> features -> load model -> predict -> quantiles
        backtest.py                  # Walk-forward over date range with multi-window averaging

      utils/
        azure_postgresql.py          # Copied from backend/src/utils/ (shared DB connection)

      api.py                         # FastAPI endpoints for serving predictions

      sql/                           # Raw SQL templates
        lmps_hourly.sql
        lmps_daily.sql
        load_da_hourly.sql
        load_rt_metered_hourly.sql
        gas_prices.sql
        fuel_mix.sql
        weather_daily.sql
        weather_hourly.sql
        wind_solar.sql
        outages.sql
        demand_bids.sql
        dates_daily.sql
        dates_hourly.sql

  artifacts/                         # Model artifacts (gitignored)
    models/                          # Serialized model files (.joblib)
    metrics/                         # Evaluation metrics per run (.json)
    predictions/                     # Cached prediction outputs (.parquet)

  notebooks/                         # Exploratory / development notebooks
    01_data_exploration.ipynb
    02_feature_engineering.ipynb
    03_model_training.ipynb
    04_backtesting.ipynb

  tests/
    __init__.py
    test_data_pull.py
    test_features.py
    test_models.py
    test_pipelines.py
    test_evaluation.py

  pyproject.toml
  requirements.txt
  Dockerfile
  README.md
```

---

## 2. Module Specifications

### 2.1 `configs.py`

Central configuration, following the pattern in `backend/src/pjm_like_day/configs.py`.

```python
SCHEMA = "dbt_pjm_v1_2026_feb_19"
HUB = "WESTERN HUB"
NEIGHBORING_HUBS = ["EASTERN", "AEP-DAYTON", "DOMINION", "CHICAGO"]
TARGET_COL = "lmp_total"
TARGET_MARKET = "da"
QUANTILES = [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]
HOURS = list(range(1, 25))

# Day-of-week groups for separate model training (PJM-specific best practice, per research)
DOW_GROUPS = {
    "weekday_early": [0, 1, 2],     # Mon-Wed
    "weekday_late": [3, 4],          # Thu-Fri
    "saturday": [5],
    "sunday": [6],
}

# Multi-window calibration (Lago 2021 best practice: combine short + long windows)
CALIBRATION_WINDOWS = {
    "8w": 56,       # 8 weeks — captures recent dynamics
    "12w": 84,      # 12 weeks — slightly longer recent context
    "3y": 1095,     # 3 years — captures annual seasonality
    "4y": 1460,     # 4 years — full cycle
}

LGBM_PARAMS = {
    "n_estimators": 1000,
    "learning_rate": 0.03,
    "max_depth": 7,
    "num_leaves": 63,
    "min_child_samples": 20,
    "subsample": 0.8,
    "colsample_bytree": 0.7,
    "reg_alpha": 0.1,
    "reg_lambda": 1.0,
    "verbose": -1,
}

# Variance stabilizing transformation (Uniejewski 2018)
# asinh(x) = ln(x + sqrt(x² + 1)) — behaves like log(2x) for large x, well-defined at 0 and negatives
VST = "asinh"

# Training window strategy (from data_assessment.md Section 7)
# Full-feature window: 2020-01-01+ (~6 years, limited by DA load history)
# Extended window (LMP + gas + calendar only): 2014-01-01+ (~12 years)
FULL_FEATURE_START = "2020-01-01"
EXTENDED_FEATURE_START = "2014-01-01"
```

### 2.2 `data/` modules

Each module follows the `backend/src/pjm_like_day/data/lmps.py` pattern: a `pull()` function that executes a SQL template, formats with schema/hub/market params, and returns a `pd.DataFrame`.

| Module | Source Table | Schema | Date Range | Grain | Key Columns |
|---|---|---|---|---|---|
| `lmps_hourly.py` | `staging_v1_pjm_lmps_hourly` | staging | 2014–2026 (12yr, 3.8M rows) | date x hour x hub x market | lmp_total, lmp_system_energy_price, lmp_congestion_price, lmp_marginal_loss_price |
| `lmps_daily.py` | `staging_v1_pjm_lmps_daily` | staging | 2014–2026 (479K rows) | date x hub x market x period | Same 4 LMP measures, periods: flat/onpeak/offpeak |
| `load_da_hourly.py` | `staging_v1_pjm_load_da_hourly` | staging | 2020–2026 (6yr, 208K rows) | date x hour x region | da_load_mw. Regions: RTO, WEST, MIDATL, SOUTH |
| `load_rt_metered_hourly.py` | `staging_v1_pjm_load_rt_metered_hourly` | staging | 2014–2026 (12yr, 426K rows) | date x hour x region | rt_load_mw |
| `gas_prices.py` | `next_day_gas` | ice | 2010–2026 (16yr, 128K rows) | date x hub | M3 VWAP, HH VWAP, Transco Z6 NY VWAP. Filter `data_type='VWAP Close'` |
| `fuel_mix.py` | `pjm_fuel_mix_hourly` | gridstatus | 2020–2026 (6yr, 54K rows) | date x hour | coal, gas, nuclear, wind, solar, hydro, etc. Zero nulls |
| `weather.py` | `daily_observed_temp*`, `hourly_observed_temp*` | wsi | 1993–2026 (30yr) | date (or hour) x station | temp, HDD, CDD, wind_speed, cloud_cover. PJM regions: PJM, PJM EAST, PJM WEST, PJM SOUTH |
| `wind_solar.py` | `wind_gen`, `solar_gen` | pjm | 2020–2025-10 (5.7yr, 584K rows) | date x hour x area | wind_gen_mw, solar_gen_mw. Areas: RTO, WEST, MIDATL, SOUTH, RFC. **Stops Oct 2025** |
| `outages.py` | `seven_day_outage_forecast` | pjm | 2020–2026 (6yr, 47K rows) | date x region | total_outage_mw, forced_outage_mw, planned_outage_mw |
| `demand_bids.py` | `hrl_dmd_bids` | pjm | 2020–2026 (6yr, 155K rows) | date x hour x area | DA demand bid MW. Areas: PJM_RTO, WESTERN_REGION, MID_ATLANTIC_REGION |
| `dates.py` | `utils_v1_pjm_dates_daily` / `_hourly` | staging | 2010–2026 (6.2K daily rows) | date (or hour) | day_of_week_number, is_weekend, is_nerc_holiday, summer_winter |

**Data quality notes (from data_assessment):**
- LMP: Zero nulls across 12+ years. 24 hours/day except 12 DST-transition days (23 hours). Hub name is `WESTERN HUB` (not `WESTERN`).
- DA Load: Zero nulls, 24 hours/day except 6 DST days. History starts 2020 only.
- Gas: M3 has 5,901 records from 2010–2026, avg $3.44/MMBtu.
- Fuel mix: Zero nulls, near-complete hourly coverage (8,700+ rows/yr).
- **Critical gap:** Load forecast views have only ~2 weeks of data (rolling snapshot). For backtesting, use DA load as proxy.
- **Critical gap:** Wind/Solar actuals stop at Oct 2025 (5 months missing). GridStatus fuel mix covers this partially.
- **Critical gap:** No weather forecast history. WSI temp forecasts only from Jan 2026. Use observed weather as "perfect forecast" proxy for backtesting.

### 2.3 `features/` modules

**`preprocessing.py` — Variance Stabilizing Transformation (Uniejewski 2018)**
- Apply `asinh(x)` to raw LMP prices before feature engineering and model training
- `asinh(x) = ln(x + sqrt(x² + 1))` — behaves like `log(2x)` for large x, compresses price spikes
- Well-defined at zero and for negative prices (unlike log)
- Inverse transform for predictions: `sinh(y)` to convert back to $/MWh
- Critical for PJM where Western Hub exhibits extreme regime shifts: $21 avg in 2020, $73 avg in 2022, $128 avg in 2026 YTD, with spikes to $2,323

**`lmp_features.py` — Price Features (Lago 2021 + Polish market weekly lag structure)**

Following Lago's LEAR input structure (price lags at d-1, d-2, d-3, d-7 all 24h each) plus d-14 lag from Polish market research (piekarsky — 7-14 day multiples capture weekly seasonality):
- `da_lmp_lag1d_h1` through `da_lmp_lag1d_h24` — yesterday's full price profile (24 features)
- `da_lmp_lag2d_h1` through `da_lmp_lag2d_h24` — 2 days ago (24 features)
- `da_lmp_lag3d_h1` through `da_lmp_lag3d_h24` — 3 days ago (24 features)
- `da_lmp_lag7d_h1` through `da_lmp_lag7d_h24` — same weekday last week (24 features)
- `da_lmp_lag14d_h1` through `da_lmp_lag14d_h24` — same weekday 2 weeks ago (24 features)
- Rolling stats on daily flat average: 7d mean, 7d std, 14d mean, 30d mean
- `lmp_same_hour_7d_mean` — mean of same hour over past 7 days (hour-specific trend)
- RT LMP same hour yesterday, DART spread same hour yesterday
- DA congestion/marginal loss components from prior day
- `lmp_congestion_share` — congestion_price / lmp_total (congestion regime indicator)
- `lmp_energy_share` — system_energy_price / lmp_total (marginal fuel cost dominance)
- Hourly shape index: prior day's LMP at this hour / prior day's flat average
- On-peak / off-peak ratio from prior day
- Cross-hub spreads: Western – Eastern lag, Western – AEP-Dayton lag (neighboring hub prices are highly predictive per Lehna 2022)

**`gas_features.py` — Fuel Price Features (from data assessment: M3 is #1 exogenous feature)**

Gas is the marginal fuel ~50% of hours in PJM, making it the single most important exogenous feature:
- `gas_m3_price` — TETCO M3 next-day gas VWAP (daily, broadcast to all hours). Avg $3.44, 16yr history.
- `gas_hh_price` — Henry Hub VWAP (national benchmark, avg $3.29)
- `gas_transco_z6_price` — Transco Z6 NY VWAP (Northeast premium, avg $3.86)
- `gas_m3_hh_spread` — M3 minus HH (regional basis)
- `gas_m3_7d_change` — 7-day price change (momentum)
- `gas_m3_30d_mean` — 30-day rolling mean (trend)
- `implied_heat_rate` — LMP / gas_price (mean-reverting market-implied efficiency)

**`load_features.py`**
- DA load forecast for target hour (known at inference time, 2020+ only). Demand is the primary LMP driver.
- RT metered load same hour yesterday (2014+, always available)
- Load shape: hour load / daily peak load (position in daily load curve)
- Load ramp: da_load[h] - da_load[h-1] (ramp needs drive scarcity)
- Load vs 7-day rolling average (demand anomaly)
- Load forecast error proxy: RT actual - DA load (lagged 1 day, captures systematic bias)
- `net_load` — DA load minus renewable generation forecast/actual (when available). Net load better captures the residual demand that thermal generators must serve.
- For 2014–2019 (pre-DA-load), use RT metered load lags as proxy

**`calendar_features.py`**
- Cyclical encodings: hour (sin/cos), day-of-week (sin/cos), month (sin/cos)
- Fourier terms: sin/cos pairs at multiple frequencies for annual (365.25-day) and weekly (7-day) seasonality. Captures smoother seasonal patterns than one-hot encoding. (From Spain demand forecasting research — Fourier features highly relevant for hourly price forecasting.)
- Binary: is_weekend, is_nerc_holiday, summer_winter
- `is_peak_hour` — hours 7–23 on non-holiday weekdays (on-peak / off-peak regime flag)
- Day-of-week group: Mon-Wed=0, Thu-Fri=1, Sat=2, Sun=3 (PJM-specific, per research — different price dynamics per group)
- Day-of-week one-hot (7 binary variables, per LEAR standard)

**`weather_features.py`** (P2 — once wsi data is promoted to staging)

Weather drives load, which drives LMP. Temperature extremes are critical (boomerang-shaped demand–temperature relationship per dash-peaky-finders research):
- Daily avg temperature, HDD, CDD (PJM weighted, from wsi tables — 30+ year history)
- Temperature squared (captures U-shaped demand-temperature relationship)
- Wind speed daily average
- For backtesting: use observed weather as "perfect forecast" proxy (no historical weather forecast data available before Jan 2026)

**`generation_features.py`** (P1 — once fuel_mix/wind_solar promoted to staging)
- Gas generation share (gas / total) — gas on margin fraction
- Wind generation MW, solar generation MW — renewables suppress LMP
- Renewable share ((wind + solar) / total) — net penetration signal
- Coal generation share (fuel switching signal — coal vs gas relative economics)
- **Note:** Wind/solar actuals stop Oct 2025. Use gridstatus fuel_mix wind/solar columns as gap-fill for recent months.

**`outage_features.py`** (P1 — once outage data promoted to staging)
- Total outage MW (PJM RTO), forced outage MW — supply reduction drives price increases
- 7-day outage change (trend)
- Planned outage MW (scheduled maintenance context)

**`demand_bid_features.py`** (P1 — from `pjm.hrl_dmd_bids`, 155K rows, 2020+)
- `da_demand_bid_mw` — hourly DA demand bid for PJM RTO. Represents realized market demand (distinct from ISO load forecast).
- `bid_vs_forecast` — demand bid / DA load forecast ratio. When bid > forecast, market participants expect higher demand than ISO.

**`builder.py`**
- Orchestrator: pulls all data, calls feature modules, merges on (date, hour_ending)
- Applies asinh variance stabilization to target and price features
- Handles dual training window strategy:
  - **Full-feature mode (2020+):** All features including DA load, fuel mix, generation, outages, demand bids
  - **Extended mode (2014+):** LMP lags + gas prices + RT load + calendar only (features available for full 12-year history)
- Drops NaN rows from lag warmup period
- Returns full feature DataFrame with target column `lmp_total_target`

### 2.4 `models/` modules

**Abstract base class (`base.py`):**

```python
class QuantileForecaster(ABC):
    def __init__(self, quantiles: list[float], name: str): ...
    def fit(self, X: pd.DataFrame, y: pd.Series) -> None: ...
    def predict(self, X: pd.DataFrame) -> pd.DataFrame: ...  # Returns q_0.01, q_0.05, ..., q_0.99
    def save(self, path: str) -> None: ...
    def load(cls, path: str) -> "QuantileForecaster": ...
```

**LightGBM Quantile (`lightgbm_quantile.py`) — Primary Model (Stage 1):**

With 12+ years of data (~105,000 hourly samples), two viable architectures:

- **Option A: 24 separate models per hour** — ~4,400 samples per model, viable for trees. Each hour gets its own quantile models (24 hours × 9 quantiles = 216 models). Allows hour-specific feature importance and hyperparameters.
- **Option B: Single model with hour_ending as feature** — ~105,000 samples, more data per model, shares information across hours.

Recommendation: **Start with Option B** for simplicity, benchmark against Option A. Literature (Lago 2021) uses per-hour LEAR models but single-model LightGBM with hour feature — compare both.

Additional techniques from research:
- **Multi-window averaging** (Lago 2021, Uniejewski 2016): Train on multiple calibration windows (8wk, 12wk, 3yr, 4yr), average predictions. Consistently improves accuracy across markets.
- **Day-of-week group models** (PJM-specific): Optionally train separate models for Mon-Wed, Thu-Fri, Sat, Sun.
- **Post-processing**: Isotonic regression to fix quantile crossing (Dudek 2015).

Trains one `LGBMRegressor(objective='quantile', alpha=q)` per quantile → 9 models (or 9 × N_windows for multi-window).

Also trains a point forecast model (`objective='mse'`) needed as input to the QRA ensemble in Stage 2.

**LEAR Baseline (`lear.py`) — Required Benchmark (Stage 1):**
- Wraps `epftoolbox.models.LEAR` — parameter-rich ARX model with LASSO
- 247 input features per hour: price lags (d-1/d-2/d-3/d-7 × 24h), 2 exogenous forecasts (d/d-1/d-7 × 24h), day-of-week dummy
- Daily recalibration with hybrid LARS-LASSO hyperparameter selection
- Multi-window averaging (8wk + 12wk + 3yr + 4yr) built into epftoolbox
- This is the state-of-the-art linear baseline that often matches deep learning (Lago 2021)
- Point forecast only — feeds into QRA for probabilistic output
- Despite being linear, LEAR with LASSO implicit feature selection achieves state-of-the-art results on PJM data

**QRA Ensemble (`qra.py`) — Stage 2:**
- Stage 1: Collect point forecasts from LEAR, LightGBM (MSE objective), and optionally DNN
- Stage 2: For each quantile τ, fit `statsmodels.QuantReg` of actual prices on the K point forecasts
- Produces well-calibrated prediction intervals from diverse base models
- Won GEFCom2014 price track (Nowotarski & Weron, 2015)
- Key variants to consider:
  - **Smoothing QRA (SQRA):** Kernel smoothing for smoother density estimates. Up to 3.5% profit improvement (Marcjasz 2023).
  - **Factor QRA (FQRA):** PCA to automatically select from large pool of point forecasters.
  - **Regularized QRA:** LASSO/elastic net on the combining regression.

**Registry (`registry.py`):**
- Save/load model artifacts via `joblib`
- Naming: `{model_name}_{run_id}/`
- `get_latest(name)` for inference

### 2.5 `evaluation/` modules

**Metrics (following Lago 2021 best practices + GEFCom2014 standard):**

| Metric | Type | Description |
|---|---|---|
| **Pinball loss** | Probabilistic (primary) | `max(q*(y-ŷ), (q-1)*(y-ŷ))` per quantile. GEFCom2014 official metric. |
| **Mean pinball loss** | Probabilistic | Average across all quantiles — single summary |
| **rMAE** | Point (primary) | Relative MAE: `MAE(model) / MAE(naive)` where naive = same hour 7 days ago. Lago's recommended metric — comparable across markets, normalizes for difficulty. rMAE < 1 means model beats naive. |
| **MAPE** | Point | Mean Absolute Percentage Error. Target: 6–7% (achievable for PJM per literature). |
| **Coverage** | Calibration | % of actuals within each interval (80% interval → ~80% coverage) |
| **Sharpness** | Calibration | Average interval width (`mean(q90 - q10)`) — narrower is better, given correct coverage |
| **CRPS** | Probabilistic | Continuous Ranked Probability Score — integrates pinball across all quantiles |
| **MAE / RMSE** | Point | Absolute metrics of the q=0.50 (median) prediction |
| **Diebold-Mariano test** | Statistical | Tests whether accuracy difference between two models is significant. Required per Lago 2021 best practices. Use multivariate variant (daily 24h vector). |

**SHAP Analysis (`shap_analysis.py`):**
- Compute SHAP values for LightGBM models per quantile (Lehna 2022 best practice)
- Different quantiles rely on different features: central quantiles (50th) depend on load/season, tail quantiles (5th, 95th) depend on volatility/gas features (Dudek 2015)
- Generate feature importance rankings and interaction plots
- Use to guide incremental feature addition

**Backtester:** Walk-forward with expanding window. With 12 years of data, use 2-year out-of-sample test period (Lago 2021 minimum standard) — e.g., train on 2014–2023, test on 2024–2025.

---

## 3. Pipeline Flows

### 3.1 Training Pipeline

```
configs.py             → Load SCHEMA, HUB, QUANTILES, LGBM_PARAMS, CALIBRATION_WINDOWS
    |
data/*.py              → Parallel SQL pulls (lmps, gas, load, dates, fuel_mix, weather, outages, demand_bids)
    |
preprocessing.py       → Apply asinh variance stabilization to prices
    |
features/builder.py    → Merge data, compute all features (lags, rolling, gas, calendar, load, generation)
    |                    → Select training window mode: full-feature (2020+) or extended (2014+)
    |
Split                  → For each window W in CALIBRATION_WINDOWS:
    |                      train = features[d - W : d - 1]
    |
models/*.py            → For each window W, for each quantile q:
    |                      fit LGBMRegressor(objective='quantile', alpha=q)
    |                    → Also fit LGBMRegressor(objective='mse') for QRA base forecast
    |
Multi-window average   → Average predictions across windows (Lago 2021)
    |
Post-process           → Isotonic fix for quantile crossing; inverse asinh transform
    |
evaluation/metrics.py  → Pinball loss, rMAE, MAPE, coverage, sharpness, CRPS, SHAP importance
    |
models/registry.py     → Save model ensemble + metrics to artifacts/
    |
Return                 → {model_path, metrics_dict, feature_importances, shap_values}
```

### 3.2 Inference Pipeline

```
Receive request        → target_date (default: tomorrow), hub, model_name
    |
data/*.py              → Pull latest data (LMPs through yesterday, gas price, load forecast, etc.)
    |
preprocessing.py       → Apply asinh to price features
    |
features/builder.py    → Build 24 feature rows (one per hour of target_date)
    |
models/registry.py     → Load latest trained model ensemble (all windows)
    |
model.predict()        → For each window: 24 hours × 9 quantiles; then average across windows
    |
Post-process           → Isotonic fix for crossing; inverse asinh transform
    |
Format                 → DataFrame: [hour_ending, q_0.01, q_0.05, ..., q_0.99]
```

### 3.3 Backtesting Pipeline

```
Pull full date range   → All historical data (2014–2026)
    |
For each date d in test period (2024-01-01 to 2025-12-31):
  For each window W in CALIBRATION_WINDOWS:
    Build features[d-W : d-1]   → Train model on window
    Build features[d]            → Predict 24 hours
  Average predictions across windows
  Apply isotonic fix + inverse asinh
  Store predictions
    |
Collect actuals        → Actual DA LMPs for all backtest dates
    |
Compute metrics        → Per-date, per-hour, per-quantile, aggregated summary
    |
Statistical tests      → Diebold-Mariano: LightGBM vs LEAR vs Naive
    |
SHAP analysis          → Feature importance per quantile, per hour
```

### 3.4 Three-Stage Probabilistic Pipeline Architecture

Based on research_summary Section 6.5 — the recommended layered approach for production:

```
Stage 1: Base Models (Point + Direct Quantile)
  |-- LightGBM Quantile Regression (primary, 9 quantiles × 4 windows)
  |-- LightGBM Point Forecast (MSE objective, for QRA input)
  |-- LEAR Point Forecast (via epftoolbox, for QRA input)

Stage 2: Ensemble Combination (Phase 3)
  |-- QRA: for each quantile τ, fit QuantReg of actuals on K point forecasts
  |-- Variant: Smoothing QRA for smoother density estimates

Stage 3: Calibration (Phase 4)
  |-- Adaptive Conformal Inference (ACI) on Stage 1 or Stage 2 output
  |-- Asymmetric formulation for right-skewed PJM prices
  |-- On-line recalibration with daily update

Output: Calibrated probabilistic forecast
  |-- Quantiles: 0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99
  |-- 80% PI: [0.10, 0.90]
  |-- 90% PI: [0.05, 0.95]
  |-- 98% PI: [0.01, 0.99]
```

This layered architecture provides:
1. **Accuracy** from diverse base models (LightGBM + LEAR)
2. **Calibration** from QRA ensemble combining
3. **Coverage guarantees** from conformal prediction recalibration
4. **Adaptivity** from on-line daily recalibration

---

## 4. Feature Set Summary

### Training Window Strategy

Two training modes based on data availability (from data_assessment Section 7):

| Mode | Date Range | Duration | Features Available | Use Case |
|---|---|---|---|---|
| **Full-feature** | 2020-01-01 → present | ~6 years | All P0 features (LMP + gas + load + calendar) | Primary training mode |
| **Extended** | 2014-01-01 → present | ~12 years | LMP lags + gas prices + RT load + calendar only | When DA load features are not needed, or for separate long-window models |

The full-feature window is limited by DA load history (starts 2020). For 2014–2019, RT metered load is used as proxy.

### P0 Features (MVP — available today, no new staging views needed)

| Category | Feature | Source | History | Count |
|---|---|---|---|---|
| LMP Lags | `da_lmp_lag{1,2,3,7,14}d_h{1..24}` — full 24h profiles for 5 lag days | staging LMP | 2014+ | 120 |
| LMP Stats | rolling 7d/14d/30d mean, 7d std, same_hour_7d_mean, shape index, onpeak/offpeak ratio | staging LMP | 2014+ | 7 |
| LMP Components | DART spread lag, congestion_share, energy_share, cross-hub spread lags (Western–Eastern, Western–AEP-Dayton) | staging LMP | 2014+ | 6 |
| Gas Prices | M3 VWAP, HH VWAP, Transco Z6 VWAP, M3-HH basis, M3 7d change, M3 30d mean, implied heat rate | ice.next_day_gas | 2010+ | 7 |
| Load | DA load forecast (2020+), RT metered load lag, load shape, load ramp, load vs 7d avg, load forecast error proxy | staging load | 2014/2020+ | 6 |
| Calendar | hour sin/cos, dow sin/cos, month sin/cos, Fourier (annual/weekly), is_weekend, is_holiday, is_peak_hour, summer_winter, dow_group, dow one-hot | staging dates | 2010+ | ~22 |

**MVP total: ~168 features** (dominated by the 120 price lag features from 5 lag days × 24 hours per Lago 2021 + Polish market research). LASSO/LightGBM will automatically select the relevant subset.

### P1 Features (after promoting source tables to staging)

| Category | Feature | Source | History |
|---|---|---|---|
| Fuel Mix | gas_gen_share, coal_gen_share, renewable_share | gridstatus.pjm_fuel_mix_hourly | 2020+ |
| Wind/Solar | wind_gen_mw, solar_gen_mw, net_load | pjm.wind_gen, pjm.solar_gen | 2020–2025 |
| Outages | total_outage_mw, forced_outage_mw, planned_outage_mw, outage_7d_change | pjm.seven_day_outage_forecast | 2020+ |
| Demand Bids | da_demand_bid_mw, bid_vs_forecast ratio | pjm.hrl_dmd_bids | 2020+ |

### P2 Features (after promoting weather data)

| Category | Feature | Source | History |
|---|---|---|---|
| Weather | daily avg temp, HDD, CDD, temp², wind speed | wsi.daily_observed_temp | 1993+ |

---

## 5. Data Readiness & Constraints

### Price Regime History (from data_assessment)

Western Hub DA LMP shows dramatic regime shifts that inform why multi-window training is critical:

| Year | Avg LMP | Max LMP | Notes |
|---|---|---|---|
| 2014 | $51 | $949 | Polar vortex spikes |
| 2016 | $29 | $122 | Low gas era |
| 2020 | $21 | $84 | COVID demand collapse |
| 2022 | $73 | $469 | Gas/energy crisis |
| 2024 | $34 | $315 | Normalized |
| 2025 | $50 | $550 | Volatility returning |
| 2026 YTD | $128 | $2,323 | Extreme winter spikes |

This range (10x difference in averages, 28x in spikes) validates the asinh transform and multi-window approach.

### Critical Data Gaps

| Gap | Impact | Mitigation |
|---|---|---|
| No historical load forecast data (only 2 weeks) | Can't evaluate forecast-based features on backtest | Use DA load (cleared schedule) as proxy for backtesting |
| Wind/Solar actuals stop Oct 2025 | 5 months missing for recent training | Use gridstatus fuel_mix wind/solar columns as gap-fill |
| No weather forecast history (WSI only from Jan 2026) | Can't train weather features with backtesting | Use observed weather as "perfect forecast" proxy |
| No transmission constraint data | Congestion component unexplained | Rely on lagged congestion_share feature as proxy |
| DA load starts 2020 only | 6 years missing full-feature data | RT metered load proxy for 2014–2019; extended training mode for LMP+gas+calendar |

### Data Not Yet in Staging (requires dbt view promotion)

| Data | Source | Rows | Date Range | Priority |
|---|---|---|---|---|
| Gas prices (M3, HH, Z6) | `ice.next_day_gas` | 128K | 2010–2026 | P0 |
| Fuel mix hourly | `gridstatus.pjm_fuel_mix_hourly` | 54K | 2020–2026 | P1 |
| Demand bids | `pjm.hrl_dmd_bids` | 155K | 2020–2026 | P1 |
| Outage forecast | `pjm.seven_day_outage_forecast` | 47K | 2020–2026 | P1 |
| Wind generation | `pjm.wind_gen` | 283K | 2020–2025 | P1 |
| Solar generation | `pjm.solar_gen` | 301K | 2020–2025 | P1 |
| Weather daily | `wsi.daily_observed_temp_v3_*` | 399K | 1993–2026 | P2 |
| Weather hourly | `wsi.hourly_observed_temp_v2_*` | 9.1M | 1995–2026 | P2 |

---

## 6. Integration Plan

### Deployment: Separate FastAPI Service (Recommended)

Add to `docker-compose.yml`:

```yaml
da-model:
  build: ./da-model
  ports: ["8001:8001"]
  env_file: ./da-model/src/.env
  command: uvicorn src.pjm_da_forecast.api:app --host 0.0.0.0 --port 8001 --reload
```

Follows the existing microservice pattern (backend:8000 + frontend:3000).

### API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Health check |
| POST | `/predict` | Generate quantile forecast for a target date |
| POST | `/train` | Trigger model training |
| GET | `/backtest` | Run or retrieve backtest results |
| GET | `/models` | List available trained models |
| GET | `/metrics/{run_id}` | Retrieve evaluation metrics |
| GET | `/shap/{run_id}` | Retrieve SHAP feature importance |

### `/predict` Response Shape

```json
{
  "target_date": "2026-02-26",
  "hub": "WESTERN HUB",
  "model": "lgbm_quantile_multi_window",
  "quantile_forecast": [
    {
      "hour_ending": 1,
      "q_0.01": 18.50, "q_0.05": 22.30, "q_0.10": 25.12,
      "q_0.25": 27.45, "q_0.50": 30.18,
      "q_0.75": 33.92, "q_0.90": 37.41,
      "q_0.95": 42.10, "q_0.99": 55.80
    },
    ...
  ],
  "prediction_intervals": {
    "80_pct": {"lower": "q_0.10", "upper": "q_0.90"},
    "90_pct": {"lower": "q_0.05", "upper": "q_0.95"},
    "98_pct": {"lower": "q_0.01", "upper": "q_0.99"}
  },
  "metadata": {
    "training_end_date": "2026-02-24",
    "calibration_windows": ["8w", "12w", "3y", "4y"],
    "n_training_samples_max": 105120,
    "feature_count": 168,
    "vst": "asinh",
    "training_mode": "full_feature"
  }
}
```

### Frontend Integration

1. **Proxy route:** `frontend/app/api/da-forecast/route.ts` → forwards to `http://da-model:8001/predict`
2. **Component:** `frontend/components/power/DaForecast.tsx` — fan chart (shaded regions for 98%/90%/80% PI), solid median line, actuals overlay when available
3. **Navigation:** Add `"da-forecast"` to `ActiveSection` type in Sidebar
4. **Dashboard:** Add KPI card showing tomorrow's median LMP and 80% confidence range

### Shared Utility

Copy `backend/src/utils/azure_postgresql.py` into `da-model/src/pjm_da_forecast/utils/`. The function is ~15 lines. Refactor to a shared package later when a third consumer appears.

---

## 7. Implementation Order

### Phase 1: Data Foundation + MVP Model

**Goal:** End-to-end working model with P0 features (LMP lags + gas + load + calendar).

1. Create `pyproject.toml` with deps: lightgbm, scikit-learn, pandas, numpy, psycopg2-binary, fastapi, uvicorn, joblib, python-dotenv, shap, epftoolbox, statsmodels
2. Create `configs.py` with all constants (9 quantiles, multi-window, asinh, LGBM params, dual training windows)
3. Copy `azure_postgresql.py` utility
4. Create SQL templates in `sql/`
5. Implement P0 data modules: `lmps_hourly.py`, `lmps_daily.py`, `load_da_hourly.py`, `load_rt_metered_hourly.py`, `gas_prices.py`, `dates.py`
6. Implement `features/preprocessing.py` (asinh transform + inverse)
7. Implement `features/lmp_features.py` with 120-feature lag structure (5 lag days × 24h) + rolling stats + component shares + cross-hub spreads
8. Implement `features/gas_features.py` (M3/HH/Z6 + spreads + momentum), `features/load_features.py` (DA/RT load + shape + ramp + net load), `features/calendar_features.py` (cyclical + Fourier + flags)
9. Implement `features/builder.py` with dual training window support
10. Implement `models/base.py`, `models/lightgbm_quantile.py` (with multi-window + point forecast), `models/registry.py`
11. Implement `pipelines/train.py`
12. Implement `evaluation/metrics.py` (pinball, rMAE, MAPE, coverage, sharpness)
13. Write `tests/` for data, features, models
14. Create notebook `01_data_exploration.ipynb` — profile all data sources, validate year-by-year LMP statistics
15. Create notebook `02_feature_engineering.ipynb` — visualize features, check correlations, validate asinh transform
16. Create notebook `03_model_training.ipynb` — train and evaluate LightGBM

**Deliverable:** LightGBM quantile model with multi-window averaging, trained on 6+ years of data, evaluated on 1+ year holdout.

### Phase 2: LEAR Baseline + Evaluation Framework

**Goal:** Rigorous evaluation against the established benchmark.

1. Implement `models/lear.py` wrapping epftoolbox LEAR
2. Implement `evaluation/backtester.py` — walk-forward over 2-year test period (2024-01-01 to 2025-12-31)
3. Implement `evaluation/shap_analysis.py` — per-quantile feature importance (central vs tail quantile analysis per Dudek 2015)
4. Implement Diebold-Mariano test in `evaluation/metrics.py`
5. Implement `pipelines/backtest.py`
6. Create notebook `04_backtesting.ipynb` — compare LightGBM vs LEAR vs naive
7. Generate `evaluation/reports.py` — stratified by hour, day-of-week, season, price regime

**Deliverable:** Rigorous comparison: LightGBM vs LEAR vs naive, with DM statistical tests, SHAP analysis, and stratified error breakdown.

### Phase 3: QRA Ensemble + Additional Features

**Goal:** Probabilistic ensemble combining diverse base models (Stage 2 of probabilistic pipeline), add P1 features.

1. Implement `models/qra.py` — combine LEAR + LightGBM point forecasts via quantile regression. Consider Smoothing QRA variant for improved tail behavior (Marcjasz 2023).
2. Implement P1 data modules: `fuel_mix.py`, `wind_solar.py`, `outages.py`, `demand_bids.py`
3. Implement P1 feature modules: `generation_features.py`, `outage_features.py`, `demand_bid_features.py`
4. Promote source tables to staging views (coordinate with dbt)
5. Retrain LightGBM with expanded feature set
6. Backtest QRA ensemble vs direct LightGBM quantile

**Deliverable:** QRA ensemble producing calibrated prediction intervals from diverse base models.

### Phase 4: API, Inference Pipeline + Conformal Prediction

**Goal:** Production-ready prediction service with coverage guarantees (Stage 3 of probabilistic pipeline).

1. Implement `pipelines/predict.py`
2. Implement `api.py` with all endpoints
3. Add Adaptive Conformal Inference (ACI) as post-hoc calibration layer — asymmetric formulation for right-skewed PJM prices (critical given $2,323 spikes)
4. Create `Dockerfile`, add to `docker-compose.yml`
5. Write `tests/test_pipelines.py`

**Deliverable:** Prediction API at port 8001 with conformally calibrated quantile forecasts.

### Phase 5: Frontend Integration

1. Create `frontend/app/api/da-forecast/route.ts`
2. Create `frontend/components/power/DaForecast.tsx` (fan chart + table)
3. Add to Sidebar and HomePageClient
4. Add forecast KPI card to Dashboard

**Deliverable:** Full stack operational — fan chart with prediction intervals in the web UI.

### Phase 6: Weather Features + Refinement

1. Implement `data/weather.py`, `features/weather_features.py` (temp, HDD/CDD, temp², wind speed)
2. Promote wsi weather data to staging
3. Experiment with per-hour models (Option A) vs single model (Option B)
4. Experiment with day-of-week group models (Mon-Wed, Thu-Fri, Sat, Sun)
5. Tune hyperparameters via Bayesian optimization (Optuna)
6. Evaluate Smoothing QRA vs standard QRA vs Regularized QRA

---

## 8. Evaluation Strategy

### Metrics

| Metric | Type | Description |
|---|---|---|
| **Pinball loss** | Probabilistic (primary) | `max(q*(y-ŷ), (q-1)*(y-ŷ))` per quantile. GEFCom2014 standard. |
| **Mean pinball loss** | Probabilistic | Average across all 9 quantiles |
| **rMAE** | Point (primary) | `MAE(model) / MAE(naive_7d)` where naive = same hour 7 days ago. rMAE < 1 = beats naive. Lago's recommended metric. |
| **MAPE** | Point | Mean Absolute Percentage Error. Target: 6–7% for PJM (per literature). |
| **Coverage** | Calibration | % of actuals within each PI (80% PI → ~80% coverage) |
| **Sharpness** | Calibration | Mean interval width `mean(q90 - q10)` |
| **CRPS** | Probabilistic | Integrates pinball across all quantile levels |
| **MAE / RMSE** | Point | Absolute metrics of q=0.50 median prediction |
| **DM test** | Statistical | Diebold-Mariano test for significance of accuracy differences between models |

### Stratified Analysis

Break down all metrics by:
- **Hour of day** — peak hours (12-20) typically have higher variance
- **Day of week** — weekday vs weekend performance
- **Season / month** — winter heating vs summer cooling vs shoulder
- **Price regime** — low-price days vs spike days (e.g., above 90th percentile). Critical given the 10x range in yearly averages ($21 in 2020 to $128 in 2026 YTD).
- **Quantile level** — central (25-75) vs tail (1, 5, 95, 99) performance. Per Dudek 2015, tail quantiles depend on different features than central quantiles.

### Baselines to Beat

| Baseline | Description | rMAE Target |
|---|---|---|
| **Weekly naive** | Same hour 7 days ago | 1.0 (by definition) |
| **Daily naive** | Same hour yesterday | ~0.85–0.95 |
| **LEAR (epftoolbox)** | LASSO ARX with multi-window averaging | ~0.70–0.80 (Lago 2021 PJM results) |
| **LightGBM (ours)** | Primary model — should match or beat LEAR | Target: < 0.75 |
| **QRA ensemble** | Should beat all individual models | Target: < 0.70 |

### Test Period

With 12 years of history, follow Lago (2021) best practice:
- **Test period:** 2 years (e.g., 2024-01-01 to 2025-12-31) — 730 days × 24 hours = 17,520 predictions
- **Training:** All data prior to each test day (expanding window) combined with fixed-length calibration windows
- **Sufficient for:** Seasonal coverage (all 4 seasons × 2 years), statistical significance via DM test, meaningful SHAP analysis, price regime coverage (2024 normalized + 2025 volatile)
