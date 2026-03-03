# LGBMRegressor Methodology — DA LMP Forecast Pipeline

## Overview

The pipeline produces **probabilistic day-ahead LMP forecasts** for PJM Western Hub using a multi-quantile, multi-window LightGBM ensemble.

## Target Variable

- **Column:** `lmp_total` (DA LMP total price, Western Hub, $/MWh)
- **Transformation:** `asinh(x) = ln(x + sqrt(x^2+1))` — compresses extreme price spikes (up to $2,300+/MWh) without breaking on zero or negative prices. Predictions are inverted with `sinh()` for output.

## Feature Set (~170 features)

| Category | Count | Key Features |
|----------|-------|-------------|
| **LMP lags** | ~120 | 5 lag days (d-1, d-2, d-3, d-7, d-14) x 24 hours; 7/14/30-day rolling stats; on/off-peak ratio; energy/congestion shares; DART spread |
| **Gas prices** | 7 | M3, HH, Transco Z6 NY raw prices; M3-HH basis spread; M3 momentum (7d change, 30d mean); implied heat rate |
| **Load** | 5-9 | DA load forecast + shape/ramp/anomaly (2020+); RT metered load lag (2014+); forecast error |
| **Calendar** | 29 | Cyclical hour/DOW/month encodings; annual + weekly Fourier harmonics; weekend/holiday/summer flags; PJM DOW groups; one-hot DOW |

### LMP Features (~120)

- **Price Lag Features:** 5 lag days (1, 2, 3, 7, 14) x 24 hourly values = 120 features
- **Rolling Statistics (4):** 7d mean/std, 14d mean, 30d mean of daily flat average
- **Same-Hour 7d Mean (1):** For each hour h, mean of prices at hour h over past 7 days
- **Peak Ratio (1):** On-peak avg / off-peak avg from prior day (on-peak: HE 7-22)
- **LMP Component Shares (2):** Energy share and congestion share of total price (d-1)
- **Shape Index (1):** Prior day's hourly price normalized by daily average
- **DART Spread Features (25):** Daily DA-RT spread (d-1) + 24 RT hourly lags (d-1)

### Gas Features (7)

- **Raw Prices (3):** M3, HH, Transco Z6 NY next-day settlement
- **Spread (1):** M3-HH basis spread (regional premium)
- **Momentum (2):** M3 7-day change, M3 30-day rolling average
- **Implied Heat Rate (1):** Daily avg LMP / gas M3 price, lagged 1 day

### Load Features (5-9)

- **DA Load (4, 2020+ only):** Raw forecast, load shape (hour/daily peak), ramp (hour-to-hour change), demand anomaly (daily avg / 7d rolling avg)
- **RT Load (3, 2014+):** Metered load lag (d-1), forecast error lag (d-1)

### Calendar Features (29)

- **Cyclical Encodings (6):** Hour, day-of-week, month (sin + cos each)
- **Annual Fourier (6):** k=1,2,3 harmonics for 365.25-day cycle
- **Weekly Fourier (4):** k=1,2 harmonics for 7-day cycle
- **Binary Flags (4):** is_weekend, is_nerc_holiday, summer_winter, is_peak_hour
- **DOW Group (1):** PJM-specific (weekday_early, weekday_late, saturday, sunday)
- **One-Hot DOW (7):** One binary per day of week

## Model Architecture

**10 LGBMRegressors per calibration window:**
- **9 quantile models** — `objective="quantile"`, alpha in {0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99}
- **1 point forecast model** — `objective="regression"` (MSE)

**Hyperparameters** (from `configs.py`):
```
n_estimators=1000, learning_rate=0.03, max_depth=7, num_leaves=63,
min_child_samples=20, subsample=0.8, colsample_bytree=0.7,
reg_alpha=0.1, reg_lambda=1.0
```

## Multi-Window Calibration Averaging (Lago 2021)

Four models are trained on different lookback windows and their predictions are averaged:

| Window | Days |
|--------|------|
| 8 weeks | 56 |
| 12 weeks | 84 |
| 3 years | 1,095 |
| 4 years | 1,460 |

This acts as implicit ensemble regularization — short windows track recent regime shifts, long windows provide stability.

## Post-Processing

1. **Average** predictions across the 4 calibration windows
2. **Isotonic sort** each row's quantiles to fix any crossings (ensures q0.01 <= q0.05 <= ... <= q0.99)
3. **Inverse asinh** (`sinh()`) to convert back to $/MWh
4. **Pivot** to output format: `Date | Type | HE1-HE24 | OnPeak | OffPeak | Flat`

## Training Discipline

- **Time-series split** — no shuffling; train on all data before forecast date
- **Two modes:** `full_feature` (2020+, includes DA load) and `extended` (2014+, no DA load)
- **Warm-up rows dropped** — first 14 days have NaN lags
- **No explicit CV or hyperparameter search** — fixed params with multi-window averaging as regularization
- **Naive baseline:** Same hour, 7 days ago — used for rMAE comparison

## Evaluation Metrics

- **Point:** MAE, RMSE, MAPE, rMAE (vs. 7-day naive baseline)
- **Probabilistic:** Pinball loss per quantile, mean pinball, CRPS
- **Interval:** Coverage (80/90/98% prediction intervals) and sharpness (interval width)
