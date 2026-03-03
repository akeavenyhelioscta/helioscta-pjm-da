# Like-Day Model: Future Improvements

Investigation date: 2026-02-25
Target forecast: DA LMP for 2026-02-26 (Thursday), Western Hub
Performance: MAE $5.48/MWh | RMSE $7.40/MWh | MAPE 11.6% | systematic downside miss (all 24 hours under-forecast)
Worst hour: HE19 error -$18.8/MWh | HE22 error -$16.5/MWh | HE8 error -$12.4/MWh

---

## 1. Root Cause Analysis

Six ranked causes of the systematic downside miss on evening peak hours:

| Rank | Root Cause | Est. Impact | Fix Complexity |
|------|-----------|-------------|----------------|
| 1 | **Missing temperature / HDD-CDD features** — model cannot distinguish cold-snap days from mild days, so analogs mix both, pulling evening peaks toward the average | $8-15/MWh on peak hours | Medium (new data source + feature module) |
| 2 | **asinh compression bias** — `asinh(80)=5.07` vs `asinh(120)=5.48` makes a $40 difference at high levels look tiny, admitting lower-volatility analogs | $3-8/MWh on peak hours | Low (parameter tuning or metric change) |
| 3 | **Regime filter too loose + no heat rate filter** — 2.0 std tolerance on asinh-compressed values retains 95%+ of candidates; `implied_heat_rate` is computed but unused in distance calc or filtering | $2-5/MWh | Low (config change) |
| 4 | **IDW weight concentration** — `1/(d+eps)^2` gives top analog 16x the weight of #30; amplifies bias if closest analogs are from quieter days | $1-4/MWh (amplifier) | Low (config change) |
| 5 | **Feature weight imbalance** — LMP groups get 43% of total weight (matching on outcomes), while driver features (load, gas) get only 39%. research.md warns this creates circular dependency | $2-4/MWh | Low (config change) |
| 6 | **Historical pool dilution** — 2014-2019 had structurally lower gas-to-power multiples (10-11x vs current 16x) | $1-3/MWh | Low (config change) |

**Key insight:** The `implied_heat_rate` composite feature is already computed in `composite.py` but never used in the distance computation or regime filtering.

### Detailed Parameter Recommendations

**High Priority:**
1. Add temperature features (HDD/CDD) — new `temperature_features.py` module with weight 2.5-3.0
2. Add net load features — subtract renewable generation from gross load
3. Reduce asinh compression for profile matching (options: scaled `asinh(x/k)` with k=10-20, or cosine distance for shape matching, or threshold-based application)

**Medium Priority:**
4. Tighten regime filter to 1.0-1.5 std; add filter on `implied_heat_rate`
5. Rebalance feature weights toward drivers: reduce `lmp_profile` from 3.0 to 2.0, increase `load_level` from 2.0 to 3.0, add `implied_heat_rate` group with weight 2.0-2.5
6. Reduce IDW concentration: change to linear inverse distance or increase epsilon to 0.1-0.5

**Lower Priority:**
7. Shorten lookback or add recency weighting
8. Add gas-to-power ratio to regime filter

---

## 2. Seasonality & Lookback Recommendations

Five ranked parameter changes to improve analog pool composition:

| Rank | Parameter Change | Current | Proposed | Expected Impact | Rationale |
|------|-----------------|---------|----------|----------------|-----------|
| 1 | `FILTER_SEASON_WINDOW_DAYS` | 60 | **30** | **HIGH** | For Feb 26, the 60-day window includes late Dec holidays and late April shoulder season. April days have longer daylight, lower heating demand, and structurally lower evening peaks. Narrowing to 30 keeps pool within core winter. |
| 2 | `EXTENDED_FEATURE_START` | `"2014-01-01"` | **`"2021-01-01"`** | **MED-HIGH** | Removes pre-COVID era with different gas prices, renewable penetration, and load shapes. Also improves regime filter z-score statistics (mean/std computed from pool are more representative of current conditions). |
| 3 | `DOW_GROUPS` | Mon-Wed / Thu-Fri / Sat / Sun | **Mon-Fri / Sat / Sun** | **MEDIUM** | 2.5x more candidates. The Thu-Fri distinction is weak for winter heating-driven evening peaks. Soft DOW distance features in `calendar_features.py` still prefer same-DOW. |
| 4 | Recency penalty (new parameter) | None | **0.05-0.10 per year** | **LOW-MED** | Penalizes older analogs in distance calc. Lower priority since lookback truncation (#2) handles worst cases. |
| 5 | `DEFAULT_N_ANALOGS` | 30 | **30 (no change)** | **LOW** | Inverse-distance-squared weighting means top analogs dominate regardless. Pool composition matters more than selection count. |

**Critical interaction note:** Combining recommendations 1+2+3 yields ~200 candidates (5 years x 43 weekdays in 60-day span, after regime filter), selecting 30 — a comfortable 15% selection ratio. The `FILTER_MIN_POOL_SIZE` safety net (currently 20) in `filtering.py` provides fallback if any combination produces too few candidates.

---

## 3. Consolidated Priority List

Merged and deduplicated action items across both analyses, ordered by expected impact and implementation effort:

| Priority | Action | Type | Files Affected |
|----------|--------|------|---------------|
| 1 | **Narrow season window** from 60 to 30 days | Config change | `configs.py` |
| 2 | **Truncate lookback** from 2014 to 2021 | Config change | `configs.py` |
| 3 | **Merge weekday DOW groups** to Mon-Fri / Sat / Sun | Config change | `configs.py`, `filtering.py` |
| 4 | **Add `implied_heat_rate` to a feature group** in distance calc (already computed in `composite.py`) | Config + engine | `engine.py`, `configs.py` |
| 5 | **Tighten regime filter** to 1.0-1.5 std | Config change | `configs.py` |
| 6 | **Add temperature/HDD-CDD features** | New data source + feature module | New `temperature_features.py`, `builder.py`, `configs.py` |

Items 1-3 and 5 are config-only changes that can be tested independently. Item 4 requires wiring the existing composite feature into the similarity engine. Item 6 requires a new data source (weather data) and a new feature builder module.

---

## 4. Investigation Notebook

Reference: `like-day-model/notebooks/investigate_forecast.ipynb`

The notebook contains 8 sections (18 cells) for reproducing and analyzing the forecast:

1. **Setup & Run Forecast** — runs the pipeline for 2026-02-26
2. **Forecast vs Actuals** — hourly line chart with 80%/90% PI shaded bands
3. **Error Analysis** — color-coded bar chart + segmented metrics (on-peak, off-peak, peak HE16-22)
4. **Analog Day Analysis** — top 10 table + top 5 next-day LMP profiles overlaid on actual
5. **Analog Date Distribution** — year histogram + date-vs-distance scatter colored by weight
6. **Feature Sensitivity** — sweeps `n_analogs` [10-50] and weight methods, plots MAE/RMSE
7. **Season Window Sensitivity** — tests windows [30, 45, 60, 90] with `find_analogs` directly
8. **Summary Statistics** — full metrics, output table, quantile bands

---

## Forecast Context (2026-02-26)

For reference, the forecast that motivated this investigation:

```
Date         Type         HE1    HE2    HE3   ...   HE18   HE19   HE20   HE21   HE22   HE23   HE24   OnPk   OffPk   Flat
2026-02-26   Actual      37.3   33.1   33.0   ...   46.9   58.6   49.6   46.6   52.7   43.8   34.7   44.41  38.43  42.41
2026-02-26   Forecast    31.1   30.3   29.6   ...   38.3   39.8   41.2   40.2   36.2   32.3   29.9   38.40  34.05  36.95
2026-02-26   Error       -6.1   -2.8   -3.4   ...   -8.7  -18.8   -8.4   -6.4  -16.5  -11.5   -4.8   -6.01  -4.38  -5.47
```

Top 5 analogs selected: 2021-02-03, 2019-03-06, 2016-01-06, 2018-03-14, 2025-01-29
