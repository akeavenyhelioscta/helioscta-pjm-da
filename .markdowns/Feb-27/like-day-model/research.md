# Like-Day / Analog-Day Model Research for PJM LMP Probabilistic Forecasting

## 1. Academic Papers and Key References

### 1.1 Foundational Like-Day / Similar-Day Papers

**Che & Chen (2007). "A Novel Approach to Forecast Electricity Price for PJM Using Neural Network and Similar Days Method."**
IEEE Transactions on Power Systems.
[IEEE Xplore](https://ieeexplore.ieee.org/abstract/document/4349103/)

- Directly targets PJM LMP forecasting using similar-day selection combined with neural networks.
- Defines "similar days" as days that are like a reference day in electricity prices and loads.
- Proposes four distance models for selecting similar days: **Euclidean norm**, **Manhattan distance**, **Cosine coefficient**, and **Pearson correlation coefficient**.
- Historical data from the PJM interchange energy market (LMP and load) was used.
- Key finding: Using load data to forecast LMP is reasonable because of the strong correlation between demand and price. Similar-day selection improves ANN forecasting accuracy.
- The similar-day selection process includes five steps: (1) reference day definition, (2) interval selection, (3) similarity calculation, (4) similarity sorting, and (5) similar day determination.

**Che & Chen (2006). "Electricity Price Forecasting for PJM Day-Ahead Market."**
IEEE Conference Publication.
[IEEE Xplore](https://ieeexplore.ieee.org/document/4075935/)

- Earlier conference version of the above work, establishing the similar-day + ANN methodology for PJM.

**Mandal, Senjyu, Funabashi (2006). "Neural Networks Approach to Forecast Several Hour Ahead Electricity Prices and Loads in Deregulated Markets."**
Part of "Electricity Price Forecasting Using Neural Networks and Similar Days" in Wiley-IEEE Press.
[IEEE Xplore](https://ieeexplore.ieee.org/document/7985020/)

- Book chapter providing comprehensive treatment of similar-day methods combined with neural networks.
- Covers how to select appropriate data of price and load for LMP forecasting.
- Demonstrates that applying the method of choosing similar days can increase forecasting performance.

**Lora, Santos, Ramos, Exposito (2020). "Short-Term Electricity Price Forecasting Based on Similar Day-Based Neural Network."**
Energies, 13(17), 4408.
[MDPI](https://www.mdpi.com/1996-1073/13/17/4408)

- Applies similar day regression (SDR) and similar day-based artificial neural network (SDANN) to obtain electricity price forecasts.
- Tests four refined distance models: Euclidean norm, Manhattan distance, cosine coefficient, and Pearson correlation coefficient.
- Finds that **cosine coefficient and Pearson correlation** often outperform pure distance-based metrics for similar-day selection in electricity markets.
- Evaluates different window sizes for historical day selection: d = 15, 30, 45, 60 days before the reference day, plus d days in the prior year.

### 1.2 Weighted KNN for Electricity Markets

**Lora, Santos, Exposito, Ramos, Santos (2007). "Electricity Market Price Forecasting Based on Weighted Nearest Neighbors Techniques."**
IEEE Transactions on Power Systems, 22(3), 1294-1301.
[IEEE Xplore](https://ieeexplore.ieee.org/document/4282040/)

- Proposes weighted nearest neighbors (WNN) methodology for next-day electricity market price forecasting.
- Uses **Weighted-Euclidean distance** with weights estimated by genetic algorithm optimization.
- The relevant parameters defining the model include the window length of the time series and the number of neighbors chosen for prediction.
- Key insight: **Inverse distance weighting** -- closer analogs receive higher weight in the forecast, rather than equal weighting of all k neighbors.
- Classical Parameters Tuning (CPTO-WNN) and Fast Parameters Tuning (FPTO-WNN) methods for selecting optimal k and feature weights.

**Troncoso Lora, Ramos, Santos, Exposito (2002). "Electricity Market Price Forecasting: Neural Networks versus Weighted-Distance k Nearest Neighbours."**
Springer LNCS, 3-540-46146-9.
[Springer](https://link.springer.com/chapter/10.1007/3-540-46146-9_32)

- Direct comparison between neural networks and weighted-distance KNN for electricity price forecasting.
- Demonstrates that KNN with appropriate distance weighting can be competitive with neural network approaches.

### 1.3 KNN Applied to Time Series Forecasting

**Tajmouati, El Wahbi, Dakkon (2024). "Applying k-nearest neighbors to time series forecasting: Two new approaches."**
Journal of Forecasting, 43(5).
[Wiley](https://onlinelibrary.wiley.com/doi/10.1002/for.3093)

- Proposes two new KNN-based approaches for time series forecasting.
- Addresses the challenge that selection of the number of neighbors and feature selection is a daunting task in KNN approaches.
- Key finding: small alterations in k values can reduce model performance, emphasizing the need for careful tuning.

**Multivariate k-nearest neighbour regression for time series data (2013).**
IEEE Conference Publication.
[IEEE Xplore](https://ieeexplore.ieee.org/document/6706742/)

- Proposes multivariate k-NN regression for UK electricity demand forecasting.
- Uses binary dummy variables to distinguish between load profiles of working days, weekends, and bank-holidays.
- Demonstrates that calendar information should be included as categorical features in the neighbor search.

### 1.4 Analog Ensemble (AnEn) -- Methodology Transfer from Meteorology

**Delle Monache, Eckel, Rife, Nagarajan, Searight (2013). "Probabilistic Weather Prediction with an Analog Ensemble."**
Monthly Weather Review, 141(10), 3498-3516.
[AMS Journals](https://journals.ametsoc.org/view/journals/mwre/141/10/mwr-d-12-00281.1.xml)

- **Seminal paper** introducing the Analog Ensemble (AnEn) methodology.
- Core idea: The probability distribution of a future state is estimated using a set of past observations that correspond to the best analogs of a deterministic prediction.
- An analog is defined as a past prediction that has similar values for selected features of the current forecast.
- Each analog's verifying observation becomes an ensemble member; taken together, these observations constitute the ensemble prediction.
- The AnEn exhibits high **statistical consistency and reliability** and captures flow-dependent error behavior.
- Training period: 12-15 months of forecasts and observations recommended.
- **Directly transferable to electricity markets**: replace "weather prediction" with "day characteristics" (load forecast, temperature, calendar) and "weather observation" with "observed LMP profile."

**Weiming Hu et al. "Parallel Analog Ensemble (PAnEn)."**
[GitHub: Weiming-Hu/AnalogsEnsemble](https://github.com/Weiming-Hu/AnalogsEnsemble)

- C++ and R implementation of the AnEn methodology.
- Python interface available via [PyAnEn](https://github.com/Weiming-Hu/PyAnEn).
- Transforms deterministic predictions to ensemble forecasts using historical analogs.

### 1.5 Probabilistic Electricity Price Forecasting Reviews

**Nowotarski & Weron (2018). "Recent advances in electricity price forecasting: A review of probabilistic forecasting."**
Renewable and Sustainable Energy Reviews, 81(1), 1548-1568.
[ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S1364032117308808)

- Comprehensive tutorial review of probabilistic electricity price forecasting (EPF).
- Establishes the paradigm of "maximizing sharpness subject to reliability."
- Covers quantile regression, prediction intervals, density forecasting, and scenario generation.
- Key insight for like-day models: probabilistic forecasts from similar-day methods can be constructed by treating the LMP profiles of the k-nearest analogs as an empirical ensemble, then deriving quantiles directly.

**Weron (2014). "Electricity Price Forecasting: A review of the state-of-the-art with a look into the future."**
International Journal of Forecasting, 30, 1030-1081.
[ScienceDirect](https://www.sciencedirect.com/science/article/pii/S0169207014001083)

- Comprehensive review covering similar-day methods, ARIMA, neural networks, and hybrid approaches.
- Notes that KNN/similar-day approaches often underperform advanced ensemble models (RF, XGBoost) due to sensitivity to neighbor selection and distance metrics.
- Recommends normalization to zero mean and unit variance to prevent features measured on different scales from dominating the distance computation.
- The choice of k is critical: small values lead to overfitting, large values over-smooth, missing local patterns.

**Lago, Marcjasz, De Schutter, Weron (2021). "Forecasting day-ahead electricity prices: A review of state-of-the-art algorithms, best practices and an open-access benchmark."**
Applied Energy, 293, 116983.
[ScienceDirect](https://www.sciencedirect.com/science/article/pii/S0306261921004529)

- Benchmark paper establishing LEAR and DNN as standard reference models.
- Includes PJM market data in the benchmark.
- Confirms the high explanatory power of: load forecasts for the target hour, last day's prices for the same or neighboring hours, and the price for the same hour a week earlier.

### 1.6 Quantile Regression Averaging (QRA) for Probabilistic Outputs

**Nowotarski & Weron (2015). "Computing electricity spot price prediction intervals using quantile regression and forecast averaging."**
Computational Statistics, 30(3).
[Springer](https://link.springer.com/article/10.1007/s00180-014-0523-0)

- Introduces QRA: applies quantile regression to point forecasts of a pool of models.
- Won GEFCom2014 price track -- established as the dominant probabilistic EPF paradigm.
- Directly applicable: like-day profiles can serve as one of the point forecast inputs to QRA.

**Marcjasz, Uniejewski, Weron (2023). "Smoothing Quantile Regression Averaging: A new approach to probabilistic forecasting of electricity prices."**
[arXiv](https://arxiv.org/html/2302.00411v3)

- Improves QRA with kernel smoothing for smoother density estimates.
- Reports up to 3.5% profit improvement over standard QRA in trading simulations.

### 1.7 Conformal Prediction for Electricity Prices

**Conformal Prediction for Electricity Price Forecasting in the Day-Ahead and Real-Time Balancing Market (2025).**
[arXiv](https://arxiv.org/abs/2502.04935)

- Applies Ensemble Batch Prediction Intervals and Sequential Predictive Conformal Inference to electricity prices.
- Provides formal coverage guarantees: realized values outside the prediction interval do not exceed the chosen error rate.
- Distribution-free and model-agnostic -- can be layered on top of like-day outputs.
- Evaluated through simulated battery trading, demonstrating improved financial returns.

**De la Torre et al. (2024). "Electricity price forecast in wholesale markets using conformal prediction: Case study in Mexico."**
Energy Science & Engineering.
[Wiley](https://scijournals.onlinelibrary.wiley.com/doi/full/10.1002/ese3.1710)

- Practical application of conformal prediction to wholesale electricity prices.
- Demonstrates that CP can transform any point prediction into calibrated prediction intervals.

---

## 2. Relevant GitHub Repositories

### 2.1 Electricity Price Forecasting Toolboxes

**epftoolbox -- Open-access benchmark and toolbox for EPF**
[GitHub: jeslago/epftoolbox](https://github.com/jeslago/epftoolbox)
- Includes PJM market data in benchmark datasets.
- Provides LEAR (LASSO Estimated AutoRegressive) and DNN reference models.
- Evaluation module with proper statistical tests (DM test, GW test).
- Python, built on scikit-learn, TensorFlow/Keras, statsmodels.
- **Relevance:** Useful as benchmark comparison for like-day model outputs.

### 2.2 Analog Ensemble Implementations

**Parallel Analog Ensemble (PAnEn)**
[GitHub: Weiming-Hu/AnalogsEnsemble](https://github.com/Weiming-Hu/AnalogsEnsemble)
- C++ and R packages for parallel ensemble forecasts using Analog Ensemble.
- Based on Delle Monache et al. (2013) AnEn methodology.
- [Python interface: PyAnEn](https://github.com/Weiming-Hu/PyAnEn)
- **Relevance:** The AnEn methodology is directly transferable to our like-day model for generating probabilistic outputs from analog matches.

**AnDA -- Analog Data Assimilation**
[GitHub: ptandeo/AnDA](https://github.com/ptandeo/AnDA)
- Python library for Analog Data Assimilation.
- Combines analog forecasting with Kalman filtering/data assimilation.
- **Relevance:** Demonstrates advanced analog-based probabilistic methods.

### 2.3 Electricity Price Forecasting Projects

**Electricity Price Prediction (Neural Networks + ML)**
[GitHub: Carterbouley/ElectricityPricePrediction](https://github.com/Carterbouley/ElectricityPricePrediction)
- Day-ahead electricity price prediction using neural networks and other ML methods.
- Includes battery storage value optimization.

**Short-Term EPF at Polish Day-Ahead Market**
[GitHub: piekarsky/Short-Term-Electricity-Price-Forecasting-at-the-Polish-Day-Ahead-Market](https://github.com/piekarsky/Short-Term-Electricity-Price-Forecasting-at-the-Polish-Day-Ahead-Market)
- 26,200+ hourly observations, uses 7-14 day lagged prices, demand, and wind generation forecasts.
- **Relevance:** The lag structure (7-14 day multiples) is important for like-day feature design.

**ADGEfficiency/forecast**
[GitHub: ADGEfficiency/forecast](https://github.com/ADGEfficiency/forecast)
- A toolkit for forecasting energy time series. General-purpose energy forecasting utilities.

**EpexPredictor (LightGBM for European electricity)**
[GitHub: b3nn0/EpexPredictor](https://github.com/b3nn0/EpexPredictor)
- LightGBM-based day-ahead price prediction for EPEX markets.
- Uses weather data from Open-Meteo, grid load from ENTSO-E, holiday/weekend indicators.
- **Relevance:** Demonstrates practical feature engineering for electricity price forecasting.

### 2.4 Probabilistic Forecasting Libraries

**MAPIE -- Model Agnostic Prediction Interval Estimator**
[GitHub: scikit-learn-contrib/MAPIE](https://github.com/scikit-learn-contrib/MAPIE)
- Conformal prediction for Python, compatible with scikit-learn.
- Can be layered on top of any point forecast model (including KNN/like-day).

**darts -- Time Series Made Easy (unit8)**
[GitHub: unit8co/darts](https://github.com/unit8co/darts)
- Includes TFTModel with built-in quantile regression and conformal prediction.
- NaiveSeasonal and other baseline models useful for benchmarking.

---

## 3. Recommended Methodology for Our Use Case

### 3.1 Current State Assessment

The existing codebase (`backend/src/pjm_like_day/`) implements a basic like-day finder using:
- **Distance metrics:** MAE, RMSE, Euclidean, Cosine distance on hourly LMP profiles.
- **Features:** LMP total, system energy price, congestion price, marginal loss price (all from the same market).
- **Multi-feature weighting:** z-score normalization per feature, then weighted blend.
- **Output:** Top-N like days with distance and similarity scores.
- **Missing:** No probabilistic output (confidence intervals, quantiles, density estimates).

### 3.2 Recommended Architecture: Enhanced Like-Day with Probabilistic Output

```
Phase 1: Feature-Enhanced Like-Day Selection
    |-- Expand feature vector beyond LMP-only
    |-- Add load, temperature, gas price, calendar features
    |-- Use weighted distance with feature importance optimization

Phase 2: Probabilistic Output Generation
    |-- Method A: Empirical Quantiles from Analog Ensemble
    |-- Method B: Kernel Density Estimation on Analog Outputs
    |-- Method C: Conformal Prediction Wrapper

Phase 3: Validation and Calibration
    |-- Reliability diagrams (coverage vs nominal level)
    |-- Sharpness assessment (interval width)
    |-- Continuous Ranked Probability Score (CRPS)
```

### 3.3 Detailed Recommendations

#### Step 1: Expand the Feature Vector for Day Matching

The current system matches only on LMP price profiles. The literature strongly recommends matching on the **drivers** of price rather than price itself, because the goal is to find days whose conditions were similar, not whose outcomes happened to be similar. Matching on outcomes creates a circular dependency when forecasting.

Recommended feature vector for like-day matching (in priority order):

| Priority | Feature | Rationale |
|----------|---------|-----------|
| 1 | Day-of-week category (weekday/weekend/holiday) | **Hard filter**, not a distance feature. Weekday prices differ fundamentally from weekend. |
| 2 | System load forecast (peak and average) | Strongest driver of LMP. Load explains most price variation. |
| 3 | Temperature (HDD/CDD or raw) | Primary driver of load, and thus price. Captures the U-shaped demand-temperature relationship. |
| 4 | Natural gas price (Henry Hub or M3/Tetco) | Gas-fired plants are the marginal fuel in PJM ~60% of the time. The electricity-to-gas price ratio (historically 10-11x, now 16x) is a key structural driver. |
| 5 | Month / season | Captures seasonal patterns in supply stack, renewable generation, and demand. |
| 6 | Wind + solar generation forecast | Renewable generation displaces expensive thermal units, suppressing LMP. Net load (load minus renewables) is a better predictor than gross load. |
| 7 | DA LMP lagged features (t-1, t-7) | Recent price levels and weekly patterns provide context about current market regime. |
| 8 | Congestion component | Hub-specific congestion patterns differentiate Western Hub from system-wide conditions. |

**Implementation note:** Use day-of-week and month as **hard filters** (pre-filtering the candidate pool) rather than distance features. Only match weekdays to weekdays, weekends to weekends. Optionally filter by season (or use a soft seasonal weighting).

#### Step 2: Improve the Distance Metric

**Current:** Z-score normalized weighted MAE/RMSE/Euclidean/Cosine across features.

**Recommended improvements:**

1. **Mahalanobis distance** -- accounts for feature correlations and different scales simultaneously. If features are correlated (e.g., load and temperature), Mahalanobis distance avoids double-counting.

2. **Weighted Euclidean with optimized weights** -- use cross-validation or genetic algorithm (per Lora et al., 2007) to optimize feature weights rather than setting them manually.

3. **Dynamic Time Warping (DTW)** -- for hourly profile matching, DTW handles small time-shifts in price patterns (e.g., a price spike occurring at hour 17 vs hour 18). More computationally expensive but captures shape similarity better than pointwise distance.

4. **Cosine similarity** is already supported and is recommended for comparing hourly profile shapes. It is invariant to the overall price level, focusing on the shape/pattern of the daily profile.

**Recommendation:** Keep cosine similarity as the default for hourly profile shape matching. Add Mahalanobis distance as an option for daily aggregate feature matching. Use cosine for "shape similarity" and Euclidean for "level similarity" -- then blend.

#### Step 3: Select the Optimal Number of Analogs (k)

The literature provides guidance on selecting k:

- **Too few (k < 5):** High variance, unstable probabilistic estimates, risk of overfitting to a single analog.
- **Too many (k > 50):** Over-smoothing, probabilistic intervals too wide, includes dissimilar days.
- **Sweet spot:** Typically **10-30 analogs** for electricity markets (Lora et al. test 15, 30, 45, 60 day windows; Delle Monache AnEn uses 15-25).

**Recommended approach:**
1. Use **leave-one-out cross-validation** on the historical period to select k.
2. Evaluate CRPS (Continuous Ranked Probability Score) as the optimization metric, since it measures both calibration and sharpness of probabilistic forecasts.
3. Start with k=20 as a default, test range [5, 10, 15, 20, 25, 30, 40, 50].
4. k may vary by season or market regime -- consider adaptive k selection.

#### Step 4: Generate Probabilistic Outputs from Like-Day Matches

This is the key enhancement the current system lacks. Three recommended methods, ordered by complexity:

##### Method A: Empirical Quantile Ensemble (Simplest -- Implement First)

Once the top k analogs are identified, retrieve their actual DA LMP profiles (24 hourly values each). For each hour h:

```
analog_prices_h = [LMP(analog_1, h), LMP(analog_2, h), ..., LMP(analog_k, h)]

# Unweighted quantiles
P10_h = np.percentile(analog_prices_h, 10)
P25_h = np.percentile(analog_prices_h, 25)
P50_h = np.percentile(analog_prices_h, 50)  # median forecast
P75_h = np.percentile(analog_prices_h, 75)
P90_h = np.percentile(analog_prices_h, 90)
```

For **weighted** quantiles (closer analogs have more influence):

```python
weights = 1.0 / distances  # inverse distance weighting
weights /= weights.sum()   # normalize to sum to 1

# Weighted quantile: sort analogs by price, accumulate weights, find threshold
sorted_idx = np.argsort(analog_prices_h)
sorted_prices = analog_prices_h[sorted_idx]
sorted_weights = weights[sorted_idx]
cumulative_weights = np.cumsum(sorted_weights)

# P50 = price where cumulative weight crosses 0.50
P50_h = sorted_prices[np.searchsorted(cumulative_weights, 0.50)]
```

**Pros:** Simple, interpretable, no distributional assumptions.
**Cons:** Quantile estimates are coarse with small k. No density between quantiles.

##### Method B: Kernel Density Estimation (KDE) on Analog Outputs

Fit a KDE to the analog prices at each hour, optionally weighted by similarity:

```python
from scipy.stats import gaussian_kde

for hour in range(1, 25):
    analog_prices = get_analog_prices(hour)
    weights = 1.0 / distances
    weights /= weights.sum()

    kde = gaussian_kde(analog_prices, weights=weights, bw_method='silverman')

    # Full density estimate
    price_grid = np.linspace(min_price, max_price, 200)
    density = kde(price_grid)

    # Quantiles from CDF
    cdf = np.cumsum(density) / np.sum(density)
    P10 = price_grid[np.searchsorted(cdf, 0.10)]
    P90 = price_grid[np.searchsorted(cdf, 0.90)]
```

**Pros:** Smooth density estimate, continuous quantile extraction, handles multimodal distributions (e.g., on/off peak price regimes).
**Cons:** Bandwidth selection matters. Can be unreliable with very few analogs (k < 10).

##### Method C: Conformal Prediction Wrapper (Most Rigorous)

Use the like-day median forecast as a point prediction, then apply conformal prediction to calibrate prediction intervals with formal coverage guarantees:

1. On a calibration set (e.g., the last 60 days), compute the median forecast from like-day analogs.
2. Calculate nonconformity scores: |actual_price - median_forecast| for each calibration day and hour.
3. For a new target day, the prediction interval at confidence level (1-alpha) is:
   `[median_forecast - q, median_forecast + q]`
   where q is the (1-alpha) quantile of the calibration nonconformity scores.

For **adaptive** conformal prediction (recommended for non-stationary electricity markets), use Sequential Predictive Conformal Inference (SPCI) which updates the calibration set in an online fashion.

**Pros:** Formal coverage guarantees, distribution-free, model-agnostic.
**Cons:** Intervals can be wide if the base model is poor. Marginal coverage only (not conditional).

**Recommendation:** Implement Method A first (empirical quantiles, both weighted and unweighted). Layer Method B (KDE) as an enhancement for smoother density estimates. Add Method C (conformal prediction) for formal coverage guarantees when validation is needed.

---

## 4. Key Features to Consider

### 4.1 Feature Hierarchy for Like-Day Matching (PJM Western Hub)

Based on the literature review, features are organized by their impact on LMP price:

#### Tier 1: Critical Features (Must Include)

| Feature | Description | Why It Matters | Source |
|---------|-------------|----------------|--------|
| **System load forecast** | PJM day-ahead load forecast (peak MW and average MW) | Single strongest predictor of LMP. Demand peaks have clear positive correlation with price spikes. | PJM Data Miner |
| **Day-of-week group** | Mon-Wed / Thu-Fri / Sat / Sun | Fundamental structural difference in demand patterns. Average PJM price for Fri/Thu is higher than remaining weekdays. Use as **hard filter**. | Calendar |
| **Natural gas price** | Henry Hub spot or next-day delivery price | Gas plants set the marginal price ~60% of the time in PJM. The electricity-to-gas multiple is the key fundamental ratio. | FRED / EIA |
| **Temperature** | HDD/CDD for PJM Western Hub zone | Primary driver of load. U-shaped relationship: high demand at both temperature extremes. | Open-Meteo / NOAA |

#### Tier 2: Important Features (Should Include)

| Feature | Description | Why It Matters | Source |
|---------|-------------|----------------|--------|
| **Month / season** | Month of year (or summer/winter flag) | Seasonal changes in supply stack (outage season, renewable availability), demand patterns. | Calendar |
| **Renewable generation forecast** | Wind + solar DA forecast for PJM | High renewable output suppresses LMP by displacing expensive thermal units. Net load (load minus renewables) is a better predictor than gross load. | PJM Data Miner |
| **Recent DA LMP level** | Previous day's DA LMP average | Captures current market regime (high/low price environment). Short-term mean reversion signal. | PJM Data |
| **Holiday indicator** | US federal holidays + PJM-specific | Holidays reduce commercial/industrial demand significantly. Use as **hard filter** (match holidays to holidays). | Calendar |

#### Tier 3: Useful Refinement Features

| Feature | Description | Why It Matters | Source |
|---------|-------------|----------------|--------|
| **Congestion component** | DA congestion price at Western Hub | Hub-specific transmission constraints affect Western Hub differently from system average. | PJM Data |
| **Net imports/exports** | PJM interchange with neighboring regions | Net imports add supply, reducing LMP; net exports increase LMP. | PJM Data Miner |
| **Reserve margin** | Available generation minus predicted demand | Tight reserve margins cause scarcity pricing, significantly amplifying price spikes. Forward markets now price this structural tightness. | PJM |
| **Prior week same-day LMP** | LMP from the same weekday one week ago | Captures weekly seasonality, particularly useful for identifying persistent congestion patterns. | PJM Data |

### 4.2 Feature Engineering for Like-Day Matching

**Hard Filters (pre-filter the candidate pool):**
- Same day-type: weekday-to-weekday, weekend-to-weekend, holiday-to-holiday
- Same season or +/- 1 month window around target date
- Exclude known outlier/event days (polar vortex, grid emergency, etc.)

**Soft Features (used in distance calculation):**
- All Tier 1-3 features listed above, z-score normalized
- Features should be scaled to comparable units before distance computation

**Profile Features (for hourly shape matching):**
- 24-hour LMP profile shape (via cosine similarity)
- 24-hour load profile shape
- Hourly temperature profile

### 4.3 The Natural Gas -- Electricity Price Relationship

This deserves special attention for PJM Western Hub:

- Historically, forward electricity prices in PJM implied roughly a **10-11x multiple** of Henry Hub natural gas prices, reflecting marginal fuel cost under comfortable reserve margins.
- Today, forward markets imply closer to **16x**, reflecting structural system tightness beyond just fuel cost changes.
- PJM forward electricity prices now embed tighter reserve margins, peak-hour scarcity pricing, and reliability risk -- not just fuel cost.
- **Implication for like-day matching:** When comparing historical days, the absolute LMP level is less meaningful than the **relative LMP level given gas prices at the time**. Consider normalizing LMP by concurrent gas price, or including the electricity/gas ratio as a matching feature.

---

## 5. How to Generate Probabilistic Outputs from Like-Day Matches

### 5.1 Overview of Approaches

| Method | Complexity | Pros | Cons | Recommended k |
|--------|-----------|------|------|--------------|
| Empirical quantiles (unweighted) | Low | Simple, interpretable | Coarse quantiles with small k | k >= 20 |
| Weighted empirical quantiles (inverse distance) | Low-Medium | Closer analogs have more influence | Still coarse with small k | k >= 15 |
| Kernel density estimation (KDE) | Medium | Smooth densities, handles multimodality | Bandwidth selection, unreliable for very small k | k >= 10 |
| Bootstrap resampling of analogs | Medium | Uncertainty quantification on the quantile estimates themselves | Computationally heavier | k >= 15 |
| Conformal prediction wrapper | Medium-High | Formal coverage guarantees | Can be wide, marginal coverage only | Any k |
| QRA combining like-day with other models | High | Leverages model diversity, well-calibrated | Requires building additional models | Any k |
| Quantile Regression Forest on analog features | High | Non-parametric, handles interactions | More complex, needs more data | N/A (uses all data) |

### 5.2 Recommended Implementation Order

**Phase 1 (Immediate -- Minimal Change to Existing Code):**

Extend the current `pipeline.py` to return hourly LMP profiles for all k analogs, then compute empirical quantiles.

```python
def compute_probabilistic_forecast(hourly_profiles, like_days, target_date):
    """
    Given the hourly profiles of k analog days, compute probabilistic forecast.

    Returns DataFrame with columns:
        hour_ending, p10, p25, p50 (median), p75, p90, mean
    """
    # Get analog day dates and their distances
    analog_dates = like_days['date'].tolist()
    distances = like_days['distance'].values

    # Inverse distance weights (smaller distance = higher weight)
    weights = 1.0 / (distances + 1e-8)
    weights /= weights.sum()

    results = []
    for hour in range(1, 25):
        # Get LMP values for this hour across all analog days
        hour_data = hourly_profiles[
            (hourly_profiles['date'].isin(analog_dates)) &
            (hourly_profiles['hour_ending'] == hour)
        ]['lmp_total'].values

        if len(hour_data) == 0:
            continue

        results.append({
            'hour_ending': hour,
            'p10': np.percentile(hour_data, 10),
            'p25': np.percentile(hour_data, 25),
            'p50': np.percentile(hour_data, 50),
            'p75': np.percentile(hour_data, 75),
            'p90': np.percentile(hour_data, 90),
            'mean': np.average(hour_data, weights=weights),
        })

    return pd.DataFrame(results)
```

**Phase 2 (Short-term -- KDE Enhancement):**

Add kernel density estimation for smooth probability density functions:

```python
from scipy.stats import gaussian_kde

def compute_kde_forecast(hourly_profiles, like_days, target_date,
                         price_grid_points=200):
    """
    Compute KDE-based probabilistic forecast from analog days.

    Returns DataFrame with quantiles AND full density curve per hour.
    """
    analog_dates = like_days['date'].tolist()
    distances = like_days['distance'].values
    weights = 1.0 / (distances + 1e-8)
    weights /= weights.sum()

    quantiles_to_compute = [0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95]

    results = []
    for hour in range(1, 25):
        hour_data = hourly_profiles[
            (hourly_profiles['date'].isin(analog_dates)) &
            (hourly_profiles['hour_ending'] == hour)
        ]['lmp_total'].values

        if len(hour_data) < 3:
            continue

        kde = gaussian_kde(hour_data, weights=weights, bw_method='silverman')

        # Build price grid
        margin = (hour_data.max() - hour_data.min()) * 0.3
        price_grid = np.linspace(
            hour_data.min() - margin,
            hour_data.max() + margin,
            price_grid_points
        )

        # Evaluate CDF
        density = kde(price_grid)
        cdf = np.cumsum(density)
        cdf /= cdf[-1]  # normalize

        row = {'hour_ending': hour}
        for q in quantiles_to_compute:
            idx = np.searchsorted(cdf, q)
            row[f'p{int(q*100)}'] = price_grid[min(idx, len(price_grid)-1)]

        row['mean'] = np.average(hour_data, weights=weights)
        results.append(row)

    return pd.DataFrame(results)
```

**Phase 3 (Medium-term -- Conformal Calibration):**

Add a conformal prediction layer to ensure coverage guarantees:

```python
def calibrate_with_conformal(forecast_df, calibration_actuals,
                             calibration_forecasts, alpha=0.10):
    """
    Apply conformal prediction to calibrate prediction intervals.

    alpha=0.10 gives 90% prediction intervals.
    """
    # Compute nonconformity scores on calibration set
    residuals = np.abs(calibration_actuals - calibration_forecasts)

    # Get the (1-alpha) quantile of residuals (with finite-sample correction)
    n = len(residuals)
    q_level = np.ceil((n + 1) * (1 - alpha)) / n
    q = np.quantile(residuals, min(q_level, 1.0))

    # Apply to forecast
    forecast_df[f'cp_lower_{int((1-alpha)*100)}'] = forecast_df['p50'] - q
    forecast_df[f'cp_upper_{int((1-alpha)*100)}'] = forecast_df['p50'] + q

    return forecast_df
```

### 5.3 Advanced: Combining Like-Day with Other Models

The most robust probabilistic forecast combines like-day outputs with other model types:

1. **Like-day median** as one point forecast.
2. **LightGBM point forecast** as another (trained on the feature set from Section 4).
3. **LEAR/LASSO AR model** as a third (from epftoolbox).
4. Apply **Quantile Regression Averaging (QRA)** to combine:

```python
from statsmodels.regression.quantile_regression import QuantReg

# X = [like_day_median, lgbm_forecast, lear_forecast] for calibration period
# y = actual DA LMP for calibration period

for tau in [0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95]:
    model = QuantReg(y, X).fit(q=tau)
    # model.predict(X_new) gives the tau-th quantile forecast
```

This leverages the **diversity** of models: like-day captures regime/pattern similarity, LightGBM captures non-linear feature interactions, LEAR captures autoregressive structure.

---

## 6. Evaluation Metrics for Probabilistic Forecasts

### 6.1 Reliability (Calibration)

- **Empirical coverage:** For a nominal 90% prediction interval, ~90% of actuals should fall within the interval.
- **PIT histogram (Probability Integral Transform):** If the probabilistic forecast is well-calibrated, PIT values should be uniformly distributed.
- **Reliability diagram:** Plot nominal coverage vs empirical coverage across multiple quantile levels.

### 6.2 Sharpness

- **Average interval width:** Narrower intervals are better, conditional on reliability.
- **Winkler score:** Penalizes both width and miscoverage, combining reliability and sharpness.

### 6.3 Overall Scoring

- **CRPS (Continuous Ranked Probability Score):** The gold standard for evaluating probabilistic forecasts. Measures the integrated squared difference between the predicted CDF and the step function at the actual value. Lower is better.
- **Pinball loss (Quantile Score):** The proper scoring rule for quantile forecasts. Sum of asymmetric penalties for each quantile.

```python
def pinball_loss(actual, predicted_quantile, tau):
    """Pinball loss for a single quantile tau."""
    error = actual - predicted_quantile
    return np.where(error >= 0, tau * error, (tau - 1) * error).mean()

def crps_empirical(actual, ensemble_members):
    """CRPS computed from an ensemble (analog day LMP values)."""
    sorted_ens = np.sort(ensemble_members)
    n = len(sorted_ens)
    crps = 0.0
    for i, member in enumerate(sorted_ens):
        weight = (2 * (i + 1) - 1) / (2 * n)
        crps += np.abs(actual - member) - weight * np.abs(member - sorted_ens).mean()
    return crps / n
```

---

## 7. Comparison: Naive Like-Day vs. Enhanced Approaches

| Aspect | Current (Naive KNN on LMP) | Enhanced Like-Day (Recommended) |
|--------|---------------------------|--------------------------------|
| **Matching features** | LMP profile only (total, energy, congestion, loss) | Load forecast, temperature, gas price, calendar + LMP |
| **Feature handling** | Z-score normalization | Hard filters (day-type, season) + z-score + optimized weights |
| **Distance metric** | MAE/RMSE/Euclidean/Cosine | Cosine for shape + weighted Euclidean for levels, or Mahalanobis |
| **Number of analogs** | Fixed k=5 (default) | Cross-validated k (typically 15-25) |
| **Output** | Distance + similarity scores only | Full probabilistic forecast (quantiles, density, intervals) |
| **Probabilistic method** | None | Empirical quantiles + KDE + optional conformal calibration |
| **Analog weighting** | Equal weight | Inverse distance weighting |
| **Evaluation** | None | CRPS, pinball loss, reliability diagrams |
| **Circular dependency** | Matches on outcome (LMP) | Matches on drivers (load, temp, gas, calendar) |

---

## 8. Key Takeaways and Implementation Priorities

### Immediate Wins (Low Effort, High Impact)

1. **Add probabilistic output** -- compute empirical quantiles from the existing k analog day LMP profiles. The hourly_profiles data is already returned by the pipeline; just compute percentiles across analogs for each hour.

2. **Increase default k** from 5 to 20 -- the literature consensus is that 10-30 analogs provides a good balance between diversity and relevance for probabilistic estimates.

3. **Add inverse distance weighting** -- closer analogs should contribute more to the forecast. Use `weight = 1/distance` and compute weighted quantiles.

4. **Hard filter on day-type** -- at minimum, separate weekdays from weekends. The current `days_of_week` filter in `pipeline.py` already supports this but it should be the default behavior.

### Medium-Term Improvements (Moderate Effort)

5. **Add load forecast as a matching feature** -- this is the single highest-impact feature addition based on the literature. Match days with similar load levels, not just similar prices.

6. **Add temperature/HDD-CDD** -- second most impactful driver of LMP through its effect on load.

7. **Add natural gas price** -- captures the fuel cost driver that sets marginal price.

8. **Implement KDE-based density estimation** -- for smoother probabilistic forecasts.

### Longer-Term Enhancements

9. **Optimize feature weights** via cross-validation minimizing CRPS.
10. **Combine with LightGBM/LEAR** via QRA for a more robust probabilistic pipeline.
11. **Add conformal prediction** for formal coverage guarantees.
12. **Implement adaptive k selection** -- different k for different market regimes.

---

## 9. References Summary

### Papers Cited
1. Che, J., Chen, J. (2007). IEEE Trans. Power Systems. [Link](https://ieeexplore.ieee.org/abstract/document/4349103/)
2. Lora, A. et al. (2007). IEEE Trans. Power Systems. [Link](https://ieeexplore.ieee.org/document/4282040/)
3. Lora, A. et al. (2020). Energies, 13(17), 4408. [Link](https://www.mdpi.com/1996-1073/13/17/4408)
4. Delle Monache, L. et al. (2013). Monthly Weather Review, 141(10). [Link](https://journals.ametsoc.org/view/journals/mwre/141/10/mwr-d-12-00281.1.xml)
5. Nowotarski, J., Weron, R. (2018). Renew. Sustain. Energy Rev., 81(1). [Link](https://www.sciencedirect.com/science/article/abs/pii/S1364032117308808)
6. Weron, R. (2014). Int. J. Forecasting, 30. [Link](https://www.sciencedirect.com/science/article/pii/S0169207014001083)
7. Lago, J. et al. (2021). Applied Energy, 293. [Link](https://www.sciencedirect.com/science/article/pii/S0306261921004529)
8. Nowotarski, J., Weron, R. (2015). Computational Statistics, 30(3). [Link](https://link.springer.com/article/10.1007/s00180-014-0523-0)
9. Marcjasz, G. et al. (2023). arXiv:2302.00411. [Link](https://arxiv.org/html/2302.00411v3)
10. Tajmouati, S. et al. (2024). J. Forecasting, 43(5). [Link](https://onlinelibrary.wiley.com/doi/10.1002/for.3093)
11. Conformal Prediction for EPF (2025). arXiv:2502.04935. [Link](https://arxiv.org/abs/2502.04935)
12. De la Torre et al. (2024). Energy Sci. & Engineering. [Link](https://scijournals.onlinelibrary.wiley.com/doi/full/10.1002/ese3.1710)
13. Troncoso Lora et al. (2002). Springer LNCS. [Link](https://link.springer.com/chapter/10.1007/3-540-46146-9_32)
14. Mandal et al. (2006). Wiley-IEEE Press. [Link](https://ieeexplore.ieee.org/document/7985020/)

### GitHub Repositories
1. [jeslago/epftoolbox](https://github.com/jeslago/epftoolbox) -- EPF benchmark with PJM data
2. [Weiming-Hu/AnalogsEnsemble](https://github.com/Weiming-Hu/AnalogsEnsemble) -- Parallel Analog Ensemble
3. [Weiming-Hu/PyAnEn](https://github.com/Weiming-Hu/PyAnEn) -- Python interface for AnEn
4. [ptandeo/AnDA](https://github.com/ptandeo/AnDA) -- Analog Data Assimilation
5. [b3nn0/EpexPredictor](https://github.com/b3nn0/EpexPredictor) -- LightGBM electricity price prediction
6. [piekarsky/Short-Term-Electricity-Price-Forecasting](https://github.com/piekarsky/Short-Term-Electricity-Price-Forecasting-at-the-Polish-Day-Ahead-Market)
7. [Carterbouley/ElectricityPricePrediction](https://github.com/Carterbouley/ElectricityPricePrediction)
8. [ADGEfficiency/forecast](https://github.com/ADGEfficiency/forecast) -- Energy time series toolkit
9. [scikit-learn-contrib/MAPIE](https://github.com/scikit-learn-contrib/MAPIE) -- Conformal prediction for Python
