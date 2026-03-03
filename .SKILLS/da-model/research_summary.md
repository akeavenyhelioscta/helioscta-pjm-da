# Research Summary: Probabilistic Day-Ahead LMP Forecasting for PJM Western Hub

## 1. Paper Summaries

### Paper 1: "Electricity Price Forecasting on the Day-Ahead Market Using Machine Learning" (main1.pdf)
**Source:** Lehna et al., Applied Energy (2022), via [HAL hal-03621974](https://hal.science/hal-03621974/document)

- **Model Architecture:** Compares multiple ML models including Deep Neural Networks (DNNs), Convolutional Neural Networks (CNNs), and traditional ML methods (LASSO, Ridge, Random Forest, XGBoost). Uses the LogCosH loss function for training neural network models.
- **Features Used:** Electricity prices from three European areas (France, Germany, Belgium) over 6 years; production and consumption forecasts; gas prices (EGSI gas index); cross-border prices (Swiss prices identified as highly discriminating). The study extends prior work by incorporating previously unused predictive features such as price histories of neighboring countries.
- **Key Contributions:**
  - Applies a rigorous, transparent, and reproducible methodology across three European areas and two separate test periods.
  - Uses Explainable ML (SHAP values) to link model results to business applications through feature importance analysis.
  - Finds that production/consumption forecasts and cross-border prices (features available without lag) are the most discriminating features.
  - Demonstrates that including new features dramatically increases model performance.
- **Results:** ML models significantly outperform naive benchmarks when supplied with rich feature sets. Feature engineering and selection are as important as model architecture choice.

### Paper 2: "Deep Learning for Day-Ahead Electricity Price Forecasting" (Zhang et al., IET Smart Grid, 2020)
**Source:** [IET Smart Grid, Volume 3, Issue 4, pp. 462-469](https://ietresearch.onlinelibrary.wiley.com/doi/full/10.1049/iet-stg.2019.0258)

- **Model Architecture:** Deep Recurrent Neural Network (DRNN) -- a multi-layer RNN architecture designed to explore complex dependence structures in multivariate electricity price forecasting models.
- **Features Used:** Electricity price and external factors (load, generation) from the New England electricity market.
- **Key Contributions:**
  - Proposes that DRNNs can learn indirect relationships between electricity price and external factors through diverse activation functions and multi-layer structure.
  - Outperforms single SVM by 29.71% and improved hybrid SVM networks by 21.04% in MAPE.
- **Results:** Validated on New England market data. Demonstrates that deep architectures significantly improve upon shallow models for EPF, though the study focuses on point forecasting rather than probabilistic methods.

---

## 2. GitHub Repo Analysis

### 2.1 [Octiembre80/DA-electricity-price-forecasting](https://github.com/Octiembre80/DA-electricity-price-forecasting)
- **Market:** German EPEX day-ahead market
- **Approach:** Deep neural networks as a Udacity ML Nanodegree capstone. Uses a multi-module pipeline: data preprocessing, feature extraction producing "bottleneck features," and a final linear model combining intermediate outputs.
- **Features:** Raw generation data, load forecast data, weather data (36+ hours to collect via custom scraper).
- **Reusability:** The pipeline design (preprocessing -> feature extraction -> final model) is a good architectural pattern. The weather data acquisition script is a useful reference.

### 2.2 [madagra/energy-ts-analysis](https://github.com/madagra/energy-ts-analysis)
- **Market:** Industrial energy consumption in Italy (not price forecasting)
- **Approach:** Time series forecasting of energy consumption using XGBoost. Demonstrates complete workflow from feature engineering to forecasting.
- **Features:** Hourly energy consumption with 24-hour seasonal component identified via autocorrelation analysis.
- **Reusability:** The XGBoost time series workflow and feature engineering approach (lag features, seasonal decomposition) is directly applicable to price forecasting.

### 2.3 [ehardwick2/Energy_Demand_Forecasting](https://github.com/ehardwick2/Energy_Demand_Forecasting)
- **Market:** Spain electricity demand
- **Approach:** Compares Linear Regression, Random Forest, KNN, SARIMAX, FB Prophet, and XGBoost for long-term demand forecasting (7 months ahead).
- **Features:** Total load (MWH), temperature, pressure, humidity, wind speed, rain, snow, cloud cover. One-hot encoded calendar features (hour, day of week, workday, season). Fourier series terms for multiple seasonal periods (via sktime).
- **Reusability:** The Fourier feature engineering for capturing multiple seasonal periods is highly relevant for hourly price forecasting. Prophet and XGBoost showed best long-term performance.

### 2.4 [Morgan-Sell/caiso-price-forecast](https://github.com/Morgan-Sell/caiso-price-forecast)
- **Market:** CAISO day-ahead wholesale electricity
- **Approach:** Compares historic average, ARIMA, and LSTM for 10-day hourly forecasts across all main CAISO trading hubs.
- **Features:** ~11,500 hours of data (March 2019 - May 2020). California-specific hourly price, generation, consumption, net export (from CAISO OASIS). Henry Hub daily natural gas spot price (from FRED).
- **Key Finding:** LSTM outperformed ARIMA with 40-50% lower RMSE.
- **Reusability:** The inclusion of natural gas spot price as a feature and the OASIS data pipeline pattern are directly applicable. Demonstrates importance of fuel price features.

### 2.5 [manukalia/CA_Electricty_Price_Prediction_Neural_Net](https://github.com/manukalia/CA_Electricty_Price_Prediction_Neural_Net)
- **Market:** California wholesale electricity (DAM and HASP)
- **Approach:** ARIMA, SARIMAX, and Recursive Neural Network.
- **Features:** 16 weather features (from 4 California NOAA stations: temperature, wind speed, cloud ceiling, visibility), 1 water level feature (CA Dept. of Water Resources), 4 datetime features, 4 electricity demand forecast features, 1 real-time spot settlement price.
- **Key Finding:** ARIMA performed surprisingly well, matching RNNs. RNNs were much faster to fit than ARIMA. SARIMAX was the worst performer.
- **Reusability:** Excellent example of rich feature engineering with multiple weather stations and domain-specific features (reservoir water levels for hydro impact). The feature set design is a useful reference.

### 2.6 [piekarsky/Short-Term-Electricity-Price-Forecasting-at-the-Polish-Day-Ahead-Market](https://github.com/piekarsky/Short-Term-Electricity-Price-Forecasting-at-the-Polish-Day-Ahead-Market)
- **Market:** Polish SPOT (Day-Ahead) market
- **Approach:** Compares RNN, LSTM, GRU, MLP, and Prophet for 24-hour ahead forecasting. Vanilla RNN achieved highest accuracy.
- **Features:** Autocorrelated lagged electricity prices (7-14 day lags in 24-hour multiples), energy demand forecasts, wind generation forecasts, time of day, holidays.
- **Key Finding:** Lagged prices at 7-14 day intervals (weekly seasonality) are highly predictive. Demand and wind generation forecasts published daily are key external features.
- **Reusability:** The lag structure design (7-14 day multiples of 24 hours) is directly applicable to PJM price forecasting. Demonstrates importance of weekly seasonality in electricity prices.

### 2.7 [b3nn0/EpexPredictor](https://github.com/b3nn0/EpexPredictor)
- **Market:** EPEX day-ahead prices for various European countries
- **Approach:** LightGBM gradient boosting model. Claims performance better than many commercial solutions.
- **Features:** Weather data from Open-Meteo.com for multiple locations per region (past 120 days default), grid load data from ENTSO-E, holiday/Sunday indicators, sunrise/sunset.
- **Key Finding:** LightGBM automatically learns non-linear relationships and feature interactions, making it well-suited for electricity price prediction where factors like low wind+solar can cause price spikes via merit order pricing.
- **Reusability:** **Highly relevant.** This is a practical, production-oriented implementation using LightGBM -- the same approach recommended for our project. The weather data acquisition from Open-Meteo and the feature design are directly reusable patterns.

### 2.8 [plotly/dash-sample-apps/dash-peaky-finders](https://github.com/plotly/dash-sample-apps/tree/main/apps/dash-peaky-finders)
- **Market:** US ISOs (multiple) -- peak load forecasting
- **Approach:** Plotly Dash visualization app with a simple day-ahead forecasting model using historical load and temperature data.
- **Features:** Historical load data (via Pyiso library), weather/temperature data (Darksky API).
- **Key Finding:** Demonstrates the boomerang-shaped relationship between peak demand and temperature (high demand at both temperature extremes, low during shoulder seasons).
- **Reusability:** The visualization dashboard pattern and the temperature-demand relationship insight are useful for building monitoring/visualization tools. Not directly applicable for price forecasting models.

---

## 3. State of the Art: Best Practices for Probabilistic DA LMP Forecasting

Based on the comprehensive literature review, the following best practices have emerged as of 2024-2025:

### 3.1 Model Architecture Trends
- **Before 2022:** Most studies optimized pointwise losses (MAE, MSE) -- pure point forecasting.
- **After 2023:** Increasing adoption of distributional or quantile-based losses to capture uncertainty. The shift reflects growing recognition that point forecasts alone are insufficient for risk-aware decision making, especially under high renewable penetration.
- **2024-2025:** Multi-country, multi-timestep, and multi-quantile approaches are becoming standard. Transformer-based architectures (especially Temporal Fusion Transformers) are gaining traction alongside gradient boosting methods.

### 3.2 Benchmark Models
The community has converged on two key benchmarks from the [epftoolbox](https://github.com/jeslago/epftoolbox) (Lago, Marcjasz, De Schutter, Weron, Applied Energy 2021):
1. **LEAR (Lasso Estimated AutoRegressive):** A parameter-rich ARX model with L1 regularization (LASSO) for implicit feature selection. Often achieves state-of-the-art results despite being a linear model. Training window typically 2 years.
2. **DNN (Deep Neural Network):** A multi-layer feedforward network. Competitive with LEAR but with higher computational cost and more hyperparameters.

### 3.3 Winning Competition Approaches
- **GEFCom2014 (Price Track):** The top two winning teams both used variants of Quantile Regression Averaging (QRA), establishing QRA as a dominant paradigm.
- **Recent competitions:** Ensemble methods combining multiple point forecasters with quantile regression post-processing consistently outperform single-model approaches.

### 3.4 PJM-Specific Insights
- PJM demand time series peaks have a clear positive correlation with system price and price spikes.
- Weather impact alone cannot explain price peaks -- demand forecasts must be included as external features.
- Different models (or parameters) should be trained for different day-of-week groups (Monday-Wednesday, Thursday-Friday, Saturday, Sunday).
- Achievable MAPE of 6-7% for DA price forecasting in PJM using well-tuned ML models.
- PJM LMP consists of three components: Energy cost, Congestion cost, and Loss cost. Forecasting total LMP is standard practice.

---

## 4. Recommended Approaches

### Approach 1: LightGBM Quantile Regression (RECOMMENDED -- Primary Model)

**Architecture:** Train separate LightGBM models for each quantile (e.g., 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95) and each hour of the day (24 hours x 7 quantiles = 168 models, or use a single model with hour as a feature).

**Pros:**
- Natively supports quantile regression via the `quantile` objective -- no custom loss function needed.
- Extremely fast training and inference (critical for daily retraining).
- Automatically handles non-linear relationships and feature interactions (e.g., low wind + high demand -> price spike).
- Robust to outliers and missing data.
- Excellent performance on tabular/structured data, which electricity price data fundamentally is.
- LightGBM's leaf-wise tree growth is more efficient than XGBoost's depth-wise approach.
- High fidelity in tail quantile prediction, which is critical for capturing price spikes.
- Easy to interpret via SHAP values.
- Production-proven: EpexPredictor uses this approach and claims better performance than many commercial solutions.

**Cons:**
- Cannot natively capture sequential/temporal dependencies -- requires manual lag feature engineering.
- Separate models per quantile can lead to quantile crossing (predicted 90th percentile < 50th percentile). This can be mitigated by post-processing (sorting/isotonic regression).
- Does not provide guaranteed coverage without additional conformal prediction layer.

**Implementation Notes:**
- Use `objective='quantile'` with `alpha` parameter for each quantile.
- Train with 1-2 years of rolling historical data.
- Consider separate models for weekday groups (Mon-Wed, Thu-Fri, Sat, Sun) per PJM-specific best practices.
- Retrain daily with most recent data.

### Approach 2: Quantile Regression Averaging (QRA) with Ensemble Point Forecasts

**Architecture:** Train multiple diverse point forecasting models (LEAR/LASSO, LightGBM, DNN, possibly LSTM), then combine their point forecasts using quantile regression to produce probabilistic forecasts.

**Pros:**
- Proven approach -- won GEFCom2014 price track.
- Leverages diversity of multiple models to improve both accuracy and calibration.
- Simple to implement once point forecasters are built.
- Naturally produces well-calibrated prediction intervals.
- Can incorporate any new point forecasting model as it becomes available.
- Extensions like Smoothing QRA and Regularized QRA have shown profit increases of up to 3.5% over point forecast strategies.

**Cons:**
- Requires building and maintaining multiple point forecasting models.
- More complex pipeline with multiple training stages.
- Performance depends on diversity of base models -- similar models add little value.
- QRA coefficients need regular re-estimation as market conditions change.

**Implementation Notes:**
- Base models: LEAR (via epftoolbox), LightGBM point forecast, and a simple DNN.
- Use `statsmodels.regression.quantile_regression.QuantReg` for the combining step.
- Estimate QRA models for quantiles: 0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99.
- Consider the Smoothing QRA variant for improved tail behavior.

### Approach 3: Penalized Temporal Fusion Transformer (TFT) with Conformal Prediction

**Architecture:** Use a Temporal Fusion Transformer with LASSO-based expert models for point forecasts, regularized with SCAD (Smoothly Clipped Absolute Deviation), and apply conformal prediction for post-hoc calibration of prediction intervals.

**Pros:**
- TFT natively produces multi-quantile probabilistic forecasts via `QuantileRegression` loss.
- Interpretable: provides variable importance and temporal attention patterns.
- Handles multiple input types: static covariates (node ID), known future inputs (calendar features, forecasts), and observed past inputs (historical prices, weather).
- Conformal prediction layer provides finite-sample coverage guarantees.
- On-line recalibration procedure adapts to changing market conditions.
- State-of-the-art probabilistic performance on Nord Pool and Polish markets (Jiang, 2024).

**Cons:**
- Most complex architecture to implement and maintain.
- Requires significant computational resources for training.
- Transformer models can overfit on smaller datasets.
- Conformal prediction adds another layer of complexity.
- Longer development time.
- May be overkill for a single-node (Western Hub) forecasting problem.

**Implementation Notes:**
- Use the `darts` library (unit8) which provides TFTModel with built-in quantile regression.
- Apply Adaptive Conformal Inference (ACI) with asymmetric formulation for post-hoc calibration.
- Consider the on-line conformalized ensemble approach (Arxiv 2404.02722) for best coverage.

### Recommendation

**Start with Approach 1 (LightGBM Quantile Regression)** as the primary model. It offers the best trade-off between implementation speed, performance, and interpretability. Layer on conformal prediction for coverage guarantees once the base model is working. If resources allow, build Approach 2 (QRA) as a second system to ensemble with Approach 1 for improved robustness.

---

## 5. Key Features for PJM Western Hub DA LMP Forecasting

### 5.1 Target Variable
- PJM Western Hub DA LMP total price (hourly, 24 values per day)

### 5.2 Historical Price Features (Autoregressive)
| Feature | Description | Lag Structure |
|---------|-------------|---------------|
| DA LMP (t-24) | Yesterday's same-hour price | 24h lag |
| DA LMP (t-48) | Two days ago same-hour price | 48h lag |
| DA LMP (t-168) | Same hour, same weekday last week | 7-day lag |
| DA LMP (t-336) | Same hour, same weekday two weeks ago | 14-day lag |
| DA LMP rolling mean (7d) | 7-day rolling average price | Smoothed |
| DA LMP rolling mean (30d) | 30-day rolling average price | Smoothed |
| DA LMP rolling std (7d) | 7-day rolling std (volatility proxy) | Smoothed |
| RT LMP (t-1 to t-24) | Recent real-time prices | Short-term signal |
| DA-RT spread history | Historical DA-RT price spread | Risk signal |

### 5.3 Load/Demand Features
| Feature | Description | Source |
|---------|-------------|--------|
| PJM system load forecast | Official PJM day-ahead load forecast | PJM Data Miner |
| PJM actual load (lagged) | Yesterday's actual system load | PJM Data Miner |
| Zonal load forecast | Western Hub zone load forecast | PJM Data Miner |
| Load forecast error (lagged) | Yesterday's forecast vs actual error | Computed |
| Peak load indicator | Binary: is this a peak demand hour? | Computed |

### 5.4 Weather Features
| Feature | Description | Source |
|---------|-------------|--------|
| Temperature forecast (multiple stations) | Hourly temperature for PJM Western Hub area | Open-Meteo / NOAA |
| Heating/Cooling Degree Days (HDD/CDD) | Temperature deviation from 65F baseline | Computed |
| Wind speed forecast | Affects both demand (wind chill) and wind generation | Open-Meteo / NOAA |
| Cloud cover / solar irradiance | Affects solar generation and lighting demand | Open-Meteo |
| Temperature squared | Captures U-shaped demand-temperature relationship | Computed |

### 5.5 Fuel Price Features
| Feature | Description | Source |
|---------|-------------|--------|
| Henry Hub natural gas spot price | Primary marginal fuel for PJM generators | FRED / EIA |
| Henry Hub futures (next month) | Forward-looking gas price signal | CME / FRED |
| Coal price index | Secondary fuel price | EIA |
| Gas-coal spread | Fuel switching signal | Computed |

### 5.6 Generation/Supply Features
| Feature | Description | Source |
|---------|-------------|--------|
| Wind generation forecast | Day-ahead wind generation forecast for PJM | PJM Data Miner |
| Solar generation forecast | Day-ahead solar generation forecast for PJM | PJM Data Miner |
| Total renewable forecast | Combined wind + solar | Computed |
| Net load forecast | Load forecast minus renewable forecast | Computed |
| Planned outages | Scheduled generator maintenance | PJM OASIS |
| Import/export schedule | Interchange with neighboring RTOs | PJM Data Miner |

### 5.7 Calendar/Temporal Features
| Feature | Description | Encoding |
|---------|-------------|----------|
| Hour of day | 0-23 | Cyclical (sin/cos) or one-hot |
| Day of week | Monday=0 to Sunday=6 | Cyclical (sin/cos) or one-hot |
| Month | 1-12 | Cyclical (sin/cos) |
| Is weekend | Binary | 0/1 |
| Is holiday | US federal + PJM-observed holidays | 0/1 |
| Day-of-week group | Mon-Wed=0, Thu-Fri=1, Sat=2, Sun=3 | Categorical |
| Season | Winter/Spring/Summer/Fall | One-hot |
| Fourier terms | Capture annual and weekly seasonality | sin/cos pairs at multiple frequencies |

### 5.8 Market/Structural Features
| Feature | Description | Source |
|---------|-------------|--------|
| Congestion component (lagged) | Historical congestion pricing | PJM Data Miner |
| Loss component (lagged) | Historical marginal loss pricing | PJM Data Miner |
| Neighboring hub prices (lagged) | AEP, APS, COMED hub DA LMPs | PJM Data Miner |
| FTR auction prices | Financial transmission rights | PJM |

### 5.9 Feature Priority (Based on Literature)
1. **Highest impact:** Load/demand forecasts, historical price lags (especially 24h, 168h), hour of day
2. **High impact:** Temperature/HDD/CDD, natural gas prices, wind generation forecast, day of week
3. **Medium impact:** Solar generation, net load, cross-hub prices, congestion history, holiday indicator
4. **Lower impact (but useful):** Humidity, wind speed (non-generation), cloud cover, coal prices, FTR prices

---

## 6. Probabilistic Methods for Prediction Intervals/Quantiles

### 6.1 Direct Quantile Regression

**How it works:** Train separate models (or a single model with quantile loss) to directly predict conditional quantiles of the price distribution (e.g., 10th, 50th, 90th percentiles).

**Implementations:**
- **LightGBM:** Set `objective='quantile'`, `alpha=tau` for each quantile tau. Natively supported, fast, and robust.
- **Linear Quantile Regression:** `statsmodels.regression.quantile_regression.QuantReg`. Simple, interpretable, good baseline.
- **Neural Network Quantile Regression:** Use pinball loss as the training objective. Can be combined with any architecture (MLP, LSTM, Transformer).

**Strengths:** Simple, direct, model-agnostic. No distributional assumptions.
**Weaknesses:** Quantile crossing possible. No formal coverage guarantees.

### 6.2 Quantile Regression Averaging (QRA)

**How it works:** (1) Train K diverse point forecasting models. (2) For each quantile tau, fit a quantile regression of actual prices on the K point forecasts. The QRA predictions form the probabilistic forecast.

**Key variants:**
- **Standard QRA** (Nowotarski & Weron, 2015): Original formulation. Won GEFCom2014.
- **Factor QRA (FQRA):** Uses PCA to automatically select from a large pool of point forecasters.
- **Regularized QRA:** Adds LASSO/elastic net regularization to the combining quantile regression.
- **Smoothing QRA (SQRA):** Applies kernel smoothing to quantile predictions for smoother density estimates. Up to 3.5% profit improvement over standard QRA.
- **Expectile Regression Averaging (ERA):** Replaces quantile regression with expectile regression for smoother and more stable estimates.

**Strengths:** Proven track record. Leverages model diversity. Well-calibrated. Easy to extend.
**Weaknesses:** Requires multiple base models. Two-stage pipeline.

### 6.3 Conformal Prediction (CP)

**How it works:** Uses a calibration set to compute nonconformity scores (residuals) from a trained model, then constructs prediction intervals that provide finite-sample coverage guarantees. Key variants for time series:
- **Ensemble Batch Prediction Intervals (EnbPI):** Batch conformal prediction adapted for time series with ensemble models.
- **Sequential Predictive Conformal Inference (SPCI):** On-line conformal method that sequentially updates prediction intervals.
- **Adaptive Conformal Inference (ACI):** Uses quantile tracking and coverage error integration to adapt intervals to non-stationarity.
- **Asymmetric CP:** Separate calibration for upper and lower prediction bands, critical for asymmetric electricity price distributions.

**Recent advances (2024-2025):**
- On-line conformalized neural network ensembles (Arxiv 2404.02722) deploy conformal inference within an on-line recalibration procedure, achieving improved hourly coverage and stable probabilistic scores across multiple markets.
- Conformal prediction combined with quantile regression ensemble (Arxiv 2502.04935) delivers both narrow prediction intervals AND high coverage -- the best of both worlds.

**Strengths:** Formal coverage guarantees. Distribution-free. Can be applied to any base model. On-line variants adapt to non-stationarity.
**Weaknesses:** Intervals can be wide if base model is poor. Requires careful calibration set management. Marginal coverage (not conditional) in standard formulation.

### 6.4 Temporal Fusion Transformer (TFT) with Quantile Output

**How it works:** TFT architecture with multi-quantile output head trained using quantile (pinball) loss. Natively produces prediction intervals alongside interpretable attention weights.

**Strengths:** End-to-end probabilistic forecasting. Interpretable. Handles mixed input types.
**Weaknesses:** Complex. Computationally expensive. May overfit on small datasets.

### 6.5 Recommended Probabilistic Pipeline

For PJM Western Hub DA LMP forecasting, the recommended probabilistic pipeline combines multiple methods:

```
Stage 1: Base Models (Point + Direct Quantile)
  |-- LightGBM Quantile Regression (primary, 7-9 quantiles)
  |-- LEAR point forecast (via epftoolbox)
  |-- LightGBM point forecast (MSE objective)

Stage 2: Ensemble Combination
  |-- QRA combining point forecasts from Stage 1 base models

Stage 3: Calibration
  |-- Adaptive Conformal Prediction on Stage 1 or Stage 2 output
  |-- On-line recalibration with daily update

Output: Calibrated probabilistic forecast
  |-- Quantiles: 0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99
  |-- 80% PI: [0.10, 0.90]
  |-- 90% PI: [0.05, 0.95]
  |-- 98% PI: [0.01, 0.99]
```

This layered approach provides:
1. **Accuracy** from diverse base models (LightGBM + LEAR)
2. **Calibration** from QRA ensemble combining
3. **Coverage guarantees** from conformal prediction recalibration
4. **Adaptivity** from on-line daily recalibration

---

## 7. Key References

### Academic Papers
- Lago, Marcjasz, De Schutter, Weron (2021). "Forecasting day-ahead electricity prices: A review of state-of-the-art algorithms, best practices and an open-access benchmark." Applied Energy, 293, 116983. [Link](https://www.sciencedirect.com/science/article/pii/S0306261921004529)
- Nowotarski, Weron (2015). "Computing electricity spot price prediction intervals using quantile regression and forecast averaging." Computational Statistics, 30(3). [Link](https://link.springer.com/article/10.1007/s00180-014-0523-0)
- Lehna et al. (2022). "Electricity price forecasting on the day-ahead market using machine learning." Applied Energy, 313. [Link](https://hal.science/hal-03621974/document)
- Zhang et al. (2020). "Deep learning for day-ahead electricity price forecasting." IET Smart Grid, 3(4), 462-469. [Link](https://ietresearch.onlinelibrary.wiley.com/doi/full/10.1049/iet-stg.2019.0258)
- Jiang (2024). "Probabilistic electricity price forecasting based on penalized temporal fusion transformer." Journal of Forecasting, 43(5), 1465-1491. [Link](https://onlinelibrary.wiley.com/doi/10.1002/for.3084)
- On-line Conformalized Neural Network Ensembles (2024/2025). Applied Energy, 398. [Link](https://arxiv.org/html/2404.02722)
- Conformal Prediction for Electricity Price Forecasting (2025). [Link](https://arxiv.org/abs/2502.04935)
- Marcjasz et al. (2023). "Smoothing Quantile Regression Averaging: A new approach to probabilistic forecasting of electricity prices." [Link](https://arxiv.org/html/2302.00411v3)
- Jiang (2024). "Electricity price forecasting using quantile regression averaging with nonconvex regularization." Journal of Forecasting. [Link](https://onlinelibrary.wiley.com/doi/10.1002/for.3103)

### Tools and Libraries
- [epftoolbox](https://github.com/jeslago/epftoolbox) -- Open-access benchmark for EPF with LEAR and DNN models, includes PJM data.
- [LightGBM](https://lightgbm.readthedocs.io/) -- Gradient boosting with native quantile regression support.
- [darts (unit8)](https://unit8co.github.io/darts/) -- TFTModel with built-in quantile regression and conformal prediction.
- [MAPIE](https://mapie.readthedocs.io/) -- Conformal prediction for Python.
- [statsmodels QuantReg](https://www.statsmodels.org/stable/generated/statsmodels.regression.quantile_regression.QuantReg.html) -- Linear quantile regression.

### Data Sources for PJM
- [PJM Data Miner 2](https://dataminer2.pjm.com/) -- DA/RT LMPs, load forecasts, generation data.
- [PJM Data Viewer](https://dataviewer.pjm.com/) -- Interactive price data visualization.
- [FRED (Henry Hub)](https://fred.stlouisfed.org/) -- Natural gas spot prices.
- [Open-Meteo](https://open-meteo.com/) -- Free weather forecast and historical API.
- [Grid Status](https://www.gridstatus.io/live/pjm) -- PJM live dashboard and API.
