from datetime import date, timedelta

# Database
SCHEMA: str = "dbt_pjm_v1_2026_feb_19"

# Target
HUB: str = "WESTERN HUB"
NEIGHBORING_HUBS: list[str] = ["EASTERN", "AEP-DAYTON", "DOMINION", "CHICAGO"]
TARGET_COL: str = "lmp_total"
TARGET_MARKET: str = "da"

# Quantiles for probabilistic output
QUANTILES: list[float] = [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]
HOURS: list[int] = list(range(1, 25))

# Date columns
DATE_COL: str = "date"
HOUR_COL: str = "hour_ending"

# Day-of-week groups for separate model training (PJM-specific)
DOW_GROUPS: dict[str, list[int]] = {
    "weekday_early": [0, 1, 2],     # Mon-Wed
    "weekday_late": [3, 4],          # Thu-Fri
    "saturday": [5],
    "sunday": [6],
}

# Multi-window calibration (Lago 2021)
CALIBRATION_WINDOWS: dict[str, int] = {
    "8w": 56,
    "12w": 84,
    "3y": 1095,
    "4y": 1460,
}

# LightGBM hyperparameters
LGBM_PARAMS: dict = {
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
VST: str = "asinh"

# Training window strategy (from data assessment)
FULL_FEATURE_START: str = "2020-01-01"
EXTENDED_FEATURE_START: str = "2014-01-01"

# Gas hubs
GAS_HUBS: list[str] = ["M3", "HH", "Transco Z6 NY"]
GAS_PRIMARY_HUB: str = "M3"

# Load region
LOAD_REGION: str = "RTO"

# Lag days for price features (Lago 2021 + Polish market research)
PRICE_LAG_DAYS: list[int] = [1, 2, 3, 7, 14]
