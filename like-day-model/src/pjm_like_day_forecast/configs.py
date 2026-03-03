"""Central configuration for the like-day probabilistic forecast model."""
from datetime import date, timedelta

# Database
SCHEMA: str = "dbt_pjm_v1_2026_feb_19"

# Target
HUB: str = "WESTERN HUB"
TARGET_COL: str = "lmp_total"
TARGET_MARKET: str = "da"

# Date columns
DATE_COL: str = "date"
HOUR_COL: str = "hour_ending"

# Quantiles for probabilistic output (matches da-model for comparability)
QUANTILES: list[float] = [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]
HOURS: list[int] = list(range(1, 25))

# LMP feature columns
LMP_COLS: list[str] = [
    "lmp_total",
    "lmp_system_energy_price",
    "lmp_congestion_price",
    "lmp_marginal_loss_price",
]

# Analog selection defaults
DEFAULT_N_ANALOGS: int = 30
DEFAULT_N_DISPLAY: int = 5

# Feature group weights for similarity (expert defaults)
FEATURE_GROUP_WEIGHTS: dict[str, float] = {
    "lmp_profile": 3.0,
    "lmp_level": 2.0,
    "lmp_volatility": 1.0,
    "load_level": 2.0,
    "load_shape": 1.0,
    "gas_price": 2.0,
    "gas_momentum": 0.5,
    "calendar_dow": 1.5,
    "calendar_season": 1.0,
    "weather_level": 2.5,
    "weather_hdd_cdd": 3.0,
    "weather_wind": 0.5,
    "composite_heat_rate": 2.0,
    "target_load_level": 2.5,
    "target_load_shape": 1.0,
    "target_weather_level": 2.0,
    "target_weather_hdd_cdd": 2.5,
}

# Pre-filtering defaults
FILTER_SAME_DOW_GROUP: bool = True
FILTER_SEASON_WINDOW_DAYS: int = 30
FILTER_MIN_POOL_SIZE: int = 20

# Day-of-week groups (PJM-specific)
DOW_GROUPS: dict[str, list[int]] = {
    "weekday": [0, 1, 2, 3, 4],     # Mon-Fri
    "saturday": [5],
    "sunday": [6],
}

# Weather
WEATHER_SCHEMA: str = "dbt_wsi_temps_v1_2026_feb_25"
WEATHER_REGION: str = "PJM"
WEATHER_STATION: str = "PJM"
HDD_BASE_TEMP: float = 65.0
CDD_BASE_TEMP: float = 65.0

# Gas hubs
GAS_HUBS: list[str] = ["M3", "HH", "Transco Z6 NY"]
GAS_PRIMARY_HUB: str = "M3"

# Load region
LOAD_REGION: str = "RTO"

# Variance stabilizing transformation
VST: str = "asinh"

# Data start dates
FULL_FEATURE_START: str = "2020-01-01"
EXTENDED_FEATURE_START: str = "2021-01-01"
