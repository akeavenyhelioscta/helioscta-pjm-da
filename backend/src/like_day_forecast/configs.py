"""Central configuration for the like-day probabilistic forecast model."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

# Database
SCHEMA: str = "pjm_cleaned"

# ── Cache ────────────────────────────────────────────────────────────
CACHE_DIR: Path = Path(__file__).parent / "cache"
CACHE_ENABLED: bool = os.getenv("CACHE_ENABLED", "true").lower() in ("true", "1", "yes")
CACHE_TTL_HOURS: float = float(os.getenv("CACHE_TTL_HOURS", "4"))
FORCE_CACHE_REFRESH: bool = os.getenv("FORCE_CACHE_REFRESH", "false").lower() in ("true", "1", "yes")

# Target
HUB: str = "WESTERN HUB"
TARGET_COL: str = "lmp_total"
TARGET_MARKET: str = "da"
TARGET_DATE: date = datetime.now().date() + timedelta(days=1)

# Date columns
DATE_COL: str = "date"
HOUR_COL: str = "hour_ending"
HOUR_ENDING_COL: str = "hour_ending"

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
FEATURE_COLS: list[str] = LMP_COLS

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
    "renewable_level": 2.5,
    "renewable_shape": 1.0,
    "outage_level": 2.0,
    "outage_composition": 1.0,
    "target_weather_level": 2.0,
    "target_weather_hdd_cdd": 2.5,
    "target_renewable_level": 2.5,
    "target_outage_level": 1.5,
}

# Pre-filtering defaults
FILTER_SAME_DOW_GROUP: bool = True
FILTER_SEASON_WINDOW_DAYS: int = 30
FILTER_MIN_POOL_SIZE: int = 20

# Adaptive filtering for extreme regimes
ADAPTIVE_FILTER_ENABLED: bool = True
ADAPTIVE_EXTREME_THRESHOLD_STD: float = 2.0    # z-score above which regime is "extreme"
ADAPTIVE_SEASON_WINDOW_DAYS: int = 120          # widened window in extreme regime
ADAPTIVE_SAME_DOW_GROUP: bool = False           # relax DOW matching in extreme regime
ADAPTIVE_LMP_TOLERANCE_STD: float = 4.0         # widened regime tolerance (from 1.5)
ADAPTIVE_GAS_TOLERANCE_STD: float = 3.0         # widened gas tolerance (from 1.5)
ADAPTIVE_N_ANALOGS: int = 15                    # fewer analogs to concentrate weight
ADAPTIVE_WEIGHT_METHOD: str = "softmax"          # sharper weighting in extreme regime
ADAPTIVE_SOFTMAX_TEMPERATURE: float = 0.3        # low T = more concentrated on best match

# Day-of-week groups (PJM-specific)
DOW_GROUPS: dict[str, list[int]] = {
    "weekday": [0, 1, 2, 3, 4],     # Mon-Fri
    "saturday": [5],
    "sunday": [6],
}

# Weather
WEATHER_SCHEMA: str = "wsi_cleaned"
WEATHER_STATION: str = "PJM"
HDD_BASE_TEMP: float = 65.0
CDD_BASE_TEMP: float = 65.0

# Gas (columns come from ice_python_cleaned mart directly)
GAS_M3_COL: str = "gas_m3_price"
GAS_HH_COL: str = "gas_hh_price"

# Load region
LOAD_REGION: str = "RTO"

# Variance stabilizing transformation
VST: str = "asinh"

# Data start dates
FULL_FEATURE_START: str = "2020-01-01"
EXTENDED_FEATURE_START: str = "2023-01-01"


@dataclass
class ScenarioConfig:
    """All tunable hyperparameters for a single like-day forecast run.

    Provides a single object to parameterize runs for W&B sweeps, backtesting,
    and reproducibility.  Module-level constants remain the defaults.
    """

    # Identity
    name: str = "default"
    forecast_date: str | None = None  # YYYY-MM-DD; defaults to tomorrow

    # Analog selection
    n_analogs: int = DEFAULT_N_ANALOGS
    weight_method: str = "inverse_distance"

    # Feature group weights (None → use FEATURE_GROUP_WEIGHTS)
    feature_group_weights: dict[str, float] | None = None

    # Pre-filtering
    season_window_days: int = FILTER_SEASON_WINDOW_DAYS
    same_dow_group: bool = FILTER_SAME_DOW_GROUP
    apply_calendar_filter: bool = True
    apply_regime_filter: bool = True
    min_pool_size: int = FILTER_MIN_POOL_SIZE

    # Adaptive filtering for extreme regimes
    adaptive_filter_enabled: bool = ADAPTIVE_FILTER_ENABLED
    adaptive_extreme_threshold_std: float = ADAPTIVE_EXTREME_THRESHOLD_STD
    adaptive_season_window_days: int = ADAPTIVE_SEASON_WINDOW_DAYS
    adaptive_same_dow_group: bool = ADAPTIVE_SAME_DOW_GROUP
    adaptive_lmp_tolerance_std: float = ADAPTIVE_LMP_TOLERANCE_STD
    adaptive_gas_tolerance_std: float = ADAPTIVE_GAS_TOLERANCE_STD
    adaptive_n_analogs: int = ADAPTIVE_N_ANALOGS
    adaptive_weight_method: str = ADAPTIVE_WEIGHT_METHOD
    adaptive_softmax_temperature: float = ADAPTIVE_SOFTMAX_TEMPERATURE

    # Quantiles
    quantiles: list[float] = field(
        default_factory=lambda: list(QUANTILES),
    )

    # Database / target
    schema: str = SCHEMA
    hub: str = HUB

    # ── helpers ──────────────────────────────────────────────────

    def resolved_weights(self) -> dict[str, float]:
        """Return feature weights, falling back to module defaults."""
        return dict(self.feature_group_weights or FEATURE_GROUP_WEIGHTS)

    def to_flat_dict(self) -> dict:
        """Flatten for W&B / MLflow param logging (no nested dicts)."""
        d: dict = {
            "name": self.name,
            "forecast_date": self.forecast_date,
            "n_analogs": self.n_analogs,
            "weight_method": self.weight_method,
            "season_window_days": self.season_window_days,
            "same_dow_group": self.same_dow_group,
            "apply_calendar_filter": self.apply_calendar_filter,
            "apply_regime_filter": self.apply_regime_filter,
            "min_pool_size": self.min_pool_size,
            "schema": self.schema,
            "hub": self.hub,
        }
        for k, v in self.resolved_weights().items():
            d[f"w_{k}"] = v
        return d
