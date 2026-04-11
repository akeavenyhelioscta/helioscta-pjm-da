"""Central configuration for the like-day probabilistic forecast model."""
from __future__ import annotations

import os
import copy
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from src.utils.day_type import (
    DAY_TYPE_SATURDAY,
    DAY_TYPE_SUNDAY,
    DAY_TYPE_WEEKDAY,
    resolve_day_type,
)

# Database
SCHEMA: str = "pjm_cleaned"

# ── Cache ────────────────────────────────────────────────────────────
_BACKEND_ROOT: Path = Path(__file__).resolve().parent.parent.parent  # backend/
_DEFAULT_CACHE_DIR: Path = _BACKEND_ROOT / "cache"
CACHE_DIR: Path = Path(os.getenv("CACHE_DIR", str(_DEFAULT_CACHE_DIR)))
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
DEFAULT_N_ANALOGS: int = 15
DEFAULT_N_DISPLAY: int = 5

# Feature group weights for similarity (expert defaults)
FEATURE_GROUP_WEIGHTS: dict[str, float] = {
    "lmp_profile": 0.5,           # keep as weak tie-breaker only
    "lmp_level": 0.5,             # keep as weak tie-breaker only
    "lmp_volatility": 0.5,
    "lmp_ramps": 0.5,
    "load_level": 1.0,             # RTO aggregate — ref-day
    "load_shape": 0.5,
    "load_west_level": 1.5,        # Western Hub pricing zone — most relevant
    "load_ramps": 1.5,             # morning + evening ramp shape (RTO + WEST)
    "load_midatl_level": 1.0,      # Mid-Atlantic
    "load_south_level": 0.5,       # South — least impact on W.Hub
    "gas_hourly_level": 2.0,       # M3 + TZ5 daily avg, on/off-peak, max
    "gas_hourly_shape": 1.5,       # intraday range, morning ramp — captures winter spikes
    "gas_basis_spread": 1.0,       # retain but lower vs load/outage drivers
    "calendar_dow": 0.25,          # pre-filter already handles DOW matching
    "calendar_season": 1.0,
    "weather_level": 0,             # disabled — load forecast already captures weather impact
    "weather_hdd_cdd": 0,           # disabled
    "weather_wind": 0,              # disabled
    "composite_heat_rate": 1.0,
    "renewable_level": 0.5,        # ref-day actuals are weaker than target forecasts
    "renewable_shape": 0.25,
    "outage_level": 4.0,           # highest-priority supply-side regime driver
    "outage_composition": 2.0,     # ↑ from 1.0: forced vs planned distinction matters
    "nuclear_level": 1.5,          # baseload floor
    "congestion_level": 1.0,       # keep as secondary signal
    "fuel_mix_shares": 0.0,        # endogenous; disable in analog distance
    "net_load_level": 2.5,         # thermal generation requirement (load - renewables)
    "net_load_ramps": 2.0,         # evening/morning thermal ramp intensity
    "target_weather_level": 0,      # disabled — load forecast captures weather impact
    "target_weather_hdd_cdd": 0,    # disabled
    "target_renewable_level": 3.0,  # core midday displacement driver
    "target_outage_level": 4.0,    # core D+1 supply adequacy signal
    "target_outage_west_level": 2.0, # D+1 WEST outage forecast — W.Hub pricing zone
    "target_load_level": 2.0,      # core demand-level signal
    "target_load_west_level": 2.0,  # D+1 WEST — most relevant for W.Hub
    "target_load_ramps": 2.0,       # D+1 morning + evening ramp forecast (RTO + WEST)
    "target_load_midatl_level": 1.0,
    "target_load_south_level": 0.5,
    "target_meteo_load_level": 1.5,  # Meteologica independent load forecast
}

# Delivery-day profiles for D+1 scheduling effects.
# Weekend profiles tighten day-type matching and downweight circular price-shape groups.
DAY_TYPE_SCENARIO_PROFILES: dict[str, dict[str, Any]] = {
    DAY_TYPE_WEEKDAY: {},
    DAY_TYPE_SATURDAY: {
        "same_dow_group": True,
        "season_window_days": 45,
        "n_analogs": 12,
        "feature_group_weights": {
            "lmp_profile": 0.25,
            "lmp_level": 0.25,
            "target_load_level": 1.75,
            "target_load_ramps": 1.5,
            "target_renewable_level": 3.25,
            "target_outage_level": 4.0,
            "calendar_dow": 0.1,
        },
    },
    DAY_TYPE_SUNDAY: {
        "same_dow_group": True,
        "season_window_days": 60,
        "n_analogs": 10,
        "feature_group_weights": {
            "lmp_profile": 0.2,
            "lmp_level": 0.2,
            "target_load_level": 1.5,
            "target_load_ramps": 1.25,
            "target_renewable_level": 3.25,
            "target_outage_level": 4.0,
            "calendar_dow": 0.1,
        },
    },
}

# Pre-filtering defaults
FILTER_SAME_DOW_GROUP: bool = True
FILTER_SEASON_WINDOW_DAYS: int = 30
FILTER_MIN_POOL_SIZE: int = 20
FILTER_EXCLUDE_HOLIDAYS: bool = True  # auto-exclude NERC holidays when target is not a holiday

# Dates to always exclude from the analog pool.
# Add any dates with anomalous prints (holidays the DB misses, extreme events, bad data).
EXCLUDE_DATES: list[str] = [
    # 2026
    "2026-04-03",  # Good Friday — depressed load/prices
    "2026-04-06",  # Easter Monday — depressed load/prices
    # Add more as needed:
    # "2025-12-26",  # day after Christmas
]

# Outage regime filter: ensure candidate pool matches target outage environment
FILTER_OUTAGE_REGIME: bool = True
FILTER_OUTAGE_TOLERANCE_STD: float = 1.0  # keep candidates within ±1 std of target outage z-score

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

# Renewable forecast sources
RENEWABLE_FORECAST_MODE: str = os.getenv("RENEWABLE_FORECAST_MODE", "blend").lower()
RENEWABLE_FORECAST_REGION: str = os.getenv("RENEWABLE_FORECAST_REGION", "RTO")
RENEWABLE_BLEND_PJM_WEIGHT_D1: float = float(os.getenv("RENEWABLE_BLEND_PJM_WEIGHT_D1", "0.50"))
RENEWABLE_BLEND_PJM_WEIGHT_D7: float = float(os.getenv("RENEWABLE_BLEND_PJM_WEIGHT_D7", "0.75"))

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
    weight_method: str = "softmax"

    # Feature group weights (None → use FEATURE_GROUP_WEIGHTS)
    feature_group_weights: dict[str, float] | None = None
    day_type_profiles: dict[str, dict[str, Any]] | None = None
    use_day_type_profiles: bool = True

    # Pre-filtering
    season_window_days: int = FILTER_SEASON_WINDOW_DAYS
    same_dow_group: bool = FILTER_SAME_DOW_GROUP
    apply_calendar_filter: bool = True
    apply_regime_filter: bool = True
    apply_outage_regime_filter: bool = FILTER_OUTAGE_REGIME
    outage_tolerance_std: float = FILTER_OUTAGE_TOLERANCE_STD
    min_pool_size: int = FILTER_MIN_POOL_SIZE
    exclude_holidays: bool = FILTER_EXCLUDE_HOLIDAYS
    exclude_dates: list[str] = field(default_factory=lambda: list(EXCLUDE_DATES))

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
    renewable_forecast_mode: str = RENEWABLE_FORECAST_MODE
    renewable_forecast_region: str = RENEWABLE_FORECAST_REGION
    renewable_blend_pjm_weight_d1: float = RENEWABLE_BLEND_PJM_WEIGHT_D1
    renewable_blend_pjm_weight_d7: float = RENEWABLE_BLEND_PJM_WEIGHT_D7

    # ── helpers ──────────────────────────────────────────────────

    def resolved_renewable_mode(self) -> str:
        """Return renewable forecast mode constrained to supported values."""
        mode = (self.renewable_forecast_mode or "blend").strip().lower()
        if mode in {"pjm", "meteologica", "blend"}:
            return mode
        return "blend"

    def renewable_blend_weight(self, offset: int = 1) -> float:
        """Return PJM blend weight for D+offset (linear from D+1 to D+7)."""
        w1 = min(1.0, max(0.0, float(self.renewable_blend_pjm_weight_d1)))
        w7 = min(1.0, max(0.0, float(self.renewable_blend_pjm_weight_d7)))
        if offset <= 1:
            return w1
        if offset >= 7:
            return w7
        frac = (offset - 1) / 6.0
        return w1 + (w7 - w1) * frac

    def resolved_weights(self) -> dict[str, float]:
        """Return feature weights, falling back to module defaults."""
        return dict(self.feature_group_weights or FEATURE_GROUP_WEIGHTS)

    def resolved_day_type_profiles(self) -> dict[str, dict[str, Any]]:
        """Return day-type override profiles with defaults filled in."""
        base = copy.deepcopy(DAY_TYPE_SCENARIO_PROFILES)
        if not self.day_type_profiles:
            return base
        for k, v in self.day_type_profiles.items():
            if k not in base:
                base[k] = {}
            if isinstance(v, dict):
                base[k].update(copy.deepcopy(v))
        return base

    def with_day_type_overrides(self, target_date: date) -> tuple["ScenarioConfig", str]:
        """Return config with weekend/weekday profile applied for target date."""
        day_type = resolve_day_type(target_date)
        if not self.use_day_type_profiles:
            return self, day_type

        profile = self.resolved_day_type_profiles().get(day_type, {})
        if not profile:
            return self, day_type

        cfg = copy.deepcopy(self)
        for key, value in profile.items():
            if key == "feature_group_weights":
                merged = cfg.resolved_weights()
                merged.update(copy.deepcopy(value))
                cfg.feature_group_weights = merged
                continue
            if hasattr(cfg, key):
                setattr(cfg, key, copy.deepcopy(value))
        return cfg, day_type

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
            "renewable_forecast_mode": self.resolved_renewable_mode(),
            "renewable_forecast_region": self.renewable_forecast_region,
            "renewable_blend_pjm_weight_d1": self.renewable_blend_pjm_weight_d1,
            "renewable_blend_pjm_weight_d7": self.renewable_blend_pjm_weight_d7,
        }
        for k, v in self.resolved_weights().items():
            d[f"w_{k}"] = v
        return d
