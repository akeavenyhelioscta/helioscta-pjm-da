"""Configuration for LASSO Quantile Regression forecast."""
from __future__ import annotations

import copy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.like_day_forecast import configs as ld_configs
from src.utils.day_type import (
    DAY_TYPE_SATURDAY,
    DAY_TYPE_SUNDAY,
    DAY_TYPE_WEEKDAY,
    resolve_day_type,
)

SCHEMA = ld_configs.SCHEMA
HUB = ld_configs.HUB
CACHE_DIR = ld_configs.CACHE_DIR
CACHE_ENABLED = ld_configs.CACHE_ENABLED
CACHE_TTL_HOURS = ld_configs.CACHE_TTL_HOURS
FORCE_CACHE_REFRESH = ld_configs.FORCE_CACHE_REFRESH

HOURS = list(range(1, 25))
ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = list(range(1, 8)) + [24]
_MODULE_DIR = Path(__file__).resolve().parent

# Columns used per feature_set mode
FUNDAMENTAL_COLS = [
    # Load (target-date forecasts)
    "tgt_load_daily_avg", "tgt_load_daily_peak", "tgt_load_daily_valley",
    "tgt_load_west_daily_avg", "tgt_load_west_daily_peak",
    "tgt_load_morning_ramp", "tgt_load_evening_ramp",
    # Gas
    "gas_m3_daily_avg", "gas_m3_onpeak_avg", "gas_m3_offpeak_avg",
    "gas_dom_south_daily_avg",
    "gas_basis_m3_dom_south",
    # Outages (target-date forecasts + reference-day actuals)
    "tgt_outage_total_mw", "tgt_outage_forced_mw",
    "outage_total_mw", "outage_forced_mw",
    # Renewables (target-date forecasts)
    "tgt_solar_daily_avg", "tgt_wind_daily_avg", "tgt_renewable_daily_avg",
    # Nuclear
    "nuclear_daily_avg",
    # Calendar
    "dow_sin", "dow_cos", "month_sin", "month_cos",
    "day_of_year_sin", "day_of_year_cos",
    "is_weekend",
    # Weather (target-date forecasts)
    "tgt_temp_daily_avg", "tgt_hdd", "tgt_cdd",
]

MINIMAL_COLS = [
    "tgt_load_daily_avg", "tgt_load_daily_peak",
    "gas_m3_daily_avg",
    "tgt_outage_total_mw", "tgt_outage_forced_mw",
    "dow_sin", "dow_cos", "month_sin", "month_cos",
    "is_weekend",
]

# Recommended pruning for "full" feature set based on recent backtest diagnostics.
# Keep these configurable so experiments can toggle them without code changes.
DEFAULT_DROP_FEATURE_PREFIXES = [
    "lmp_profile_h",          # circular regime anchoring
    "fuel_share_",            # endogenous / leakage-prone
    "gas_basis_",             # low incremental value in recent runs
    "congestion_daily_",      # consistently unused
    "congestion_onpeak_avg",  # consistently unused
    "congestion_7d_rolling_std",
    "tgt_outage_west_",       # currently unavailable in source data
    "tgt_meteo_load_",        # currently unavailable in source data
]

DEFAULT_DROP_FEATURE_NAMES = [
    "lmp_congestion_share",
    "implied_heat_rate",
]

DEFAULT_FORCE_INCLUDE_FEATURES = [
    # RT stress and DA-RT spread signals.
    "rt_lmp_daily_flat",
    "dart_spread_daily",
]

DAY_TYPE_LASSO_PROFILES: dict[str, dict[str, Any]] = {
    DAY_TYPE_WEEKDAY: {},
    DAY_TYPE_SATURDAY: {
        "alpha": 0.1,
        "alpha_grid": [0.01, 0.05, 0.1, 0.5, 1.0],
        "include_lagged_lmp": False,
        "drop_feature_prefixes": [
            *DEFAULT_DROP_FEATURE_PREFIXES,
            "load_morning_ramp",
            "load_evening_ramp",
            "net_load_morning_ramp",
            "net_load_evening_ramp",
        ],
    },
    DAY_TYPE_SUNDAY: {
        "alpha": 0.1,
        "alpha_grid": [0.01, 0.05, 0.1, 0.5, 1.0],
        "include_lagged_lmp": False,
        "drop_feature_prefixes": [
            *DEFAULT_DROP_FEATURE_PREFIXES,
            "load_morning_ramp",
            "load_evening_ramp",
            "net_load_morning_ramp",
            "net_load_evening_ramp",
            "lmp_morning_ramp",
            "lmp_evening_ramp",
        ],
    },
}


@dataclass
class LassoQRConfig:
    # ── Identity ─────────────────────────────────────────────
    name: str = "lasso_qr_default"
    forecast_date: str | None = None

    # ── Model ────────────────────────────────────────────────
    quantiles: list[float] = field(
        default_factory=lambda: [0.10, 0.25, 0.50, 0.75, 0.90],
    )
    alpha: float = 0.1
    alpha_search: bool = True
    alpha_grid: list[float] = field(
        default_factory=lambda: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
    )

    # ── Training window ──────────────────────────────────────
    train_window_days: int = 730
    min_train_samples: int = 365
    retrain_if_stale_hours: int = 24

    # ── Feature selection ────────────────────────────────────
    feature_set: str = "full"  # "full", "fundamental", "minimal"
    include_lagged_lmp: bool = False
    include_interaction_terms: bool = True
    drop_feature_prefixes: list[str] = field(
        default_factory=lambda: list(DEFAULT_DROP_FEATURE_PREFIXES),
    )
    drop_feature_names: list[str] = field(
        default_factory=lambda: list(DEFAULT_DROP_FEATURE_NAMES),
    )
    force_include_features: list[str] = field(
        default_factory=lambda: list(DEFAULT_FORCE_INCLUDE_FEATURES),
    )
    day_type_profiles: dict[str, dict[str, Any]] | None = None
    use_day_type_profiles: bool = True
    day_type_tag: str | None = None

    # ── Database / output ────────────────────────────────────
    hub: str = HUB
    schema: str = SCHEMA

    # ── Paths ────────────────────────────────────────────────
    model_dir: Path = field(default_factory=lambda: _MODULE_DIR / "models")
    cache_dir: Path = field(default_factory=lambda: CACHE_DIR)
    cache_enabled: bool = CACHE_ENABLED
    cache_ttl_hours: float = CACHE_TTL_HOURS
    force_refresh: bool = FORCE_CACHE_REFRESH

    def resolved_day_type_profiles(self) -> dict[str, dict[str, Any]]:
        base = copy.deepcopy(DAY_TYPE_LASSO_PROFILES)
        if not self.day_type_profiles:
            return base
        for k, v in self.day_type_profiles.items():
            if k not in base:
                base[k] = {}
            if isinstance(v, dict):
                base[k].update(copy.deepcopy(v))
        return base

    def with_day_type_overrides(self, target_date) -> tuple["LassoQRConfig", str]:
        day_type = resolve_day_type(target_date)
        if not self.use_day_type_profiles:
            cfg = copy.deepcopy(self)
            cfg.day_type_tag = day_type
            return cfg, day_type

        profile = self.resolved_day_type_profiles().get(day_type, {})
        cfg = copy.deepcopy(self)
        cfg.day_type_tag = day_type
        for key, value in profile.items():
            if hasattr(cfg, key):
                setattr(cfg, key, copy.deepcopy(value))
        return cfg, day_type

    def to_flat_dict(self) -> dict:
        return {
            k: str(v) if isinstance(v, (Path, list)) else v
            for k, v in self.__dict__.items()
        }
