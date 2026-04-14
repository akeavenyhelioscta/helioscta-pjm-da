"""Configuration for the supply stack forecast model."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

SCHEMA: str = "pjm_cleaned"
HUB: str = "WESTERN HUB"
REGION: str = "RTO"
OUTAGE_COLUMN: str = "total_outages_mw"

DEFAULT_QUANTILES: list[float] = [0.10, 0.25, 0.50, 0.75, 0.90]

_BACKEND_ROOT: Path = Path(__file__).resolve().parents[2]
_DEFAULT_CACHE_DIR: Path = _BACKEND_ROOT / "cache"
_DEFAULT_FLEET_PATH: Path = Path(__file__).resolve().parent / "data" / "pjm_fleet.csv"


@dataclass
class SupplyStackConfig:
    """All tunable parameters for a single supply stack forecast run."""

    forecast_date: str | date | None = None
    schema: str = SCHEMA
    hub: str = HUB

    region: str = REGION
    region_preset: str | None = None
    gas_hub_col: str | None = None
    outage_column: str = OUTAGE_COLUMN
    outages_lookback_days: int = 14

    congestion_adder_usd: float = 3.0
    coal_price_usd_mmbtu: float = 2.5
    oil_price_usd_mmbtu: float = 15.0
    scarcity_price_cap_usd_mwh: float = 500.0

    quantiles: list[float] = field(default_factory=lambda: list(DEFAULT_QUANTILES))
    n_monte_carlo_draws: int = 300
    monte_carlo_seed: int | None = 7
    net_load_error_std_pct: float = 0.025
    gas_price_error_std_pct: float = 0.05
    outage_error_std_pct: float = 0.08

    cache_dir: Path = field(
        default_factory=lambda: Path(os.getenv("CACHE_DIR", str(_DEFAULT_CACHE_DIR)))
    )
    cache_enabled: bool = field(
        default_factory=lambda: os.getenv("CACHE_ENABLED", "true").lower()
        in ("true", "1", "yes")
    )
    cache_ttl_hours: float = field(
        default_factory=lambda: float(os.getenv("CACHE_TTL_HOURS", "4"))
    )
    force_cache_refresh: bool = field(
        default_factory=lambda: os.getenv("FORCE_CACHE_REFRESH", "false").lower()
        in ("true", "1", "yes")
    )

    fleet_csv_path: Path = field(default_factory=lambda: _DEFAULT_FLEET_PATH)

    def resolved_forecast_date(self) -> date:
        """Return forecast delivery date, defaulting to tomorrow."""
        if self.forecast_date is None:
            return datetime.now().date() + timedelta(days=1)
        if isinstance(self.forecast_date, date):
            return self.forecast_date
        return pd.to_datetime(self.forecast_date).date()

    def sorted_quantiles(self) -> list[float]:
        """Return normalized, sorted quantiles in [0, 1]."""
        cleaned = [min(1.0, max(0.0, float(q))) for q in self.quantiles]
        return sorted(set(cleaned))
