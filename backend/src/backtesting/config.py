"""Configuration objects for shared backtesting workflows."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pandas as pd

from src.like_day_forecast import configs as ld_configs

DEFAULT_QUANTILES = [0.01, 0.05, 0.10, 0.25, 0.375, 0.50, 0.625, 0.75, 0.90, 0.95, 0.99]


@dataclass
class BacktestConfig:
    """Settings for walk-forward model comparison."""

    start_date: str
    end_date: str
    models: list[str] = field(default_factory=lambda: ["like_day", "lasso_qr"])
    quantiles: list[float] = field(default_factory=lambda: list(DEFAULT_QUANTILES))

    # LASSO retrain cadence (days). 1 = strict retrain each forecast date.
    retrain_every_n_days: int = 1

    hub: str = ld_configs.HUB
    schema: str = ld_configs.SCHEMA

    cache_dir: Path = field(default_factory=lambda: ld_configs.CACHE_DIR)
    cache_enabled: bool = ld_configs.CACHE_ENABLED
    cache_ttl_hours: float = ld_configs.CACHE_TTL_HOURS
    force_refresh: bool = ld_configs.FORCE_CACHE_REFRESH

    output_dir: Path = field(
        default_factory=lambda: Path("backend/output/backtests"),
    )
    max_days: int | None = None
    drop_incomplete_days: bool = True
    weekdays_only: bool = False

    def forecast_dates(self) -> list[date]:
        """Inclusive date range in ascending order."""
        start = pd.to_datetime(self.start_date).date()
        end = pd.to_datetime(self.end_date).date()
        if end < start:
            raise ValueError(f"end_date {end} is before start_date {start}")
        dates = [
            d.date()
            for d in pd.date_range(start=start, end=end, freq="D")
        ]
        if self.weekdays_only:
            dates = [d for d in dates if d.weekday() < 5]  # Mon=0..Fri=4
        if self.max_days is not None:
            return dates[: self.max_days]
        return dates
