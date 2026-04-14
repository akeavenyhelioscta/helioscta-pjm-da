"""Base adapter contract for model-agnostic backtesting."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Protocol

import pandas as pd


@dataclass
class ForecastResult:
    """Normalized single-day forecast payload."""

    model: str
    forecast_date: date
    reference_date: date
    point_by_he: dict[int, float]
    quantiles_by_he: dict[int, dict[float, float]]
    metadata: dict = field(default_factory=dict)


class ForecastAdapter(Protocol):
    """Adapter interface implemented by each forecasting model."""

    name: str
    quantiles: list[float]

    def forecast_for_date(
        self,
        forecast_date: date,
        force_retrain: bool = False,
        df_features: pd.DataFrame | None = None,
    ) -> ForecastResult:
        """Generate one-day ahead forecast for a delivery date."""
        ...
