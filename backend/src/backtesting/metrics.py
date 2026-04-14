"""Shared scoring helpers for model-comparison backtests."""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from src.like_day_forecast.evaluation.metrics import evaluate_forecast

ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = list(range(1, 8)) + [24]
ALL_HOURS = list(range(1, 25))


@dataclass
class PeriodSlice:
    """Hourly arrays used for period-level metric computation."""

    hours: list[int]
    y_true: np.ndarray
    point_forecast: np.ndarray
    pred_df: pd.DataFrame


def period_hours(period: str) -> list[int]:
    """Return hours for `all`, `on_peak`, or `off_peak`."""
    if period == "all":
        return ALL_HOURS
    if period == "on_peak":
        return ONPEAK_HOURS
    if period == "off_peak":
        return OFFPEAK_HOURS
    raise ValueError(f"Unsupported period '{period}'")


def build_period_slice(
    actual_by_he: dict[int, float],
    point_by_he: dict[int, float],
    quantiles_by_he: dict[int, dict[float, float]],
    quantiles: list[float],
    hours: list[int],
) -> PeriodSlice | None:
    """Construct aligned arrays for metric evaluation."""
    valid_hours = [
        h for h in hours
        if h in actual_by_he and h in point_by_he
        and actual_by_he[h] is not None and point_by_he[h] is not None
    ]
    if not valid_hours:
        return None

    y_true = np.array([float(actual_by_he[h]) for h in valid_hours], dtype=float)
    point = np.array([float(point_by_he[h]) for h in valid_hours], dtype=float)
    pred_df = pd.DataFrame({"point_forecast": point})

    for q in sorted(quantiles):
        q_vals = []
        missing = False
        for h in valid_hours:
            val = quantiles_by_he.get(h, {}).get(q)
            if val is None:
                missing = True
                break
            q_vals.append(float(val))
        if not missing:
            pred_df[f"q_{q:.2f}"] = q_vals

    return PeriodSlice(
        hours=valid_hours,
        y_true=y_true,
        point_forecast=point,
        pred_df=pred_df,
    )


def evaluate_period_slice(
    period_slice: PeriodSlice,
    quantiles: list[float],
) -> dict:
    """Compute deterministic and probabilistic metrics for a period."""
    metrics = evaluate_forecast(
        y_true=period_slice.y_true,
        y_pred_df=period_slice.pred_df,
        quantiles=quantiles,
        y_naive=None,
    )
    errors = period_slice.point_forecast - period_slice.y_true
    metrics["bias"] = float(np.mean(errors))
    metrics["n_hours"] = int(len(period_slice.hours))
    return metrics
