"""Metric helper tests for backtesting module."""
from __future__ import annotations

import numpy as np

from src.backtesting.metrics import build_period_slice, evaluate_period_slice


def test_period_metrics_perfect_forecast_has_zero_point_error() -> None:
    actual = {h: float(50 + h) for h in range(1, 25)}
    point = {h: float(50 + h) for h in range(1, 25)}
    quantiles = {
        h: {
            0.10: float(48 + h),
            0.25: float(49 + h),
            0.50: float(50 + h),
            0.75: float(51 + h),
            0.90: float(52 + h),
        }
        for h in range(1, 25)
    }
    p_slice = build_period_slice(
        actual_by_he=actual,
        point_by_he=point,
        quantiles_by_he=quantiles,
        quantiles=[0.10, 0.25, 0.50, 0.75, 0.90],
        hours=list(range(1, 25)),
    )
    assert p_slice is not None
    metrics = evaluate_period_slice(p_slice, quantiles=[0.10, 0.25, 0.50, 0.75, 0.90])
    assert np.isclose(metrics["mae"], 0.0)
    assert np.isclose(metrics["rmse"], 0.0)
    assert np.isclose(metrics["bias"], 0.0)
