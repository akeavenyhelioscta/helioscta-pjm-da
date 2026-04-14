"""Shared helpers for LightGBM quantile regression pipelines."""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.lightgbm_quantile.configs import HOURS, OFFPEAK_HOURS, ONPEAK_HOURS


def expected_value_from_quantiles(quantile_values: dict[float, float]) -> float | None:
    """Approximate E[Y] by trapezoidal integration of the quantile function.

    Given quantile predictions at levels τ₁ < τ₂ < ... < τₖ with values
    q₁, q₂, ..., qₖ, the conditional mean is:

        E[Y] ≈ ∫₀¹ Q(τ) dτ

    Tail handling:
      - Left  [0, τ₁]:   flat at q₁ (conservative; prices are left-bounded).
      - Right [τₖ, 1]:    linear extrapolation from the last two quantiles.
        This captures the fat right tail of electricity price distributions
        instead of capping at the highest observed quantile.

    Returns None if *quantile_values* is empty.
    """
    if not quantile_values:
        return None

    sorted_items = sorted(quantile_values.items())
    taus = [t for t, _ in sorted_items]
    vals = [v for _, v in sorted_items]

    # Left tail: [0, tau_0] — flat (prices bounded below)
    ev = taus[0] * vals[0]

    # Interior segments: trapezoidal rule
    for i in range(len(taus) - 1):
        width = taus[i + 1] - taus[i]
        ev += width * (vals[i] + vals[i + 1]) / 2

    # Right tail: [tau_k, 1] — linear extrapolation from last two quantiles
    tail_width = 1.0 - taus[-1]
    if len(taus) >= 2:
        slope = (vals[-1] - vals[-2]) / (taus[-1] - taus[-2])
        tail_endpoint = vals[-1] + slope * tail_width
        ev += tail_width * (vals[-1] + tail_endpoint) / 2
    else:
        ev += tail_width * vals[-1]

    return float(ev)


def build_X(
    ref_row: pd.DataFrame,
    feature_cols: list[str],
    feature_medians: dict[str, float] | None = None,
) -> np.ndarray:
    """Extract a (1, n_features) array from a single-row DataFrame.

    Missing values are replaced with the training-set median for that
    feature (if available), falling back to 0.0 only when no median is
    provided.
    """
    X = np.zeros((1, len(feature_cols)))
    for i, col in enumerate(feature_cols):
        if col in ref_row.columns:
            val = ref_row[col].iloc[0]
            if pd.notna(val):
                X[0, i] = val
            elif feature_medians and col in feature_medians:
                X[0, i] = feature_medians[col]
    return X


def enforce_monotonic_quantiles(
    forecasts: dict[int, dict[float, float]],
    quantiles: list[float],
) -> None:
    """Apply monotonic rearrangement so lower quantiles never exceed higher ones."""
    q_sorted = sorted(quantiles)
    for h in HOURS:
        if h not in forecasts:
            continue
        preds = [forecasts[h].get(q) for q in q_sorted]
        if any(v is None for v in preds):
            continue
        monotone = np.maximum.accumulate(np.array(preds, dtype=float))
        for i, q in enumerate(q_sorted):
            forecasts[h][q] = float(monotone[i])


def period_avg(row: dict, hours: list[int]) -> float | None:
    """Compute the average of HE columns for given hours."""
    vals = [row.get(f"HE{h}") for h in hours]
    vals = [v for v in vals if v is not None and not (isinstance(v, float) and np.isnan(v))]
    return round(float(np.mean(vals)), 2) if vals else None


def add_summary(row: dict) -> dict:
    """Add OnPeak, OffPeak, and Flat summary columns to a row dict."""
    row["OnPeak"] = period_avg(row, ONPEAK_HOURS)
    row["OffPeak"] = period_avg(row, OFFPEAK_HOURS)
    row["Flat"] = period_avg(row, HOURS)
    return row
