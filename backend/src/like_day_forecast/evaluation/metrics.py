"""Evaluation metrics for probabilistic forecasting.

Following Lago 2021 best practices + GEFCom2014 standard.
Identical to da-model for comparability.
"""
import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def pinball_loss(y_true: np.ndarray, y_pred: np.ndarray, q: float) -> float:
    """Pinball (quantile) loss — GEFCom2014 official metric."""
    delta = y_true - y_pred
    return np.mean(np.maximum(q * delta, (q - 1) * delta))


def mean_pinball_loss(y_true: np.ndarray, y_pred_df: pd.DataFrame, quantiles: list[float]) -> float:
    """Average pinball loss across all quantiles."""
    losses = []
    for q in quantiles:
        col = f"q_{q:.2f}"
        if col in y_pred_df.columns:
            losses.append(pinball_loss(y_true, y_pred_df[col].values, q))
    return np.mean(losses) if losses else np.nan


def rmae(y_true: np.ndarray, y_pred: np.ndarray, y_naive: np.ndarray) -> float:
    """Relative MAE: MAE(model) / MAE(naive). rMAE < 1 means model beats naive."""
    mae_model = np.mean(np.abs(y_true - y_pred))
    mae_naive = np.mean(np.abs(y_true - y_naive))
    if mae_naive == 0:
        return np.inf
    return mae_model / mae_naive


def mae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return np.mean(np.abs(y_true - y_pred))


def rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return np.sqrt(np.mean((y_true - y_pred) ** 2))


def mape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    mask = y_true != 0
    if not np.any(mask):
        return np.nan
    return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100


def coverage(y_true: np.ndarray, lower: np.ndarray, upper: np.ndarray) -> float:
    """Prediction interval coverage: % of actuals within [lower, upper]."""
    return np.mean((y_true >= lower) & (y_true <= upper))


def sharpness(lower: np.ndarray, upper: np.ndarray) -> float:
    """Average prediction interval width."""
    return np.mean(upper - lower)


def crps(y_true: np.ndarray, y_pred_df: pd.DataFrame, quantiles: list[float]) -> float:
    """CRPS approximated via trapezoidal integration of pinball losses."""
    losses = []
    for q in quantiles:
        col = f"q_{q:.2f}"
        if col in y_pred_df.columns:
            losses.append((q, pinball_loss(y_true, y_pred_df[col].values, q)))

    if len(losses) < 2:
        return np.nan

    losses.sort(key=lambda x: x[0])
    qs = [l[0] for l in losses]
    pls = [l[1] for l in losses]
    return np.trapz(pls, qs)


def evaluate_forecast(
    y_true: np.ndarray,
    y_pred_df: pd.DataFrame,
    quantiles: list[float],
    y_naive: np.ndarray | None = None,
) -> dict:
    """Compute all evaluation metrics for a quantile forecast."""
    results = {}

    # Point metrics on median or point forecast
    if "point_forecast" in y_pred_df.columns:
        y_point = y_pred_df["point_forecast"].values
    elif "q_0.50" in y_pred_df.columns:
        y_point = y_pred_df["q_0.50"].values
    else:
        y_point = None

    if y_point is not None:
        results["mae"] = mae(y_true, y_point)
        results["rmse"] = rmse(y_true, y_point)
        results["mape"] = mape(y_true, y_point)
        if y_naive is not None:
            results["rmae"] = rmae(y_true, y_point, y_naive)

    results["mean_pinball"] = mean_pinball_loss(y_true, y_pred_df, quantiles)
    results["crps"] = crps(y_true, y_pred_df, quantiles)

    # Per-quantile pinball
    for q in quantiles:
        col = f"q_{q:.2f}"
        if col in y_pred_df.columns:
            results[f"pinball_{q:.2f}"] = pinball_loss(y_true, y_pred_df[col].values, q)

    # Coverage and sharpness
    intervals = [
        ("80pct", 0.10, 0.90),
        ("90pct", 0.05, 0.95),
        ("98pct", 0.01, 0.99),
    ]
    for name, q_lo, q_hi in intervals:
        col_lo = f"q_{q_lo:.2f}"
        col_hi = f"q_{q_hi:.2f}"
        if col_lo in y_pred_df.columns and col_hi in y_pred_df.columns:
            results[f"coverage_{name}"] = coverage(y_true, y_pred_df[col_lo].values, y_pred_df[col_hi].values)
            results[f"sharpness_{name}"] = sharpness(y_pred_df[col_lo].values, y_pred_df[col_hi].values)

    return results
