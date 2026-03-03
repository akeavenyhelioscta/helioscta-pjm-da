"""Training pipeline.

Pulls data → builds features → splits train/test → trains LightGBM quantile models
with multi-window calibration averaging → evaluates → saves artifacts.
"""
import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd

from src.pjm_da_forecast import configs
from src.pjm_da_forecast.features.builder import build_features
from src.pjm_da_forecast.features.preprocessing import asinh_inverse
from src.pjm_da_forecast.models.lightgbm_quantile import LightGBMQuantile
from src.pjm_da_forecast.models.registry import save_model
from src.pjm_da_forecast.evaluation.metrics import evaluate_forecast

logger = logging.getLogger(__name__)

# Columns to exclude from feature matrix (identifiers + target)
NON_FEATURE_COLS = {"date", "hour_ending", "lmp_total_target"}


def _split_train_test(
    df: pd.DataFrame,
    test_start: date,
    test_end: date | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split feature matrix into train and test sets by date."""
    if test_end is None:
        test_end = df["date"].max()

    train = df[df["date"] < test_start].copy()
    test = df[(df["date"] >= test_start) & (df["date"] <= test_end)].copy()

    logger.info(f"Train: {len(train):,} rows ({train['date'].min()} to {train['date'].max()})")
    logger.info(f"Test:  {len(test):,} rows ({test['date'].min()} to {test['date'].max()})")
    return train, test


def _get_feature_cols(df: pd.DataFrame) -> list[str]:
    """Get feature column names (everything except identifiers and target)."""
    return [c for c in df.columns if c not in NON_FEATURE_COLS]


def _build_naive_forecast(df_test: pd.DataFrame, df_full: pd.DataFrame) -> np.ndarray:
    """Build weekly naive forecast: same hour 7 days ago."""
    naive = []
    for _, row in df_test[["date", "hour_ending"]].iterrows():
        naive_date = row["date"] - timedelta(days=7)
        match = df_full[
            (df_full["date"] == naive_date) &
            (df_full["hour_ending"] == row["hour_ending"])
        ]
        if len(match) > 0:
            naive.append(match.iloc[0]["lmp_total_target"])
        else:
            naive.append(np.nan)
    return np.array(naive)


def train_single_window(
    df_train: pd.DataFrame,
    df_test: pd.DataFrame,
    window_days: int | None = None,
) -> tuple[LightGBMQuantile, pd.DataFrame]:
    """Train a single LightGBM quantile model on a given training window.

    Args:
        df_train: Training data (full history available).
        df_test: Test data to predict on.
        window_days: If set, use only the last N days of training data.

    Returns:
        (trained_model, predictions_df)
    """
    if window_days is not None:
        cutoff = df_train["date"].max() - timedelta(days=window_days)
        df_train = df_train[df_train["date"] >= cutoff].copy()
        logger.info(f"  Window: last {window_days} days ({len(df_train):,} training rows)")

    feature_cols = _get_feature_cols(df_train)

    X_train = df_train[feature_cols].astype(float)
    y_train = df_train["lmp_total_target"].astype(float)
    X_test = df_test[feature_cols].astype(float)

    model = LightGBMQuantile()
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    return model, predictions


def train_multi_window(
    df_train: pd.DataFrame,
    df_test: pd.DataFrame,
    windows: dict[str, int] | None = None,
) -> tuple[LightGBMQuantile, pd.DataFrame]:
    """Train models on multiple calibration windows and average predictions (Lago 2021).

    Args:
        df_train: Full training data.
        df_test: Test data.
        windows: Dict of window_name -> window_days. Defaults to CALIBRATION_WINDOWS.

    Returns:
        (last_trained_model, averaged_predictions_df)
    """
    if windows is None:
        windows = configs.CALIBRATION_WINDOWS

    logger.info(f"Multi-window training with {len(windows)} windows: {list(windows.keys())}")

    all_predictions = []
    last_model = None

    for window_name, window_days in windows.items():
        logger.info(f"Training window '{window_name}' ({window_days} days)...")
        model, preds = train_single_window(df_train, df_test, window_days=window_days)
        all_predictions.append(preds)
        last_model = model

    # Average predictions across all windows
    avg_predictions = sum(all_predictions) / len(all_predictions)

    # Re-sort quantiles after averaging to fix any crossing
    q_cols = [f"q_{q:.2f}" for q in configs.QUANTILES]
    available_q_cols = [c for c in q_cols if c in avg_predictions.columns]
    avg_predictions[available_q_cols] = np.sort(avg_predictions[available_q_cols].values, axis=1)

    logger.info(f"Averaged predictions across {len(windows)} windows")
    return last_model, avg_predictions


def run(
    mode: str = "full_feature",
    test_start: str = "2024-01-01",
    test_end: str | None = None,
    multi_window: bool = True,
) -> dict:
    """Run the full training pipeline.

    Args:
        mode: "full_feature" (2020+) or "extended" (2014+).
        test_start: Start date for test period (YYYY-MM-DD).
        test_end: End date for test period. Defaults to latest available date.
        multi_window: Whether to use multi-window calibration averaging.

    Returns:
        Dict with model, metrics, predictions, and feature importance.
    """
    logger.info("=" * 60)
    logger.info("Starting training pipeline")
    logger.info("=" * 60)

    # 1. Build features
    df = build_features(mode=mode)

    # 2. Split train/test
    test_start_date = pd.to_datetime(test_start).date()
    test_end_date = pd.to_datetime(test_end).date() if test_end else None
    df_train, df_test = _split_train_test(df, test_start_date, test_end_date)

    if len(df_test) == 0:
        raise ValueError(f"No test data available after {test_start}")

    # 3. Train model(s)
    if multi_window:
        model, predictions_asinh = train_multi_window(df_train, df_test)
    else:
        model, predictions_asinh = train_single_window(df_train, df_test)

    # 4. Inverse asinh transform predictions back to $/MWh
    q_cols = [c for c in predictions_asinh.columns if c.startswith("q_")]
    predictions = predictions_asinh.copy()
    for col in q_cols:
        predictions[col] = asinh_inverse(predictions_asinh[col])
    if "point_forecast" in predictions.columns:
        predictions["point_forecast"] = asinh_inverse(predictions_asinh["point_forecast"])

    # Inverse transform actuals
    y_true_asinh = df_test["lmp_total_target"].values
    y_true = asinh_inverse(y_true_asinh)

    # 5. Build naive forecast for rMAE
    y_naive_asinh = _build_naive_forecast(df_test, df)
    y_naive = asinh_inverse(y_naive_asinh)

    # 6. Evaluate
    metrics = evaluate_forecast(
        y_true=y_true,
        y_pred_df=predictions,
        quantiles=configs.QUANTILES,
        y_naive=y_naive,
    )

    logger.info("Evaluation metrics:")
    for k, v in metrics.items():
        if not k.startswith("pinball_"):
            logger.info(f"  {k}: {v:.4f}" if isinstance(v, float) else f"  {k}: {v}")

    # 7. Feature importance
    importance = model.get_feature_importance()

    # 8. Save model
    model_path = save_model(model=model, metrics=metrics)

    # 9. Save predictions
    pred_output = df_test[["date", "hour_ending"]].copy().reset_index(drop=True)
    pred_output = pd.concat([pred_output, predictions.reset_index(drop=True)], axis=1)

    logger.info("=" * 60)
    logger.info("Training pipeline complete")
    logger.info("=" * 60)

    return {
        "model": model,
        "model_path": str(model_path),
        "metrics": metrics,
        "predictions": pred_output,
        "feature_importance": importance,
        "test_start": test_start,
        "test_end": str(df_test["date"].max()),
        "n_train": len(df_train),
        "n_test": len(df_test),
    }
