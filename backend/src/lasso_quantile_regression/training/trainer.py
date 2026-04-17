"""Train and persist 24 x Q quantile regression models.

Each (hour, quantile) pair gets its own LASSO quantile regression model
wrapped in a Pipeline with StandardScaler for feature normalization.
Models are serialized to ``config.model_dir`` as joblib files.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import QuantileRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.lasso_quantile_regression.configs import HOURS, LassoQRConfig
from src.lasso_quantile_regression.features.builder import build_regression_features

logger = logging.getLogger(__name__)


def _artifact_filename(reference_date: date, day_type_tag: str | None) -> str:
    if day_type_tag:
        return f"lasso_qr_{reference_date.isoformat()}_{day_type_tag}.joblib"
    return f"lasso_qr_{reference_date.isoformat()}.joblib"


def train_models(
    config: LassoQRConfig,
    reference_date: date | None = None,
    df_features: pd.DataFrame | None = None,
) -> dict:
    """Train all 24 x Q models and save to disk.

    Args:
        config: Model configuration.
        reference_date: Last date to include in training (inclusive).
            Defaults to yesterday.
        df_features: Pre-built feature matrix. If ``None``, will build.

    Returns:
        Artifact dict with ``models``, ``feature_columns``, metadata.
    """
    if reference_date is None:
        reference_date = date.today() - timedelta(days=1)

    df, feature_cols = build_regression_features(config, df_features)

    # Filter to training window
    train_start = reference_date - timedelta(days=config.train_window_days)
    mask = (df["date"] >= train_start) & (df["date"] <= reference_date)
    df_train = df[mask].copy()

    target_cols = [f"target_HE{h}" for h in HOURS]

    # Drop features that are entirely missing in this training window.
    # This prevents full-row dropout when upstream sources are unavailable.
    active_feature_cols = [c for c in feature_cols if df_train[c].notna().any()]
    dropped_feature_cols = [c for c in feature_cols if c not in active_feature_cols]
    if dropped_feature_cols:
        logger.info(
            "Dropping %s all-NaN feature(s) for window ending %s",
            len(dropped_feature_cols),
            reference_date,
        )
    feature_cols = active_feature_cols

    df_train = df_train.dropna(subset=feature_cols + target_cols)

    n_samples = len(df_train)
    if n_samples < config.min_train_samples:
        raise ValueError(
            f"Only {n_samples} training samples (need {config.min_train_samples}). "
            f"Window: {train_start} to {reference_date}"
        )

    logger.info(
        f"Training on {n_samples} samples ({train_start} to {reference_date}), "
        f"{len(feature_cols)} features"
    )

    X_train = df_train[feature_cols].values

    # Select base alpha via time-series CV if enabled
    base_alpha = config.alpha
    if config.alpha_search:
        base_alpha = _select_alpha_cv(df_train, feature_cols, config)
        logger.info(f"Selected base alpha={base_alpha} via time-series CV")

    # Recency weights: exponential decay so recent samples dominate
    sample_weights = None
    if config.recency_gamma is not None:
        gamma = config.recency_gamma
        sample_weights = gamma ** np.arange(n_samples - 1, -1, -1, dtype=float)
        half_life = -np.log(2) / np.log(gamma) if gamma < 1.0 else float("inf")
        logger.info(f"Recency weighting: gamma={gamma}, half-life={half_life:.0f} days")

    # Train Pipeline(StandardScaler -> QuantileRegressor) per (hour, quantile)
    models: dict[tuple[int, float], Pipeline] = {}
    alpha_scales = config.quantile_alpha_scales
    for h in HOURS:
        y_raw = df_train[f"target_HE{h}"].values
        y_train = np.arcsinh(y_raw) if config.use_asinh_transform else y_raw
        for q in config.quantiles:
            q_alpha = base_alpha * alpha_scales.get(q, 1.0)
            pipe = Pipeline([
                ("scaler", StandardScaler()),
                ("qr", QuantileRegressor(
                    quantile=q,
                    alpha=q_alpha,
                    solver="highs",
                    fit_intercept=True,
                )),
            ])
            fit_params = {}
            if sample_weights is not None:
                fit_params["qr__sample_weight"] = sample_weights
            pipe.fit(X_train, y_train, **fit_params)
            models[(h, q)] = pipe
        logger.info(f"  HE{h}: trained {len(config.quantiles)} quantile models")

    # Serialize
    artifact = {
        "models": models,
        "feature_columns": feature_cols,
        "train_start": train_start,
        "train_end": reference_date,
        "day_type_tag": config.day_type_tag,
        "n_samples": n_samples,
        "alpha": base_alpha,
        "quantile_alpha_scales": dict(alpha_scales),
        "use_asinh_transform": config.use_asinh_transform,
        "recency_gamma": config.recency_gamma,
        "trained_at": pd.Timestamp.now().isoformat(),
        "config_name": config.name,
    }
    config.model_dir.mkdir(parents=True, exist_ok=True)
    model_path = config.model_dir / _artifact_filename(reference_date, config.day_type_tag)
    joblib.dump(artifact, model_path)
    logger.info(f"Saved model artifact to {model_path}")

    return artifact


def load_latest_model(config: LassoQRConfig) -> dict | None:
    """Load the most recent model artifact from disk.

    Returns ``None`` if no model is found or the latest model is stale
    (older than ``config.retrain_if_stale_hours``).
    """
    if not config.model_dir.exists():
        return None

    if config.day_type_tag:
        pattern = f"lasso_qr_*_{config.day_type_tag}.joblib"
    else:
        pattern = "lasso_qr_*.joblib"
    model_files = sorted(config.model_dir.glob(pattern), reverse=True)
    if not model_files:
        return None

    artifact = joblib.load(model_files[0])

    trained_at = pd.Timestamp(artifact["trained_at"])
    age_hours = (pd.Timestamp.now() - trained_at).total_seconds() / 3600
    if age_hours > config.retrain_if_stale_hours:
        logger.info(
            f"Model is {age_hours:.1f}h old (limit {config.retrain_if_stale_hours}h), "
            f"needs retrain"
        )
        return None

    logger.info(f"Loaded model from {model_files[0].name} (age {age_hours:.1f}h)")
    return artifact


# ── Alpha selection via time-series CV ─────────────────────────────


def _select_alpha_cv(
    df_train: pd.DataFrame,
    feature_cols: list[str],
    config: LassoQRConfig,
    n_folds: int = 3,
) -> float:
    """Time-series cross-validation for alpha selection.

    Uses expanding window with *n_folds* folds.  Metric: mean pinball loss
    across representative hours (HE8, HE12, HE17, HE20) and all quantiles.
    """
    n = len(df_train)
    n_features = len(feature_cols)

    # Ensure the first fold has at least 2x features as training samples
    # to avoid degenerate solutions that bias toward high alpha.
    min_train_size = min(2 * n_features, n // 2)
    fold_size = max(1, (n - min_train_size) // n_folds)
    representative_hours = [8, 12, 17, 20]

    best_alpha = config.alpha_grid[0]
    best_loss = float("inf")

    use_asinh = config.use_asinh_transform
    alpha_scales = config.quantile_alpha_scales

    for alpha in config.alpha_grid:
        total_loss = 0.0
        n_evals = 0

        for fold in range(n_folds):
            train_end = min_train_size + fold * fold_size
            test_start = train_end
            test_end = min(test_start + fold_size, n)
            if test_end <= test_start:
                continue

            X_tr = df_train.iloc[:train_end][feature_cols].values
            X_te = df_train.iloc[test_start:test_end][feature_cols].values

            # Recency weights for this fold's training portion
            fold_weights = None
            if config.recency_gamma is not None:
                n_tr = train_end
                fold_weights = config.recency_gamma ** np.arange(
                    n_tr - 1, -1, -1, dtype=float,
                )

            for h in representative_hours:
                y_col = f"target_HE{h}"
                y_tr_raw = df_train.iloc[:train_end][y_col].values
                y_te_raw = df_train.iloc[test_start:test_end][y_col].values
                y_tr = np.arcsinh(y_tr_raw) if use_asinh else y_tr_raw
                # Evaluate CV in the same space the model optimizes in.
                # Inverse-transforming via sinh() amplifies overfit predictions
                # and makes low-alpha models look catastrophically bad.
                y_te = np.arcsinh(y_te_raw) if use_asinh else y_te_raw

                for q in config.quantiles:
                    q_alpha = alpha * alpha_scales.get(q, 1.0)
                    pipe = Pipeline([
                        ("scaler", StandardScaler()),
                        ("qr", QuantileRegressor(
                            quantile=q, alpha=q_alpha, solver="highs",
                        )),
                    ])
                    fit_params = {}
                    if fold_weights is not None:
                        fit_params["qr__sample_weight"] = fold_weights
                    pipe.fit(X_tr, y_tr, **fit_params)
                    y_pred = pipe.predict(X_te)

                    errors = y_te - y_pred
                    pinball = np.where(
                        errors >= 0, q * errors, (q - 1) * errors
                    )
                    total_loss += pinball.mean()
                    n_evals += 1

        avg_loss = total_loss / max(n_evals, 1)
        logger.info(f"  alpha={alpha}: avg pinball={avg_loss:.4f}")
        if avg_loss < best_loss:
            best_loss = avg_loss
            best_alpha = alpha

    return best_alpha
