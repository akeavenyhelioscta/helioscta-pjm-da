"""Train and persist 24 x Q LightGBM quantile regression models."""
from __future__ import annotations

import logging
from datetime import date, timedelta

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd

from src.lightgbm_quantile.configs import HOURS, LGBMQRConfig
from src.lightgbm_quantile.features.builder import build_regression_features

logger = logging.getLogger(__name__)


def _artifact_filename(reference_date: date, day_type_tag: str | None) -> str:
    if day_type_tag:
        return f"lgbm_qr_{reference_date.isoformat()}_{day_type_tag}.joblib"
    return f"lgbm_qr_{reference_date.isoformat()}.joblib"


def train_models(
    config: LGBMQRConfig,
    reference_date: date | None = None,
    df_features: pd.DataFrame | None = None,
) -> dict:
    """Train all 24 x Q models and save to disk."""
    if reference_date is None:
        reference_date = date.today() - timedelta(days=1)

    df, feature_cols = build_regression_features(config, df_features)

    train_start = reference_date - timedelta(days=config.train_window_days)
    mask = (df["date"] >= train_start) & (df["date"] <= reference_date)
    df_train = df[mask].copy()

    target_cols = [f"target_HE{h}" for h in HOURS]

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
        "Training on %s samples (%s to %s), %s features",
        n_samples,
        train_start,
        reference_date,
        len(feature_cols),
    )

    X_train = df_train[feature_cols].values

    # Feature medians for NaN imputation at inference time
    feature_medians = {
        col: float(np.nanmedian(df_train[col].values))
        for col in feature_cols
    }

    # Temporal sample weights (spike weights computed per-hour in loop)
    train_dates = df_train["date"]
    days_ago = np.array([
        (reference_date - (d.date() if hasattr(d, "date") else d)).days
        for d in train_dates
    ])
    temporal_weights = np.exp(-np.log(2) * days_ago / 180.0)

    # Early stopping: hold out last 90 days as validation
    val_cutoff = reference_date - timedelta(days=90)
    val_mask = (df_train["date"] > val_cutoff).values
    use_early_stopping = val_mask.sum() >= 30
    if use_early_stopping:
        train_mask = ~val_mask
        X_tr, X_val = X_train[train_mask], X_train[val_mask]
        temporal_w_tr = temporal_weights[train_mask]
        logger.info(
            "Early stopping: %s train / %s val samples",
            train_mask.sum(),
            val_mask.sum(),
        )

    best_params = {
        "n_estimators": config.n_estimators,
        "max_depth": config.max_depth,
        "learning_rate": config.learning_rate,
    }
    if config.hyperparam_search:
        best_params = _select_hyperparams_cv(df_train, feature_cols, config)
        logger.info("Selected hyperparams via time-series CV: %s", best_params)

    models: dict[tuple[int, float], lgb.LGBMRegressor] = {}
    for h in HOURS:
        y_raw = df_train[f"target_HE{h}"].values
        y_transformed = np.arcsinh(y_raw)

        # Per-hour spike weights (3x for top decile of raw prices)
        p90 = np.percentile(y_raw, 90)
        spike_w = np.where(y_raw > p90, 3.0, 1.0)
        all_weights = temporal_weights * spike_w

        for q in config.quantiles:
            n_est = best_params["n_estimators"]
            if use_early_stopping:
                n_est = max(n_est, 1500)

            model = lgb.LGBMRegressor(
                objective="quantile",
                alpha=q,
                n_estimators=n_est,
                max_depth=best_params["max_depth"],
                learning_rate=best_params["learning_rate"],
                min_child_samples=config.min_child_samples,
                subsample=config.subsample,
                colsample_bytree=config.colsample_bytree,
                verbosity=-1,
            )

            if use_early_stopping:
                y_tr = y_transformed[train_mask]
                y_val = y_transformed[val_mask]
                w_tr = all_weights[train_mask]
                model.fit(
                    X_tr,
                    y_tr,
                    sample_weight=w_tr,
                    eval_set=[(X_val, y_val)],
                    callbacks=[
                        lgb.early_stopping(50, verbose=False),
                        lgb.log_evaluation(0),
                    ],
                )
            else:
                model.fit(X_train, y_transformed, sample_weight=all_weights)

            models[(h, q)] = model
        logger.info("  HE%s: trained %s quantile models", h, len(config.quantiles))

    artifact = {
        "models": models,
        "feature_columns": feature_cols,
        "train_start": train_start,
        "train_end": reference_date,
        "day_type_tag": config.day_type_tag,
        "n_samples": n_samples,
        "best_params": best_params,
        "hyperparam_search": config.hyperparam_search,
        "trained_at": pd.Timestamp.now().isoformat(),
        "config_name": config.name,
        "vst": "arcsinh",
        "feature_medians": feature_medians,
    }
    config.model_dir.mkdir(parents=True, exist_ok=True)
    model_path = config.model_dir / _artifact_filename(reference_date, config.day_type_tag)
    joblib.dump(artifact, model_path)
    logger.info("Saved model artifact to %s", model_path)

    return artifact


def load_latest_model(config: LGBMQRConfig) -> dict | None:
    """Load the most recent model artifact from disk."""
    if not config.model_dir.exists():
        return None

    if config.day_type_tag:
        pattern = f"lgbm_qr_*_{config.day_type_tag}.joblib"
    else:
        pattern = "lgbm_qr_*.joblib"
    model_files = sorted(config.model_dir.glob(pattern), reverse=True)
    if not model_files:
        return None

    artifact = joblib.load(model_files[0])

    trained_at = pd.Timestamp(artifact["trained_at"])
    age_hours = (pd.Timestamp.now() - trained_at).total_seconds() / 3600
    if age_hours > config.retrain_if_stale_hours:
        logger.info(
            "Model is %.1fh old (limit %sh), needs retrain",
            age_hours,
            config.retrain_if_stale_hours,
        )
        return None

    logger.info("Loaded model from %s (age %.1fh)", model_files[0].name, age_hours)
    return artifact


def _select_hyperparams_cv(
    df_train: pd.DataFrame,
    feature_cols: list[str],
    config: LGBMQRConfig,
    n_folds: int = 3,
) -> dict[str, int | float]:
    """Time-series cross-validation for LightGBM hyperparameter selection."""
    n = len(df_train)
    fold_size = n // (n_folds + 1)
    representative_hours = [8, 12, 17, 20]

    depth_grid = [4, 6, 8]
    n_estimators_grid = [300, 500, 800]
    learning_rate_grid = [0.03, 0.05, 0.1]

    best_params: dict[str, int | float] = {
        "max_depth": depth_grid[0],
        "n_estimators": n_estimators_grid[0],
        "learning_rate": learning_rate_grid[0],
    }
    best_loss = float("inf")

    for max_depth in depth_grid:
        for n_estimators in n_estimators_grid:
            for learning_rate in learning_rate_grid:
                total_loss = 0.0
                n_evals = 0

                for fold in range(n_folds):
                    train_end = (fold + 1) * fold_size
                    test_start = train_end
                    test_end = min(test_start + fold_size, n)
                    if test_end <= test_start:
                        continue

                    X_tr = df_train.iloc[:train_end][feature_cols].values
                    X_te = df_train.iloc[test_start:test_end][feature_cols].values

                    for h in representative_hours:
                        y_col = f"target_HE{h}"
                        y_tr = df_train.iloc[:train_end][y_col].values
                        y_te = df_train.iloc[test_start:test_end][y_col].values

                        for q in config.quantiles:
                            model = lgb.LGBMRegressor(
                                objective="quantile",
                                alpha=q,
                                n_estimators=n_estimators,
                                max_depth=max_depth,
                                learning_rate=learning_rate,
                                min_child_samples=config.min_child_samples,
                                subsample=config.subsample,
                                colsample_bytree=config.colsample_bytree,
                                verbosity=-1,
                            )
                            model.fit(X_tr, np.arcsinh(y_tr))
                            y_pred = np.sinh(model.predict(X_te))

                            errors = y_te - y_pred
                            pinball = np.where(
                                errors >= 0,
                                q * errors,
                                (q - 1) * errors,
                            )
                            total_loss += pinball.mean()
                            n_evals += 1

                avg_loss = total_loss / max(n_evals, 1)
                logger.info(
                    "  max_depth=%s n_estimators=%s learning_rate=%s: avg pinball=%.4f",
                    max_depth,
                    n_estimators,
                    learning_rate,
                    avg_loss,
                )
                if avg_loss < best_loss:
                    best_loss = avg_loss
                    best_params = {
                        "max_depth": max_depth,
                        "n_estimators": n_estimators,
                        "learning_rate": learning_rate,
                    }

    return best_params
