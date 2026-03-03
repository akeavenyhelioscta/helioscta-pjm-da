"""LightGBM quantile regression model.

Trains one LGBMRegressor(objective='quantile', alpha=q) per quantile.
Supports multi-window calibration averaging (Lago 2021).
"""
import logging
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from lightgbm import LGBMRegressor
from sklearn.isotonic import IsotonicRegression

from src.pjm_da_forecast import configs
from src.pjm_da_forecast.models.base import QuantileForecaster

logger = logging.getLogger(__name__)


class LightGBMQuantile(QuantileForecaster):
    """LightGBM-based quantile regression forecaster."""

    def __init__(
        self,
        quantiles: list[float] = configs.QUANTILES,
        lgbm_params: dict | None = None,
        name: str = "lgbm_quantile",
    ):
        super().__init__(quantiles=quantiles, name=name)
        self.lgbm_params = lgbm_params or configs.LGBM_PARAMS.copy()
        self.models: dict[float, LGBMRegressor] = {}
        self.point_model: LGBMRegressor | None = None  # MSE model for QRA input
        self.feature_names: list[str] = []

    def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
        """Train one LightGBM model per quantile + one point forecast model.

        Args:
            X: Feature matrix (no date/hour columns).
            y: Target values (asinh-transformed LMP).
        """
        self.feature_names = list(X.columns)
        logger.info(f"Training {len(self.quantiles)} quantile models + 1 point model on {len(X):,} samples, {len(self.feature_names)} features")

        # Train quantile models
        for q in self.quantiles:
            model = LGBMRegressor(
                objective="quantile",
                alpha=q,
                **self.lgbm_params,
            )
            model.fit(X, y)
            self.models[q] = model
            logger.info(f"  Trained q={q:.2f}")

        # Train point forecast model (MSE objective, for QRA input later)
        self.point_model = LGBMRegressor(
            objective="regression",
            **self.lgbm_params,
        )
        self.point_model.fit(X, y)
        logger.info("  Trained point forecast model (MSE)")

    def predict(self, X: pd.DataFrame) -> pd.DataFrame:
        """Generate quantile predictions.

        Returns:
            DataFrame with columns q_0.01, q_0.05, ..., q_0.99, and point_forecast.
        """
        predictions = {}
        for q in self.quantiles:
            col_name = f"q_{q:.2f}"
            predictions[col_name] = self.models[q].predict(X)

        if self.point_model is not None:
            predictions["point_forecast"] = self.point_model.predict(X)

        df_pred = pd.DataFrame(predictions)

        # Fix quantile crossing via sorting (isotonic)
        q_cols = [f"q_{q:.2f}" for q in self.quantiles]
        df_pred[q_cols] = np.sort(df_pred[q_cols].values, axis=1)

        return df_pred

    def get_feature_importance(self) -> pd.DataFrame:
        """Get feature importance from the median (q=0.50) model."""
        median_model = self.models.get(0.50)
        if median_model is None:
            return pd.DataFrame()

        importance = median_model.feature_importances_
        return pd.DataFrame({
            "feature": self.feature_names,
            "importance": importance,
        }).sort_values("importance", ascending=False).reset_index(drop=True)

    def save(self, path: str) -> None:
        """Save all model artifacts."""
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)

        joblib.dump({
            "models": self.models,
            "point_model": self.point_model,
            "quantiles": self.quantiles,
            "lgbm_params": self.lgbm_params,
            "feature_names": self.feature_names,
            "name": self.name,
        }, path / "model.joblib")
        logger.info(f"Saved model to {path}")

    @classmethod
    def load(cls, path: str) -> "LightGBMQuantile":
        """Load model from disk."""
        path = Path(path)
        data = joblib.load(path / "model.joblib")

        instance = cls(
            quantiles=data["quantiles"],
            lgbm_params=data["lgbm_params"],
            name=data["name"],
        )
        instance.models = data["models"]
        instance.point_model = data["point_model"]
        instance.feature_names = data["feature_names"]
        logger.info(f"Loaded model from {path}")
        return instance
