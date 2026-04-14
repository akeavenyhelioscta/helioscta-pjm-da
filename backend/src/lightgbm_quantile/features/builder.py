"""LightGBM QR feature builder.

Reuses the exact regression feature matrix from LASSO QR.
"""
from src.lasso_quantile_regression.features.builder import build_regression_features

__all__ = ["build_regression_features"]
