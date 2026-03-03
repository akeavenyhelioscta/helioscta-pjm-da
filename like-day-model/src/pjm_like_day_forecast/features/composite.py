"""Cross-domain composite features for similarity matching.

Captures relationships between LMP, gas, and load that are important
for finding truly similar market conditions.
"""
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def build(
    df_lmp_features: pd.DataFrame,
    df_gas_features: pd.DataFrame,
    df_load_features: pd.DataFrame,
) -> pd.DataFrame:
    """Build composite cross-domain features.

    Args:
        df_lmp_features: Daily LMP features (must have lmp_daily_flat).
        df_gas_features: Daily gas features (must have gas_m3_price).
        df_load_features: Daily load features (must have load_daily_avg).

    Returns:
        DataFrame with one row per date, composite features.
    """
    result = df_lmp_features[["date"]].copy()

    # Merge in gas and load on date
    merged = result.merge(
        df_lmp_features[["date", "lmp_daily_flat"]], on="date", how="left"
    ).merge(
        df_gas_features[["date", "gas_m3_price"]], on="date", how="left"
    )

    if "load_daily_avg" in df_load_features.columns:
        merged = merged.merge(
            df_load_features[["date", "load_daily_avg"]], on="date", how="left"
        )

    # Implied heat rate: LMP / gas price (marginal generation efficiency)
    # Use raw values — asinh(LMP) / gas doesn't have physical meaning
    if "gas_m3_price" in merged.columns:
        result["implied_heat_rate"] = (
            np.sinh(merged["lmp_daily_flat"]) /
            merged["gas_m3_price"].replace(0, np.nan)
        )

    # Price intensity: LMP / load ($/MWh per MW of demand)
    if "load_daily_avg" in merged.columns:
        result["lmp_per_load"] = (
            np.sinh(merged["lmp_daily_flat"]) /
            merged["load_daily_avg"].replace(0, np.nan)
        )

    n_features = len([c for c in result.columns if c != "date"])
    logger.info(f"Built {n_features} composite similarity features")
    return result
