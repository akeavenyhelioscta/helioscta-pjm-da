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
        df_gas_features: Gas features (must have gas_m3_daily_avg from hourly gas).
        df_load_features: Daily load features (must have load_daily_avg).

    Returns:
        DataFrame with one row per date, composite features.
    """
    result = df_lmp_features[["date"]].copy()

    # Find gas price column — prefer hourly-derived gas_m3_daily_avg
    gas_col = None
    for candidate in ["gas_m3_daily_avg", "gas_m3_price"]:
        if candidate in df_gas_features.columns:
            gas_col = candidate
            break

    # Merge in gas and load on date
    merged = result.merge(
        df_lmp_features[["date", "lmp_daily_flat"]], on="date", how="left"
    )
    if gas_col:
        merged = merged.merge(
            df_gas_features[["date", gas_col]], on="date", how="left"
        )

    if "load_daily_avg" in df_load_features.columns:
        merged = merged.merge(
            df_load_features[["date", "load_daily_avg"]], on="date", how="left"
        )

    # Implied heat rate: LMP / gas price (marginal generation efficiency)
    if gas_col and gas_col in merged.columns:
        result["implied_heat_rate"] = (
            merged["lmp_daily_flat"] /
            merged[gas_col].replace(0, np.nan)
        )

    # Price intensity: LMP / load ($/MWh per MW of demand)
    if "load_daily_avg" in merged.columns:
        result["lmp_per_load"] = (
            merged["lmp_daily_flat"] /
            merged["load_daily_avg"].replace(0, np.nan)
        )

    n_features = len([c for c in result.columns if c != "date"])
    logger.info(f"Built {n_features} composite similarity features")
    return result
