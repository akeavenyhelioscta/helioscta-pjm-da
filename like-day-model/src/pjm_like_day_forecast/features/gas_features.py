"""Gas price features for similarity matching.

M3 (TETCO) is the benchmark gas hub for PJM Western Hub power pricing.
Gas is the marginal fuel ~50% of hours, making it the #1 exogenous feature.
"""
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def build(df_gas: pd.DataFrame) -> pd.DataFrame:
    """Build daily gas price features for similarity matching.

    Args:
        df_gas: Gas prices with columns [date, gas_m3_price, gas_hh_price, gas_transco_z6_price].

    Returns:
        DataFrame with one row per date, gas similarity features.
    """
    df = df_gas.copy()
    df = df.sort_values("date").reset_index(drop=True)
    df = df.set_index("date")

    result = pd.DataFrame(index=df.index)

    # Raw prices
    if "gas_m3_price" in df.columns:
        result["gas_m3_price"] = df["gas_m3_price"]
    if "gas_hh_price" in df.columns:
        result["gas_hh_price"] = df["gas_hh_price"]

    # M3-HH basis spread (regional premium)
    if "gas_m3_price" in df.columns and "gas_hh_price" in df.columns:
        result["gas_m3_hh_spread"] = df["gas_m3_price"] - df["gas_hh_price"]

    # Momentum and trend
    if "gas_m3_price" in df.columns:
        m3 = df["gas_m3_price"]
        result["gas_m3_7d_change"] = m3 - m3.shift(7)
        result["gas_m3_30d_mean"] = m3.rolling(30, min_periods=1).mean()

    result = result.reset_index()

    n_features = len([c for c in result.columns if c != "date"])
    logger.info(f"Built {n_features} gas similarity features")
    return result
