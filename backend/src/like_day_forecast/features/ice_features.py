"""ICE forward price features for analog selection.

Builds daily features from ICE PJM power settlement data.  These features
capture the market's forward-looking price consensus for the delivery day,
providing a signal that competes with backward-looking reference-day LMP.

Feature mapping:
  trade_date  →  feature matrix ``date`` column (the reference date)
  ICE NxtDay DA settle on that trade_date  →  market forward price for D+1

So the ICE features on row date=2026-04-13 represent the market's forward
view of the April 14 delivery day — exactly the target date for that row.
"""
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def build(df_settles: pd.DataFrame | None) -> pd.DataFrame:
    """Build daily ICE forward price features from settlement data.

    Args:
        df_settles: Output of ``ice_power_intraday.pull_settles()``.
            Expected columns: trade_date, symbol, product, settle,
            prior_settle, settle_vs_prior, vwap, high, low, volume.

    Returns:
        DataFrame with one row per trade_date where NxtDay DA data exists.
        Columns: date, ice_da_onpeak_settle, ice_da_onpeak_vwap,
        ice_da_onpeak_vs_prior, ice_da_onpeak_high, ice_da_onpeak_low,
        ice_da_onpeak_range, ice_da_onpeak_volume.
    """
    if df_settles is None or len(df_settles) == 0:
        logger.warning("No ICE settlement data available for ICE features")
        return pd.DataFrame(columns=["date"])

    # Filter to NxtDay DA product (PDA D1-IUS)
    df_da = df_settles[df_settles["product"] == "NxtDay DA"].copy()
    if len(df_da) == 0:
        logger.warning("No NxtDay DA settlements found in ICE data")
        return pd.DataFrame(columns=["date"])

    df_da["trade_date"] = pd.to_datetime(df_da["trade_date"]).dt.date

    # One row per trade_date — settle, vwap, change, range
    result = (
        df_da.groupby("trade_date")
        .agg(
            ice_da_onpeak_settle=("settle", "first"),
            ice_da_onpeak_vwap=("vwap", "first"),
            ice_da_onpeak_vs_prior=("settle_vs_prior", "first"),
            ice_da_onpeak_high=("high", "first"),
            ice_da_onpeak_low=("low", "first"),
            ice_da_onpeak_volume=("volume", "first"),
        )
        .reset_index()
        .rename(columns={"trade_date": "date"})
    )

    # Derived: intraday price range
    high = pd.to_numeric(result["ice_da_onpeak_high"], errors="coerce")
    low = pd.to_numeric(result["ice_da_onpeak_low"], errors="coerce")
    result["ice_da_onpeak_range"] = high - low

    logger.info(
        "Built ICE forward features: %d days (%s to %s)",
        len(result),
        result["date"].min(),
        result["date"].max(),
    )
    return result
