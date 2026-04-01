"""Daily fuel mix share features for similarity matching.

Fuel mix shares from gridstatus.pjm_fuel_mix_hourly (2020+). Captures the
generation dispatch stack composition — when gas share is high vs coal share
is high, marginal cost dynamics differ.
"""
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def build(
    df_fuel_mix: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build daily fuel mix share features from fuel mix actuals.

    Args:
        df_fuel_mix: Hourly fuel mix [date, hour_ending, gas, coal, nuclear,
                     solar, wind, hydro, ...].

    Returns:
        DataFrame with one row per date, fuel mix share features.
    """
    if df_fuel_mix is None or len(df_fuel_mix) == 0:
        logger.warning("No fuel mix data provided for fuel mix share features")
        return pd.DataFrame(columns=["date"])

    gen_cols = ["solar", "wind", "gas", "coal", "nuclear", "hydro",
                "oil", "storage", "other", "other_renewables", "multiple_fuels"]
    available_gen = [c for c in gen_cols if c in df_fuel_mix.columns]

    df = df_fuel_mix[["date", "hour_ending"] + available_gen].copy()
    df = df.sort_values(["date", "hour_ending"]).reset_index(drop=True)

    for col in available_gen:
        df[col] = df[col].fillna(0)

    df["total_gen"] = df[available_gen].sum(axis=1)

    renewable_cols = [c for c in ["solar", "wind", "hydro", "other_renewables"]
                      if c in df.columns]
    df["renewable_total"] = df[renewable_cols].sum(axis=1)

    daily_gas = df.groupby("date")["gas"].sum()
    daily_coal = df.groupby("date")["coal"].sum()
    daily_nuclear = df.groupby("date")["nuclear"].sum()
    daily_renewable = df.groupby("date")["renewable_total"].sum()
    daily_total = df.groupby("date")["total_gen"].sum().replace(0, np.nan)

    daily = pd.DataFrame(index=daily_total.index)
    daily["fuel_share_gas"] = daily_gas / daily_total
    daily["fuel_share_coal"] = daily_coal / daily_total
    daily["fuel_share_nuclear"] = daily_nuclear / daily_total
    daily["fuel_share_renewable"] = daily_renewable / daily_total

    result = daily.reset_index()

    n_features = len([c for c in result.columns if c != "date"])
    logger.info(f"Built {n_features} fuel mix share similarity features")
    return result
