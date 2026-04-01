"""Net load features for similarity matching.

Net load = gross load - renewable generation (solar + wind). This is the
thermal generation requirement — the demand that gas, coal, and nuclear
must serve. Net load is a better predictor of LMP than gross load because
it directly captures the supply-demand balance at the margin.
"""
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = list(range(1, 8)) + [24]


def build(
    df_rt_load: pd.DataFrame | None = None,
    df_fuel_mix: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build daily net load features from RT load and renewable generation.

    Args:
        df_rt_load: RT metered load [date, hour_ending, region, rt_load_mw].
        df_fuel_mix: Hourly fuel mix [date, hour_ending, solar, wind, ...].

    Returns:
        DataFrame with one row per date, net load features.
    """
    if df_rt_load is None or df_fuel_mix is None:
        logger.warning("Missing load or fuel mix data for net load features")
        return pd.DataFrame(columns=["date"])

    # RTO load
    load = df_rt_load[df_rt_load["region"] == "RTO"][
        ["date", "hour_ending", "rt_load_mw"]
    ].copy()

    # Renewable generation
    renew = df_fuel_mix[["date", "hour_ending"]].copy()
    renew["renewable_mw"] = (
        df_fuel_mix["solar"].fillna(0) + df_fuel_mix["wind"].fillna(0)
    )

    # Merge on date + hour
    merged = load.merge(renew, on=["date", "hour_ending"], how="inner")
    merged["net_load_mw"] = merged["rt_load_mw"] - merged["renewable_mw"]

    if len(merged) == 0:
        logger.warning("No overlapping load + fuel mix data for net load")
        return pd.DataFrame(columns=["date"])

    # Daily aggregates
    daily = merged.groupby("date").agg(
        net_load_daily_avg=("net_load_mw", "mean"),
        net_load_daily_peak=("net_load_mw", "max"),
        net_load_daily_valley=("net_load_mw", "min"),
    )

    # Evening net load ramp: HE20 - HE15 (thermal ramp requirement)
    for d, grp in merged.groupby("date"):
        he15 = grp.loc[grp["hour_ending"] == 15, "net_load_mw"]
        he20 = grp.loc[grp["hour_ending"] == 20, "net_load_mw"]
        if len(he15) > 0 and len(he20) > 0:
            daily.at[d, "net_load_evening_ramp"] = float(he20.iloc[0]) - float(he15.iloc[0])

    # Morning net load ramp: HE8 - HE5
    for d, grp in merged.groupby("date"):
        he5 = grp.loc[grp["hour_ending"] == 5, "net_load_mw"]
        he8 = grp.loc[grp["hour_ending"] == 8, "net_load_mw"]
        if len(he5) > 0 and len(he8) > 0:
            daily.at[d, "net_load_morning_ramp"] = float(he8.iloc[0]) - float(he5.iloc[0])

    # Rolling trend
    daily["net_load_7d_rolling_mean"] = (
        daily["net_load_daily_avg"].rolling(7, min_periods=1).mean()
    )

    result = daily.reset_index()
    n_features = len([c for c in result.columns if c != "date"])
    logger.info(f"Built {n_features} net load similarity features")
    return result
