"""Target-date (D+1) load features for similarity matching.

Shifts DA load aggregates back by 1 day so they align with the reference date.
This lets the model match on "what tomorrow's demand is expected to look like,"
addressing systematic evening-peak bias from reference-only matching.

DA load for D+1 is published by 1:30 PM the day before — available for both
backtest and production.
"""
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def build(
    df_da_load: pd.DataFrame | None = None,
    df_ref_load_features: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build target-date (D+1) load features, shifted back to the reference date.

    Args:
        df_da_load: DA load hourly [date, hour_ending, da_load_mw]. Available 2020+.
        df_ref_load_features: Reference-date load features (for cross-day delta).
            Must have columns [date, load_daily_avg].

    Returns:
        DataFrame with one row per date, target-date load features.
        The date column represents the reference date (D), features describe D+1.
    """
    if df_da_load is None or len(df_da_load) == 0:
        logger.warning("No DA load data provided for target load features")
        return pd.DataFrame(columns=["date"])

    load_df = df_da_load[["date", "hour_ending", "da_load_mw"]].copy()
    load_df = load_df.sort_values(["date", "hour_ending"]).reset_index(drop=True)

    # Daily aggregates on D+1
    daily = load_df.groupby("date").agg(
        tgt_load_daily_avg=("da_load_mw", "mean"),
        tgt_load_daily_peak=("da_load_mw", "max"),
        tgt_load_daily_valley=("da_load_mw", "min"),
    )

    # Shape indicator: peak / average
    daily["tgt_load_peak_ratio"] = (
        daily["tgt_load_daily_peak"] / daily["tgt_load_daily_avg"].replace(0, np.nan)
    )

    # Maximum hour-over-hour ramp within the day
    load_df["ramp"] = load_df.groupby("date")["da_load_mw"].diff()
    daily["tgt_load_ramp_max"] = load_df.groupby("date")["ramp"].max()

    daily = daily.reset_index()

    # Shift date back by 1 day: D+1 features → assigned to reference date D
    daily["date"] = daily["date"] - pd.Timedelta(days=1)

    # Cross-day delta: D+1 avg load - D avg load (demand transition signal)
    if df_ref_load_features is not None and "load_daily_avg" in df_ref_load_features.columns:
        ref = df_ref_load_features[["date", "load_daily_avg"]].copy()
        daily = daily.merge(ref, on="date", how="left")
        daily["tgt_load_change_vs_ref"] = daily["tgt_load_daily_avg"] - daily["load_daily_avg"]
        daily = daily.drop(columns=["load_daily_avg"])

    n_features = len([c for c in daily.columns if c != "date"])
    logger.info(f"Built {n_features} target-load similarity features")
    return daily
