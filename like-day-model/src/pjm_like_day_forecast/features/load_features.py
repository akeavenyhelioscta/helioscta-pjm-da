"""Daily load features for similarity matching.

DA load forecast is the primary demand signal (available 2020+).
RT metered load provides features back to 2014.
"""
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def build(
    df_da_load: pd.DataFrame | None = None,
    df_rt_load: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build daily load feature vectors for similarity matching.

    Args:
        df_da_load: DA load hourly [date, hour_ending, da_load_mw]. Available 2020+.
        df_rt_load: RT metered load hourly [date, hour_ending, rt_load_mw]. Available 2014+.

    Returns:
        DataFrame with one row per date, load similarity features.
    """
    # Use DA load if available, else fall back to RT metered load
    if df_da_load is not None and len(df_da_load) > 0:
        load_df = df_da_load[["date", "hour_ending", "da_load_mw"]].copy()
        load_col = "da_load_mw"
    elif df_rt_load is not None and len(df_rt_load) > 0:
        load_df = df_rt_load[["date", "hour_ending", "rt_load_mw"]].copy()
        load_col = "rt_load_mw"
    else:
        logger.warning("No load data provided")
        return pd.DataFrame(columns=["date"])

    load_df = load_df.sort_values(["date", "hour_ending"]).reset_index(drop=True)

    # Daily aggregates
    daily = load_df.groupby("date").agg(
        load_daily_avg=(load_col, "mean"),
        load_daily_peak=(load_col, "max"),
        load_daily_valley=(load_col, "min"),
    )

    # Shape indicator: peak / average
    daily["load_peak_ratio"] = daily["load_daily_peak"] / daily["load_daily_avg"].replace(0, np.nan)

    # Maximum hour-over-hour ramp within the day
    load_df["ramp"] = load_df.groupby("date")[load_col].diff()
    daily["load_ramp_max"] = load_df.groupby("date")["ramp"].max()

    # Rolling statistics
    daily["load_7d_rolling_mean"] = daily["load_daily_avg"].rolling(7, min_periods=1).mean()

    # Day-over-day change
    daily["load_daily_change"] = daily["load_daily_avg"].diff()

    result = daily.reset_index()

    n_features = len([c for c in result.columns if c != "date"])
    logger.info(f"Built {n_features} load similarity features")
    return result
