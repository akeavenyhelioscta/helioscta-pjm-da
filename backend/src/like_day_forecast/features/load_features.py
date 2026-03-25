"""Daily load features for similarity matching.

RT metered load is the demand signal (available 2014+). DA load is excluded
because the model must run before DA market clearing (~1:30 PM).
"""
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def build(
    df_rt_load: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build daily load feature vectors for similarity matching.

    Args:
        df_rt_load: RT metered load hourly [date, hour_ending, rt_load_mw]. Available 2014+.

    Returns:
        DataFrame with one row per date, load similarity features.
    """
    if df_rt_load is None or len(df_rt_load) == 0:
        logger.warning("No load data provided")
        return pd.DataFrame(columns=["date"])

    load_df = df_rt_load[["date", "hour_ending", "rt_load_mw"]].copy()
    load_col = "rt_load_mw"

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
