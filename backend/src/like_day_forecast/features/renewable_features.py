"""Daily solar and wind generation features for similarity matching.

Uses actual generation from gridstatus.pjm_fuel_mix_hourly (2020+) to build
reference-date renewable supply features. These capture how much renewable
generation was displacing thermal units on a given day.
"""
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def build(
    df_fuel_mix: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build daily renewable generation features from fuel mix actuals.

    Args:
        df_fuel_mix: Hourly fuel mix [date, hour_ending, solar, wind, ...].

    Returns:
        DataFrame with one row per date, renewable similarity features.
    """
    if df_fuel_mix is None or len(df_fuel_mix) == 0:
        logger.warning("No fuel mix data provided")
        return pd.DataFrame(columns=["date"])

    df = df_fuel_mix[["date", "hour_ending", "solar", "wind"]].copy()
    df = df.sort_values(["date", "hour_ending"]).reset_index(drop=True)

    # Fill missing values (nighttime solar is often 0, not NaN)
    df["solar"] = df["solar"].fillna(0)
    df["wind"] = df["wind"].fillna(0)
    df["renewable_total"] = df["solar"] + df["wind"]

    # --- Daily aggregates ---
    daily = df.groupby("date").agg(
        solar_daily_avg=("solar", "mean"),
        solar_daily_max=("solar", "max"),
        wind_daily_avg=("wind", "mean"),
        wind_daily_max=("wind", "max"),
        renewable_daily_avg=("renewable_total", "mean"),
        renewable_daily_max=("renewable_total", "max"),
    )

    # Solar shape: fraction of daily generation in peak hours (HE 10-16)
    peak_mask = df["hour_ending"].between(10, 16)
    solar_peak = df[peak_mask].groupby("date")["solar"].sum()
    solar_total = df.groupby("date")["solar"].sum().replace(0, np.nan)
    daily["solar_peak_concentration"] = solar_peak / solar_total

    # Wind variability within the day
    daily["wind_intraday_std"] = df.groupby("date")["wind"].std()

    # Rolling trends
    daily["renewable_7d_rolling_mean"] = (
        daily["renewable_daily_avg"].rolling(7, min_periods=1).mean()
    )

    # Day-over-day change
    daily["renewable_daily_change"] = daily["renewable_daily_avg"].diff()

    result = daily.reset_index()

    n_features = len([c for c in result.columns if c != "date"])
    logger.info(f"Built {n_features} renewable similarity features")
    return result
