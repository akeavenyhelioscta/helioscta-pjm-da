"""Daily weather features for similarity matching.

Uses the PJM population-weighted aggregate station from WSI observed + forecast temps.
Produces HDD/CDD, temperature level, and trend features that capture
heating/cooling demand drivers missing from LMP-only matching.
"""
import pandas as pd
import numpy as np
import logging

from src.like_day_forecast import configs

logger = logging.getLogger(__name__)


def build(
    df_weather: pd.DataFrame,
    hdd_base: float = configs.HDD_BASE_TEMP,
    cdd_base: float = configs.CDD_BASE_TEMP,
) -> pd.DataFrame:
    """Build daily weather feature vectors for similarity matching.

    Args:
        df_weather: Hourly weather data [date, hour_ending, temp].
        hdd_base: Base temperature for heating degree days (deg F).
        cdd_base: Base temperature for cooling degree days (deg F).

    Returns:
        DataFrame with one row per date, weather similarity features.
    """
    if df_weather is None or len(df_weather) == 0:
        logger.warning("No weather data provided")
        return pd.DataFrame(columns=["date"])

    df = df_weather.sort_values(["date", "hour_ending"]).reset_index(drop=True)

    # --- Daily temperature aggregates ---
    agg_dict = {
        "temp_daily_avg": ("temp", "mean"),
        "temp_daily_max": ("temp", "max"),
        "temp_daily_min": ("temp", "min"),
    }

    # Optional columns — include only if present
    for col, agg_name in [
        ("feels_like_temp", "feels_like_daily_avg"),
        ("dew_point_temp", "dew_point_daily_avg"),
        ("wind_speed_mph", "wind_speed_daily_avg"),
        ("relative_humidity", "humidity_daily_avg"),
        ("cloud_cover_pct", "cloud_cover_daily_avg"),
    ]:
        if col in df.columns:
            agg_dict[agg_name] = (col, "mean")

    daily = df.groupby("date").agg(**agg_dict)

    # Intraday temperature range
    daily["temp_intraday_range"] = daily["temp_daily_max"] - daily["temp_daily_min"]

    # HDD / CDD
    daily["hdd"] = np.maximum(0, hdd_base - daily["temp_daily_avg"])
    daily["cdd"] = np.maximum(0, daily["temp_daily_avg"] - cdd_base)

    # Rolling statistics
    daily["temp_7d_rolling_mean"] = daily["temp_daily_avg"].rolling(7, min_periods=1).mean()

    # Day-over-day temperature change
    daily["temp_daily_change"] = daily["temp_daily_avg"].diff()

    result = daily.reset_index()

    n_features = len([c for c in result.columns if c != "date"])
    logger.info(f"Built {n_features} weather similarity features")
    return result
