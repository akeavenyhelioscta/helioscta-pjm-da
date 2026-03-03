"""Daily weather features for similarity matching.

Uses the PJM population-weighted aggregate station from WSI observed temps.
Produces HDD/CDD, temperature level, and wind features that capture
heating/cooling demand drivers missing from LMP-only matching.
"""
import pandas as pd
import numpy as np
import logging

from pjm_like_day_forecast import configs

logger = logging.getLogger(__name__)


def build(
    df_weather: pd.DataFrame,
    hdd_base: float = configs.HDD_BASE_TEMP,
    cdd_base: float = configs.CDD_BASE_TEMP,
) -> pd.DataFrame:
    """Build daily weather feature vectors for similarity matching.

    Args:
        df_weather: Hourly weather data [date, hour_ending, temp, feels_like_temp,
                    wind_speed_mph, dew_point_temp, relative_humidity, cloud_cover_pct].
        hdd_base: Base temperature for heating degree days (°F).
        cdd_base: Base temperature for cooling degree days (°F).

    Returns:
        DataFrame with one row per date, weather similarity features.
    """
    if df_weather is None or len(df_weather) == 0:
        logger.warning("No weather data provided")
        return pd.DataFrame(columns=["date"])

    df = df_weather.sort_values(["date", "hour_ending"]).reset_index(drop=True)

    # --- Daily temperature aggregates ---
    daily = df.groupby("date").agg(
        temp_daily_avg=("temp", "mean"),
        temp_daily_max=("temp", "max"),
        temp_daily_min=("temp", "min"),
        feels_like_daily_avg=("feels_like_temp", "mean"),
        dew_point_daily_avg=("dew_point_temp", "mean"),
        wind_speed_daily_avg=("wind_speed_mph", "mean"),
        humidity_daily_avg=("relative_humidity", "mean"),
        cloud_cover_daily_avg=("cloud_cover_pct", "mean"),
    )

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
