"""Target-date (D+1) weather features for similarity matching.

Shifts weather aggregates back by 1 day so they align with the reference date.
For backtest this uses D+1 observed weather. For production (today), D+1
forecast weather is included via the UNION query in weather_hourly.sql.
"""
import pandas as pd
import numpy as np
import logging

from src.like_day_forecast import configs

logger = logging.getLogger(__name__)


def build(
    df_weather: pd.DataFrame | None = None,
    df_ref_weather_features: pd.DataFrame | None = None,
    hdd_base: float = configs.HDD_BASE_TEMP,
    cdd_base: float = configs.CDD_BASE_TEMP,
) -> pd.DataFrame:
    """Build target-date (D+1) weather features, shifted back to the reference date.

    Args:
        df_weather: Hourly weather data [date, hour_ending, temp].
        df_ref_weather_features: Reference-date weather features (for cross-day delta).
            Must have columns [date, temp_daily_avg].
        hdd_base: Base temperature for heating degree days (deg F).
        cdd_base: Base temperature for cooling degree days (deg F).

    Returns:
        DataFrame with one row per date, target-date weather features.
        The date column represents the reference date (D), features describe D+1.
    """
    if df_weather is None or len(df_weather) == 0:
        logger.warning("No weather data provided for target weather features")
        return pd.DataFrame(columns=["date"])

    df = df_weather.sort_values(["date", "hour_ending"]).reset_index(drop=True)

    # Daily aggregates on D+1
    agg_dict = {
        "tgt_temp_daily_avg": ("temp", "mean"),
        "tgt_temp_daily_max": ("temp", "max"),
        "tgt_temp_daily_min": ("temp", "min"),
    }

    if "feels_like_temp" in df.columns:
        agg_dict["tgt_feels_like_daily_avg"] = ("feels_like_temp", "mean")

    daily = df.groupby("date").agg(**agg_dict)

    # HDD / CDD
    daily["tgt_hdd"] = np.maximum(0, hdd_base - daily["tgt_temp_daily_avg"])
    daily["tgt_cdd"] = np.maximum(0, daily["tgt_temp_daily_avg"] - cdd_base)

    daily = daily.reset_index()

    # Shift date back by 1 day: D+1 features → assigned to reference date D
    daily["date"] = daily["date"] - pd.Timedelta(days=1)

    # Cross-day delta: D+1 avg temp - D avg temp (temperature transition signal)
    if df_ref_weather_features is not None and "temp_daily_avg" in df_ref_weather_features.columns:
        ref = df_ref_weather_features[["date", "temp_daily_avg"]].copy()
        daily = daily.merge(ref, on="date", how="left")
        daily["tgt_temp_change_vs_ref"] = daily["tgt_temp_daily_avg"] - daily["temp_daily_avg"]
        daily = daily.drop(columns=["temp_daily_avg"])

    n_features = len([c for c in daily.columns if c != "date"])
    logger.info(f"Built {n_features} target-weather similarity features")
    return daily
