"""Calendar and temporal features.

Includes cyclical encodings, Fourier terms for annual/weekly seasonality,
binary flags, and PJM-specific day-of-week groups.
"""
import pandas as pd
import numpy as np
import logging

from src.pjm_da_forecast import configs

logger = logging.getLogger(__name__)


def build(df_dates: pd.DataFrame) -> pd.DataFrame:
    """Build calendar features from dates data.

    Args:
        df_dates: Hourly calendar data with columns
                  [date, hour_ending, day_of_week_number, is_weekend, is_nerc_holiday, summer_winter].

    Returns:
        DataFrame with columns [date, hour_ending, ...calendar features].
    """
    df = df_dates.copy()
    df = df.sort_values(["date", "hour_ending"]).reset_index(drop=True)

    result = df[["date", "hour_ending"]].copy()

    # --- Cyclical encodings ---
    # Hour (period = 24)
    result["hour_sin"] = np.sin(2 * np.pi * df["hour_ending"] / 24)
    result["hour_cos"] = np.cos(2 * np.pi * df["hour_ending"] / 24)

    # Day of week (period = 7)
    result["dow_sin"] = np.sin(2 * np.pi * df["day_of_week_number"] / 7)
    result["dow_cos"] = np.cos(2 * np.pi * df["day_of_week_number"] / 7)

    # Month (period = 12)
    month = pd.to_datetime(df["date"]).dt.month
    result["month_sin"] = np.sin(2 * np.pi * month / 12)
    result["month_cos"] = np.cos(2 * np.pi * month / 12)

    # --- Fourier terms for annual seasonality (365.25-day period) ---
    day_of_year = pd.to_datetime(df["date"]).dt.dayofyear
    for k in [1, 2, 3]:  # 3 harmonics
        result[f"annual_sin_{k}"] = np.sin(2 * np.pi * k * day_of_year / 365.25)
        result[f"annual_cos_{k}"] = np.cos(2 * np.pi * k * day_of_year / 365.25)

    # --- Fourier terms for weekly seasonality (7-day period) ---
    for k in [1, 2]:  # 2 harmonics
        result[f"weekly_sin_{k}"] = np.sin(2 * np.pi * k * df["day_of_week_number"] / 7)
        result[f"weekly_cos_{k}"] = np.cos(2 * np.pi * k * df["day_of_week_number"] / 7)

    # --- Binary flags ---
    result["is_weekend"] = df["is_weekend"].astype(int)
    result["is_nerc_holiday"] = df["is_nerc_holiday"].astype(int)
    result["summer_winter"] = df["summer_winter"].str.upper().map({"SUMMER": 1, "WINTER": 0}).fillna(0).astype(int)

    # --- Peak hour flag: hours 7-22 on non-holiday weekdays ---
    result["is_peak_hour"] = (
        (df["hour_ending"] >= 7) &
        (df["hour_ending"] <= 22) &
        (df["is_weekend"] == False) &
        (df["is_nerc_holiday"] == False)
    ).astype(int)

    # --- Day-of-week group (PJM-specific) ---
    dow_group_map = {}
    for group_idx, (group_name, days) in enumerate(configs.DOW_GROUPS.items()):
        for day in days:
            dow_group_map[day] = group_idx
    result["dow_group"] = df["day_of_week_number"].map(dow_group_map)

    # --- Day-of-week one-hot (per LEAR standard) ---
    for d in range(7):
        result[f"dow_{d}"] = (df["day_of_week_number"] == d).astype(int)

    n_features = len([c for c in result.columns if c not in ["date", "hour_ending"]])
    logger.info(f"Built {n_features} calendar features")
    return result
