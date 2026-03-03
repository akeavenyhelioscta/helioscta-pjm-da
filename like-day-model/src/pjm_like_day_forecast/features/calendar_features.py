"""Calendar features for similarity matching.

Cyclical encodings for distance computation + categorical flags for pre-filtering.
"""
import pandas as pd
import numpy as np
import logging

from pjm_like_day_forecast import configs

logger = logging.getLogger(__name__)


def build(df_dates: pd.DataFrame) -> pd.DataFrame:
    """Build daily calendar features for similarity matching.

    Args:
        df_dates: Daily calendar data with columns
                  [date, day_of_week_number, is_weekend, is_nerc_holiday, summer_winter].

    Returns:
        DataFrame with one row per date, calendar similarity features.
    """
    df = df_dates.copy()
    df = df.sort_values("date").reset_index(drop=True)

    result = df[["date"]].copy()

    # --- Day-of-week group (for pre-filtering) ---
    dow_group_map = {}
    for group_idx, (group_name, days) in enumerate(configs.DOW_GROUPS.items()):
        for day in days:
            dow_group_map[day] = group_idx
    result["dow_group"] = df["day_of_week_number"].map(dow_group_map)

    # --- Cyclical day-of-week encoding (for distance computation) ---
    result["dow_sin"] = np.sin(2 * np.pi * df["day_of_week_number"] / 7)
    result["dow_cos"] = np.cos(2 * np.pi * df["day_of_week_number"] / 7)

    # --- Cyclical month encoding ---
    month = pd.to_datetime(df["date"]).dt.month
    result["month_sin"] = np.sin(2 * np.pi * month / 12)
    result["month_cos"] = np.cos(2 * np.pi * month / 12)

    # --- Cyclical day-of-year encoding (seasonal proximity) ---
    day_of_year = pd.to_datetime(df["date"]).dt.dayofyear
    result["day_of_year_sin"] = np.sin(2 * np.pi * day_of_year / 365.25)
    result["day_of_year_cos"] = np.cos(2 * np.pi * day_of_year / 365.25)

    # --- Binary flags ---
    result["is_weekend"] = df["is_weekend"].astype(int)
    result["is_nerc_holiday"] = df["is_nerc_holiday"].astype(int)
    result["summer_winter"] = (
        df["summer_winter"].str.upper().map({"SUMMER": 1, "WINTER": 0}).fillna(0).astype(int)
    )

    # --- Day-of-week one-hot ---
    for d in range(7):
        result[f"dow_{d}"] = (df["day_of_week_number"] == d).astype(int)

    n_features = len([c for c in result.columns if c != "date"])
    logger.info(f"Built {n_features} calendar similarity features")
    return result
