"""Calendar features for similarity matching.

Cyclical encodings for distance computation + categorical flags for pre-filtering.
"""
from datetime import date

import pandas as pd
import numpy as np
import logging

from src.like_day_forecast import configs

logger = logging.getLogger(__name__)


def compute_for_date(d: date) -> dict:
    """Compute all calendar feature columns for a single date (no DB needed).

    Returns a dict matching the columns produced by ``build()``.
    """
    # dates_daily.day_of_week_number uses Sun=0..Sat=6.
    wd_py = d.weekday()
    dow_num = (wd_py + 1) % 7
    month = d.month
    doy = d.timetuple().tm_yday

    # DOW group (matches build() logic)
    dow_group_map: dict[int, int] = {}
    for group_idx, (_, days) in enumerate(configs.DOW_GROUPS.items()):
        for day in days:
            dow_group_map[day] = group_idx

    result: dict = {
        "dow_group": dow_group_map.get(dow_num, 0),
        "dow_sin": np.sin(2 * np.pi * dow_num / 7),
        "dow_cos": np.cos(2 * np.pi * dow_num / 7),
        "month_sin": np.sin(2 * np.pi * month / 12),
        "month_cos": np.cos(2 * np.pi * month / 12),
        "day_of_year_sin": np.sin(2 * np.pi * doy / 365.25),
        "day_of_year_cos": np.cos(2 * np.pi * doy / 365.25),
        "is_weekend": 1 if wd_py >= 5 else 0,
        "is_nerc_holiday": 0,  # safe default — not in distance groups
        "summer_winter": 1 if 4 <= month <= 10 else 0,
    }
    for i in range(7):
        result[f"dow_{i}"] = 1 if dow_num == i else 0
    return result


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
