"""Daily LMP features for similarity matching.

Unlike the da-model (which creates hourly lag features for regression), this module
creates features that describe *what a day looks like* for similarity matching.
Each day gets a single feature vector capturing its LMP market conditions.
"""
import pandas as pd
import numpy as np
import logging

from pjm_like_day_forecast import configs
from pjm_like_day_forecast.features import preprocessing

logger = logging.getLogger(__name__)

# On-peak: HE 8-23, Off-peak: HE 1-7, 24
ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = list(range(1, 8)) + [24]


def build(
    df_lmp_da: pd.DataFrame,
    df_lmp_rt: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build daily LMP feature vectors for similarity matching.

    Args:
        df_lmp_da: DA LMP hourly with [date, hour_ending, lmp_total, ...].
        df_lmp_rt: RT LMP hourly (optional). Used for DART spread.

    Returns:
        DataFrame with one row per date, all LMP similarity features.
    """
    df = df_lmp_da[["date", "hour_ending", "lmp_total", "lmp_system_energy_price",
                     "lmp_congestion_price", "lmp_marginal_loss_price"]].copy()
    df = df.sort_values(["date", "hour_ending"]).reset_index(drop=True)

    # --- Pivot to wide: one row per date, lmp_h1..lmp_h24 ---
    pivot = df.pivot_table(index="date", columns="hour_ending", values="lmp_total")
    pivot.columns = [f"lmp_profile_h{int(c)}" for c in pivot.columns]
    pivot = pivot.sort_index()

    # Apply asinh transform to profile columns (compress spikes for distance calc)
    for col in pivot.columns:
        pivot[col] = preprocessing.asinh_transform(pivot[col])

    result = pivot.copy()

    # --- Daily summary statistics ---
    daily_raw = df.groupby("date").agg(
        lmp_daily_flat=("lmp_total", "mean"),
        lmp_intraday_std=("lmp_total", "std"),
        lmp_intraday_range=("lmp_total", lambda x: x.max() - x.min()),
        lmp_daily_max=("lmp_total", "max"),
        lmp_daily_min=("lmp_total", "min"),
    )

    # On-peak / off-peak averages
    onpeak = df[df["hour_ending"].isin(ONPEAK_HOURS)].groupby("date")["lmp_total"].mean().rename("lmp_onpeak_avg")
    offpeak = df[df["hour_ending"].isin(OFFPEAK_HOURS)].groupby("date")["lmp_total"].mean().rename("lmp_offpeak_avg")

    daily_raw = daily_raw.join(onpeak).join(offpeak)
    daily_raw["lmp_peak_ratio"] = daily_raw["lmp_onpeak_avg"] / daily_raw["lmp_offpeak_avg"].replace(0, np.nan)

    # Component shares
    daily_energy = df.groupby("date")["lmp_system_energy_price"].mean()
    daily_congestion = df.groupby("date")["lmp_congestion_price"].mean()
    daily_total = df.groupby("date")["lmp_total"].mean()

    daily_raw["lmp_congestion_share"] = daily_congestion / daily_total.replace(0, np.nan)
    daily_raw["lmp_energy_share"] = daily_energy / daily_total.replace(0, np.nan)

    # Rolling statistics
    flat = daily_raw["lmp_daily_flat"]
    daily_raw["lmp_7d_rolling_mean"] = flat.rolling(7, min_periods=1).mean()
    daily_raw["lmp_7d_rolling_std"] = flat.rolling(7, min_periods=1).std()
    daily_raw["lmp_30d_rolling_mean"] = flat.rolling(30, min_periods=1).mean()

    # Day-over-day change
    daily_raw["lmp_daily_change"] = flat.diff()

    # Apply asinh to level features (not ratios/shares)
    for col in ["lmp_daily_flat", "lmp_onpeak_avg", "lmp_offpeak_avg",
                 "lmp_7d_rolling_mean", "lmp_30d_rolling_mean",
                 "lmp_daily_max", "lmp_daily_min", "lmp_daily_change"]:
        if col in daily_raw.columns:
            daily_raw[col] = preprocessing.asinh_transform(daily_raw[col])

    result = result.join(daily_raw)

    # --- DART spread (if RT data provided) ---
    if df_lmp_rt is not None and len(df_lmp_rt) > 0:
        rt_daily = df_lmp_rt.groupby("date")["lmp_total"].mean().rename("rt_lmp_daily_flat")
        da_daily = df.groupby("date")["lmp_total"].mean()
        dart = (da_daily - rt_daily).rename("dart_spread_daily")
        result = result.join(preprocessing.asinh_transform(dart))

    result = result.reset_index()

    n_features = len([c for c in result.columns if c != "date"])
    logger.info(f"Built {n_features} LMP similarity features")
    return result
