"""LMP price features following Lago 2021 LEAR structure.

Creates 120 price lag features (5 lag days x 24 hours), rolling statistics,
component shares, and cross-hub spread features.
"""
import pandas as pd
import numpy as np
import logging

from src.pjm_da_forecast import configs

logger = logging.getLogger(__name__)


def build(
    df_lmp_da: pd.DataFrame,
    df_lmp_rt: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build LMP features from DA (and optionally RT) hourly LMP data.

    Args:
        df_lmp_da: DA LMP hourly data with columns [date, hour_ending, lmp_total,
                   lmp_system_energy_price, lmp_congestion_price, lmp_marginal_loss_price]
        df_lmp_rt: RT LMP hourly data (same columns). Used for DART spread features.

    Returns:
        DataFrame indexed by (date, hour_ending) with all LMP features.
    """
    df = df_lmp_da[["date", "hour_ending", "lmp_total", "lmp_system_energy_price",
                     "lmp_congestion_price", "lmp_marginal_loss_price"]].copy()
    df = df.sort_values(["date", "hour_ending"]).reset_index(drop=True)

    # --- Pivot to wide format: one row per date, columns = lmp_h1..lmp_h24 ---
    pivot = df.pivot_table(index="date", columns="hour_ending", values="lmp_total")
    pivot.columns = [f"lmp_h{int(c)}" for c in pivot.columns]
    # Reindex to preserve scaffold dates (pivot_table drops all-NaN rows)
    pivot = pivot.reindex(sorted(df["date"].unique()))

    # --- Daily flat average ---
    daily_flat = df.groupby("date")["lmp_total"].mean().rename("lmp_daily_flat")

    # --- Price lag features: d-1, d-2, d-3, d-7, d-14 (all 24h each) ---
    lag_frames = []
    for lag_d in configs.PRICE_LAG_DAYS:
        lagged = pivot.shift(lag_d)
        lagged.columns = [f"da_lmp_lag{lag_d}d_h{c.split('_h')[1]}" for c in lagged.columns]
        lag_frames.append(lagged)

    df_lags = pd.concat(lag_frames, axis=1)

    # --- Rolling stats on daily flat average ---
    df_rolling = pd.DataFrame(index=daily_flat.index)
    df_rolling["lmp_rolling_7d_mean"] = daily_flat.rolling(7, min_periods=1).mean()
    df_rolling["lmp_rolling_7d_std"] = daily_flat.rolling(7, min_periods=1).std()
    df_rolling["lmp_rolling_14d_mean"] = daily_flat.rolling(14, min_periods=1).mean()
    df_rolling["lmp_rolling_30d_mean"] = daily_flat.rolling(30, min_periods=1).mean()

    # --- Same-hour 7-day mean (per hour) ---
    same_hour_7d = pivot.rolling(7, min_periods=1).mean()
    same_hour_7d.columns = [f"lmp_same_hour_7d_mean_h{c.split('_h')[1]}" for c in same_hour_7d.columns]

    # --- On-peak / off-peak ratio from prior day ---
    # On-peak: hours 7-22, Off-peak: hours 1-6, 23-24
    onpeak_cols = [f"lmp_h{h}" for h in range(7, 23)]
    offpeak_cols = [f"lmp_h{h}" for h in list(range(1, 7)) + [23, 24]]
    onpeak_avg = pivot[onpeak_cols].mean(axis=1)
    offpeak_avg = pivot[offpeak_cols].mean(axis=1)
    peak_ratio = (onpeak_avg / offpeak_avg.replace(0, np.nan)).shift(1).rename("lmp_onpeak_offpeak_ratio_lag1d")

    # --- LMP component shares from prior day ---
    daily_energy = df.groupby("date")["lmp_system_energy_price"].mean()
    daily_congestion = df.groupby("date")["lmp_congestion_price"].mean()
    daily_total = df.groupby("date")["lmp_total"].mean()

    df_components = pd.DataFrame(index=daily_total.index)
    df_components["lmp_energy_share_lag1d"] = (daily_energy / daily_total.replace(0, np.nan)).shift(1)
    df_components["lmp_congestion_share_lag1d"] = (daily_congestion / daily_total.replace(0, np.nan)).shift(1)

    # --- Shape index: prior day's LMP at each hour / prior day's flat avg ---
    shape_index = pivot.div(daily_flat, axis=0).shift(1)
    shape_index.columns = [f"lmp_shape_lag1d_h{c.split('_h')[1]}" for c in shape_index.columns]

    # --- DART spread features (if RT data provided) ---
    df_dart = pd.DataFrame(index=daily_flat.index)
    if df_lmp_rt is not None and len(df_lmp_rt) > 0:
        rt_pivot = df_lmp_rt.pivot_table(index="date", columns="hour_ending", values="lmp_total")
        rt_pivot.columns = [f"rt_lmp_h{int(c)}" for c in rt_pivot.columns]
        # Reindex to preserve scaffold dates
        rt_pivot = rt_pivot.reindex(sorted(df_lmp_rt["date"].unique()))

        # Daily DART spread (DA - RT)
        common_dates = pivot.index.intersection(rt_pivot.index)
        da_aligned = pivot.loc[common_dates]
        rt_aligned = rt_pivot.loc[common_dates]

        dart_daily = da_aligned.values - rt_aligned.values
        dart_daily_mean = pd.Series(np.nanmean(dart_daily, axis=1), index=common_dates, name="dart_spread_daily_lag1d")
        df_dart = dart_daily_mean.shift(1).to_frame()

        # RT LMP same hour yesterday
        rt_lag1d = rt_pivot.shift(1)
        rt_lag1d.columns = [f"rt_lmp_lag1d_h{c.split('_h')[1]}" for c in rt_lag1d.columns]
        df_dart = df_dart.join(rt_lag1d, how="outer")

    # --- Merge all daily-level features ---
    features_daily = pd.concat([
        df_lags,
        df_rolling,
        peak_ratio,
        df_components,
        df_dart,
    ], axis=1)

    # --- Merge back to hourly grain ---
    result = df[["date", "hour_ending"]].copy()
    result = result.merge(features_daily, left_on="date", right_index=True, how="left")

    # Add same-hour-7d-mean for the specific hour (select the matching column per row)
    for h in configs.HOURS:
        col = f"lmp_same_hour_7d_mean_h{h}"
        if col in same_hour_7d.columns:
            same_hour_vals = same_hour_7d[[col]].shift(1)  # lag by 1 day
            same_hour_vals.columns = ["lmp_same_hour_7d_mean"]
            mask = result["hour_ending"] == h
            matched = result.loc[mask, "date"].map(
                same_hour_vals["lmp_same_hour_7d_mean"].to_dict()
            )
            result.loc[mask, "lmp_same_hour_7d_mean"] = matched.values

    # Add shape index for the specific hour
    for h in configs.HOURS:
        col = f"lmp_shape_lag1d_h{h}"
        if col in shape_index.columns:
            shape_vals = shape_index[col].to_dict()
            mask = result["hour_ending"] == h
            result.loc[mask, "lmp_shape_lag1d"] = result.loc[mask, "date"].map(shape_vals).values

    # Drop the per-hour shape columns (already extracted into single column)
    shape_cols = [c for c in result.columns if c.startswith("lmp_shape_lag1d_h")]
    result = result.drop(columns=shape_cols, errors="ignore")

    # Drop per-hour same_hour_7d columns
    sh_cols = [c for c in result.columns if c.startswith("lmp_same_hour_7d_mean_h")]
    result = result.drop(columns=sh_cols, errors="ignore")

    n_features = len([c for c in result.columns if c not in ["date", "hour_ending"]])
    logger.info(f"Built {n_features} LMP features")
    return result
