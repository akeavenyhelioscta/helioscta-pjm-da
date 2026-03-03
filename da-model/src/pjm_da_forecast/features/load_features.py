"""Load features.

DA load forecast is the primary demand signal (available 2020+).
RT metered load provides lag features back to 2014.
"""
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def build(
    df_da_load: pd.DataFrame | None = None,
    df_rt_load: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build load features from DA and/or RT load data.

    Args:
        df_da_load: DA load hourly [date, hour_ending, da_load_mw]. Available 2020+.
        df_rt_load: RT metered load hourly [date, hour_ending, rt_load_mw]. Available 2014+.

    Returns:
        DataFrame with columns [date, hour_ending, ...load features].
    """
    frames = []

    # --- DA load features (2020+ only) ---
    if df_da_load is not None and len(df_da_load) > 0:
        da = df_da_load[["date", "hour_ending", "da_load_mw"]].copy()
        da = da.sort_values(["date", "hour_ending"]).reset_index(drop=True)

        # Daily peak for shape calculation
        daily_peak = da.groupby("date")["da_load_mw"].max().rename("da_load_daily_peak")

        # Merge peak back
        da = da.merge(daily_peak, on="date", how="left")

        # Load shape: hour load / daily peak
        da["da_load_shape"] = da["da_load_mw"] / da["da_load_daily_peak"].replace(0, np.nan)

        # Load ramp: da_load[h] - da_load[h-1] (within same date)
        da["da_load_ramp"] = da.groupby("date")["da_load_mw"].diff()

        # Load vs 7-day rolling average (demand anomaly)
        daily_avg = da.groupby("date")["da_load_mw"].mean()
        load_7d_avg = daily_avg.rolling(7, min_periods=1).mean().rename("da_load_7d_avg")
        load_anomaly = (daily_avg / load_7d_avg).rename("da_load_vs_7d_avg")

        da = da.merge(load_anomaly, on="date", how="left")

        da_features = da[["date", "hour_ending", "da_load_mw", "da_load_shape",
                          "da_load_ramp", "da_load_vs_7d_avg"]]
        frames.append(da_features)

    # --- RT metered load features (2014+, lag-based) ---
    if df_rt_load is not None and len(df_rt_load) > 0:
        rt = df_rt_load[["date", "hour_ending", "rt_load_mw"]].copy()
        rt = rt.sort_values(["date", "hour_ending"]).reset_index(drop=True)

        # Pivot to wide: rt_load per hour per date
        rt_pivot = rt.pivot_table(index="date", columns="hour_ending", values="rt_load_mw")
        # Reindex to preserve scaffold dates (pivot_table drops all-NaN rows)
        rt_pivot = rt_pivot.reindex(sorted(rt["date"].unique()))

        # RT load same hour yesterday (lag 1 day)
        rt_lag1d = rt_pivot.shift(1)

        # Unpivot back to hourly
        rt_lag1d_long = rt_lag1d.stack().reset_index()
        rt_lag1d_long.columns = ["date", "hour_ending", "rt_load_lag1d"]

        frames.append(rt_lag1d_long)

        # RT load forecast error proxy (if DA load available)
        if df_da_load is not None and len(df_da_load) > 0:
            # Merge RT actual with DA forecast on date-1 (lagged)
            rt_daily = rt.groupby("date")["rt_load_mw"].mean().reset_index()
            rt_daily.columns = ["date", "rt_load_daily_avg"]
            da_daily = df_da_load.groupby("date")["da_load_mw"].mean().reset_index()
            da_daily.columns = ["date", "da_load_daily_avg"]

            merged = rt_daily.merge(da_daily, on="date", how="inner")
            merged["load_forecast_error"] = merged["rt_load_daily_avg"] - merged["da_load_daily_avg"]
            # Lag by 1 day (yesterday's forecast error)
            merged = merged.sort_values("date")
            merged["load_forecast_error_lag1d"] = merged["load_forecast_error"].shift(1)
            error_feature = merged[["date", "load_forecast_error_lag1d"]]
            frames.append(error_feature)

    if not frames:
        logger.warning("No load data provided")
        return pd.DataFrame(columns=["date", "hour_ending"])

    # Merge all load features
    result = frames[0]
    for f in frames[1:]:
        merge_cols = ["date", "hour_ending"] if "hour_ending" in f.columns else ["date"]
        result = result.merge(f, on=merge_cols, how="outer")

    n_features = len([c for c in result.columns if c not in ["date", "hour_ending"]])
    logger.info(f"Built {n_features} load features")
    return result
