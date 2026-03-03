"""Feature builder — orchestrator that pulls data, builds features, and merges.

Supports two training modes:
- full_feature (2020+): All P0 features including DA load
- extended (2014+): LMP lags + gas prices + RT load + calendar only
"""
import pandas as pd
import numpy as np
import logging
from datetime import date

from src.pjm_da_forecast import configs
from src.pjm_da_forecast.data import (
    lmps_hourly,
    load_da_hourly,
    load_rt_metered_hourly,
    gas_prices,
    dates,
)
from src.pjm_da_forecast.features import (
    preprocessing,
    lmp_features,
    gas_features,
    load_features,
    calendar_features,
)

logger = logging.getLogger(__name__)


def _scaffold_hourly(df: pd.DataFrame, target_date: date) -> pd.DataFrame:
    """Add 24 NaN rows for target_date if not already present in df."""
    if target_date in df["date"].values:
        return df
    scaffold = pd.DataFrame({"date": [target_date] * 24, "hour_ending": list(range(1, 25))})
    for col in df.columns:
        if col not in scaffold.columns:
            scaffold[col] = np.nan
    return pd.concat([df, scaffold], ignore_index=True)


def _scaffold_dates(df: pd.DataFrame, target_date: date) -> pd.DataFrame:
    """Add 24 calendar rows for target_date if not already present."""
    if target_date in df["date"].values:
        return df
    dt = pd.Timestamp(target_date)
    scaffold = pd.DataFrame({
        "date": [target_date] * 24,
        "hour_ending": list(range(1, 25)),
        "day_of_week_number": [dt.dayofweek] * 24,
        "is_weekend": [dt.dayofweek >= 5] * 24,
        "is_nerc_holiday": [False] * 24,
        "summer_winter": ["SUMMER" if 4 <= dt.month <= 10 else "WINTER"] * 24,
    })
    return pd.concat([df, scaffold], ignore_index=True)


def build_features(
    mode: str = "full_feature",
    schema: str = configs.SCHEMA,
    forecast_date: date | None = None,
) -> pd.DataFrame:
    """Pull all data and build the full feature matrix.

    Args:
        mode: "full_feature" (2020+, includes DA load) or "extended" (2014+, LMP+gas+calendar only).
        schema: Database schema to query.
        forecast_date: Optional future date to forecast. When provided and not
            already in the data, scaffold rows with NaN values are added so
            that lag-based features are computed for this date.

    Returns:
        DataFrame with one row per (date, hour_ending), containing all features
        and the target column 'lmp_total_target'.
    """
    logger.info(f"Building features in '{mode}' mode from schema '{schema}'")

    # --- 1. Pull data ---
    logger.info("Pulling DA LMP data (Western Hub)...")
    df_lmp_da = lmps_hourly.pull(schema=schema, hub=configs.HUB, market="da")

    logger.info("Pulling RT LMP data (Western Hub)...")
    df_lmp_rt = lmps_hourly.pull(schema=schema, hub=configs.HUB, market="rt")

    logger.info("Pulling gas prices...")
    df_gas = gas_prices.pull()

    logger.info("Pulling calendar data...")
    df_dates = dates.pull_hourly(schema=schema)

    logger.info("Pulling RT metered load...")
    df_rt_load = load_rt_metered_hourly.pull(schema=schema)

    df_da_load = None
    if mode == "full_feature":
        logger.info("Pulling DA load...")
        df_da_load = load_da_hourly.pull(schema=schema)

    # --- 1b. Scaffold forecast date rows if not present in data ---
    if forecast_date is not None:
        logger.info(f"Scaffolding forecast date {forecast_date}...")
        df_lmp_da = _scaffold_hourly(df_lmp_da, forecast_date)
        df_lmp_rt = _scaffold_hourly(df_lmp_rt, forecast_date)
        df_rt_load = _scaffold_hourly(df_rt_load, forecast_date)
        df_dates = _scaffold_dates(df_dates, forecast_date)
        if df_da_load is not None:
            df_da_load = _scaffold_hourly(df_da_load, forecast_date)

    # --- 2. Apply asinh to LMP prices ---
    for col in ["lmp_total", "lmp_system_energy_price", "lmp_congestion_price", "lmp_marginal_loss_price"]:
        df_lmp_da[f"{col}_raw"] = df_lmp_da[col]
        df_lmp_da[col] = preprocessing.asinh_transform(df_lmp_da[col])

    for col in ["lmp_total", "lmp_system_energy_price", "lmp_congestion_price", "lmp_marginal_loss_price"]:
        if col in df_lmp_rt.columns:
            df_lmp_rt[col] = preprocessing.asinh_transform(df_lmp_rt[col])

    # --- 3. Build feature modules ---
    logger.info("Building LMP features...")
    df_feat_lmp = lmp_features.build(df_lmp_da=df_lmp_da, df_lmp_rt=df_lmp_rt)

    logger.info("Building gas features...")
    df_feat_gas = gas_features.build(df_gas=df_gas)

    # Implied heat rate: LMP / gas price (using raw LMP before asinh)
    daily_lmp_raw = df_lmp_da.groupby("date")["lmp_total_raw"].mean().reset_index()
    daily_lmp_raw.columns = ["date", "lmp_daily_avg_raw"]
    df_feat_gas = df_feat_gas.merge(daily_lmp_raw, on="date", how="left")
    df_feat_gas["implied_heat_rate"] = (
        df_feat_gas["lmp_daily_avg_raw"] /
        df_feat_gas["gas_m3_price"].replace(0, np.nan)
    ).shift(1)  # lag by 1 day
    df_feat_gas = df_feat_gas.drop(columns=["lmp_daily_avg_raw"])

    logger.info("Building load features...")
    df_feat_load = load_features.build(df_da_load=df_da_load, df_rt_load=df_rt_load)

    logger.info("Building calendar features...")
    df_feat_cal = calendar_features.build(df_dates=df_dates)

    # --- 4. Merge all features on (date, hour_ending) ---
    logger.info("Merging features...")

    # Start with the target: DA LMP (asinh-transformed)
    target = df_lmp_da[["date", "hour_ending", "lmp_total"]].copy()
    target = target.rename(columns={"lmp_total": "lmp_total_target"})

    result = target.copy()

    # Merge LMP features
    result = result.merge(df_feat_lmp, on=["date", "hour_ending"], how="left")

    # Merge gas features (daily grain → broadcast to all hours)
    result = result.merge(df_feat_gas, on="date", how="left")

    # Merge load features
    if "hour_ending" in df_feat_load.columns:
        result = result.merge(df_feat_load, on=["date", "hour_ending"], how="left")
    elif len(df_feat_load) > 0:
        result = result.merge(df_feat_load, on="date", how="left")

    # Merge calendar features
    result = result.merge(df_feat_cal, on=["date", "hour_ending"], how="left")

    # --- 5. Filter by training mode date range ---
    start_date = configs.FULL_FEATURE_START if mode == "full_feature" else configs.EXTENDED_FEATURE_START
    start_date = pd.to_datetime(start_date).date()
    result = result[result["date"] >= start_date].copy()

    # --- 6. Drop warmup rows (NaN from lag computation) ---
    # Keep rows where at least the d-1 lag features are populated
    lag1d_cols = [c for c in result.columns if "lag1d_h1" in c]
    if lag1d_cols:
        result = result.dropna(subset=lag1d_cols[:1])

    result = result.sort_values(["date", "hour_ending"]).reset_index(drop=True)

    n_features = len([c for c in result.columns if c not in ["date", "hour_ending", "lmp_total_target"]])
    n_rows = len(result)
    date_range = f"{result['date'].min()} to {result['date'].max()}"
    logger.info(f"Feature matrix: {n_rows:,} rows, {n_features} features, {date_range}")

    return result
