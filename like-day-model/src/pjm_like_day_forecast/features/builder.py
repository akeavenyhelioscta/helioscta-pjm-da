"""Feature builder — orchestrator that pulls data, builds daily features, and merges.

Produces one row per date with all features needed for similarity matching.
"""
import pandas as pd
import numpy as np
import logging

from pjm_like_day_forecast import configs
from pjm_like_day_forecast.data import (
    lmps_hourly,
    load_da_hourly,
    load_rt_metered_hourly,
    gas_prices,
    dates,
    weather_hourly,
)
from pjm_like_day_forecast.features import (
    lmp_features,
    gas_features,
    load_features,
    calendar_features,
    composite,
    weather_features,
    target_load_features,
    target_weather_features,
)

logger = logging.getLogger(__name__)


def build_daily_features(
    schema: str = configs.SCHEMA,
) -> pd.DataFrame:
    """Pull all data and build the daily feature matrix for similarity matching.

    Returns:
        DataFrame with one row per date, containing all feature columns.
    """
    logger.info(f"Building daily similarity features from schema '{schema}'")

    # --- 1. Pull data ---
    logger.info("Pulling DA LMP data (Western Hub)...")
    df_lmp_da = lmps_hourly.pull(schema=schema, hub=configs.HUB, market="da")

    logger.info("Pulling RT LMP data (Western Hub)...")
    df_lmp_rt = lmps_hourly.pull(schema=schema, hub=configs.HUB, market="rt")

    logger.info("Pulling gas prices...")
    df_gas = gas_prices.pull()

    logger.info("Pulling calendar data...")
    df_dates = dates.pull_daily(schema=schema)

    logger.info("Pulling RT metered load...")
    df_rt_load = load_rt_metered_hourly.pull(schema=schema)

    logger.info("Pulling DA load...")
    df_da_load = None
    try:
        df_da_load = load_da_hourly.pull(schema=schema)
    except Exception as e:
        logger.warning(f"DA load pull failed (may not be available): {e}")

    logger.info("Pulling observed weather (PJM aggregate)...")
    df_weather = None
    try:
        df_weather = weather_hourly.pull()
    except Exception as e:
        logger.warning(f"Weather pull failed (may not be available): {e}")

    # --- 2. Build feature modules ---
    logger.info("Building LMP features...")
    df_feat_lmp = lmp_features.build(df_lmp_da=df_lmp_da, df_lmp_rt=df_lmp_rt)

    logger.info("Building gas features...")
    df_feat_gas = gas_features.build(df_gas=df_gas)

    logger.info("Building load features...")
    df_feat_load = load_features.build(df_da_load=df_da_load, df_rt_load=df_rt_load)

    logger.info("Building calendar features...")
    df_feat_cal = calendar_features.build(df_dates=df_dates)

    logger.info("Building weather features...")
    df_feat_weather = weather_features.build(df_weather=df_weather)

    logger.info("Building composite features...")
    df_feat_composite = composite.build(
        df_lmp_features=df_feat_lmp,
        df_gas_features=df_feat_gas,
        df_load_features=df_feat_load,
    )

    logger.info("Building target-date load features...")
    df_feat_target_load = target_load_features.build(
        df_da_load=df_da_load,
        df_ref_load_features=df_feat_load,
    )

    logger.info("Building target-date weather features...")
    df_feat_target_weather = target_weather_features.build(
        df_weather=df_weather,
        df_ref_weather_features=df_feat_weather,
    )

    # --- 3. Merge all features on date ---
    logger.info("Merging features...")
    result = df_feat_lmp.copy()
    result = result.merge(df_feat_gas, on="date", how="left")
    result = result.merge(df_feat_load, on="date", how="left")
    result = result.merge(df_feat_cal, on="date", how="left")
    result = result.merge(df_feat_weather, on="date", how="left")
    result = result.merge(df_feat_composite, on="date", how="left")
    result = result.merge(df_feat_target_load, on="date", how="left")
    result = result.merge(df_feat_target_weather, on="date", how="left")

    # --- 4. Filter date range ---
    start_date = pd.to_datetime(configs.EXTENDED_FEATURE_START).date()
    result = result[result["date"] >= start_date].copy()

    # --- 5. Drop warmup NaN rows (from rolling/diff computations) ---
    # Keep rows where the 30-day rolling mean is populated
    if "lmp_30d_rolling_mean" in result.columns:
        result = result.dropna(subset=["lmp_30d_rolling_mean"])

    result = result.sort_values("date").reset_index(drop=True)

    n_features = len([c for c in result.columns if c != "date"])
    n_rows = len(result)
    date_range = f"{result['date'].min()} to {result['date'].max()}"
    logger.info(f"Daily feature matrix: {n_rows:,} days, {n_features} features, {date_range}")

    return result
