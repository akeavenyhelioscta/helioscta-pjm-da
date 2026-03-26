"""Feature builder — orchestrator that pulls data, builds daily features, and merges.

Produces one row per date with all features needed for similarity matching.
"""
import pandas as pd
import numpy as np
import logging
from pathlib import Path

from src.like_day_forecast import configs
from src.like_day_forecast.utils.cache_utils import pull_with_cache
from src.like_day_forecast.data import (
    lmps_hourly,
    load_rt_metered_hourly,
    gas_prices,
    dates,
    weather_hourly,
    fuel_mix_hourly,
    outages_actual_daily,
    solar_forecast_hourly,
    wind_forecast_hourly,
)
from src.like_day_forecast.features import (
    lmp_features,
    gas_features,
    load_features,
    calendar_features,
    composite,
    weather_features,
    renewable_features,
    outage_features,
    target_weather_features,
    target_renewable_features,
    target_outage_features,
)

logger = logging.getLogger(__name__)


def build_daily_features(
    schema: str = configs.SCHEMA,
    hub: str = configs.HUB,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> pd.DataFrame:
    """Pull all data and build the daily feature matrix for similarity matching.

    Returns:
        DataFrame with one row per date, containing all feature columns.
    """
    logger.info(f"Building daily similarity features from schema '{schema}' for hub '{hub}'")

    cache_kwargs = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    # --- 1. Pull data (cached) ---
    logger.info(f"Pulling DA LMP data ({hub})...")
    df_lmp_da = pull_with_cache(
        source_name="lmps_hourly_da",
        pull_fn=lmps_hourly.pull,
        pull_kwargs={"schema": schema, "hub": hub, "market": "da"},
        **cache_kwargs,
    )

    logger.info(f"Pulling RT LMP data ({hub})...")
    df_lmp_rt = pull_with_cache(
        source_name="lmps_hourly_rt",
        pull_fn=lmps_hourly.pull,
        pull_kwargs={"schema": schema, "hub": hub, "market": "rt"},
        **cache_kwargs,
    )

    logger.info("Pulling gas prices...")
    df_gas = pull_with_cache(
        source_name="gas_prices",
        pull_fn=gas_prices.pull,
        pull_kwargs={},
        **cache_kwargs,
    )

    logger.info("Pulling calendar data...")
    df_dates = pull_with_cache(
        source_name="dates_daily",
        pull_fn=dates.pull_daily,
        pull_kwargs={"schema": schema},
        **cache_kwargs,
    )

    logger.info("Pulling RT metered load...")
    df_rt_load = pull_with_cache(
        source_name="load_rt_metered_hourly",
        pull_fn=load_rt_metered_hourly.pull,
        pull_kwargs={"schema": schema},
        **cache_kwargs,
    )

    logger.info("Pulling observed weather (PJM aggregate)...")
    df_weather = None
    try:
        df_weather = pull_with_cache(
            source_name="weather_hourly",
            pull_fn=weather_hourly.pull,
            pull_kwargs={},
            **cache_kwargs,
        )
    except Exception as e:
        logger.warning(f"Weather pull failed (may not be available): {e}")

    logger.info("Pulling fuel mix (actual solar/wind generation)...")
    df_fuel_mix = None
    try:
        df_fuel_mix = pull_with_cache(
            source_name="fuel_mix_hourly",
            pull_fn=fuel_mix_hourly.pull,
            pull_kwargs={},
            **cache_kwargs,
        )
    except Exception as e:
        logger.warning(f"Fuel mix pull failed: {e}")

    logger.info("Pulling outages actual daily...")
    df_outages = None
    try:
        df_outages = pull_with_cache(
            source_name="outages_actual_daily",
            pull_fn=outages_actual_daily.pull,
            pull_kwargs={"schema": schema},
            **cache_kwargs,
        )
    except Exception as e:
        logger.warning(f"Outages pull failed: {e}")

    logger.info("Pulling D+1 solar forecast...")
    df_solar_forecast = None
    try:
        df_solar_forecast = pull_with_cache(
            source_name="solar_forecast_hourly",
            pull_fn=solar_forecast_hourly.pull,
            pull_kwargs={},
            **cache_kwargs,
        )
    except Exception as e:
        logger.warning(f"Solar forecast pull failed: {e}")

    logger.info("Pulling D+1 wind forecast...")
    df_wind_forecast = None
    try:
        df_wind_forecast = pull_with_cache(
            source_name="wind_forecast_hourly",
            pull_fn=wind_forecast_hourly.pull,
            pull_kwargs={},
            **cache_kwargs,
        )
    except Exception as e:
        logger.warning(f"Wind forecast pull failed: {e}")

    # --- 2. Build feature modules ---
    logger.info("Building LMP features...")
    df_feat_lmp = lmp_features.build(df_lmp_da=df_lmp_da, df_lmp_rt=df_lmp_rt)

    logger.info("Building gas features...")
    df_feat_gas = gas_features.build(df_gas=df_gas)

    logger.info("Building load features...")
    df_feat_load = load_features.build(df_rt_load=df_rt_load)

    logger.info("Building calendar features...")
    df_feat_cal = calendar_features.build(df_dates=df_dates)

    logger.info("Building weather features...")
    df_feat_weather = weather_features.build(df_weather=df_weather)

    logger.info("Building renewable features...")
    df_feat_renewable = renewable_features.build(df_fuel_mix=df_fuel_mix)

    logger.info("Building outage features...")
    df_feat_outage = outage_features.build(df_outages=df_outages)

    logger.info("Building composite features...")
    df_feat_composite = composite.build(
        df_lmp_features=df_feat_lmp,
        df_gas_features=df_feat_gas,
        df_load_features=df_feat_load,
    )

    logger.info("Building target-date weather features...")
    df_feat_target_weather = target_weather_features.build(
        df_weather=df_weather,
        df_ref_weather_features=df_feat_weather,
    )

    logger.info("Building target-date renewable features...")
    df_feat_target_renewable = target_renewable_features.build(
        df_fuel_mix=df_fuel_mix,
        df_solar_forecast=df_solar_forecast,
        df_wind_forecast=df_wind_forecast,
        df_ref_renewable_features=df_feat_renewable,
    )

    logger.info("Building target-date outage features...")
    df_feat_target_outage = target_outage_features.build(
        df_outages=df_outages,
        df_ref_outage_features=df_feat_outage,
    )

    # --- 3. Merge all features on date ---
    logger.info("Merging features...")
    result = df_feat_lmp.copy()
    result = result.merge(df_feat_gas, on="date", how="left")
    result = result.merge(df_feat_load, on="date", how="left")
    result = result.merge(df_feat_cal, on="date", how="left")
    result = result.merge(df_feat_weather, on="date", how="left")
    result = result.merge(df_feat_renewable, on="date", how="left")
    result = result.merge(df_feat_outage, on="date", how="left")
    result = result.merge(df_feat_composite, on="date", how="left")
    result = result.merge(df_feat_target_weather, on="date", how="left")
    result = result.merge(df_feat_target_renewable, on="date", how="left")
    result = result.merge(df_feat_target_outage, on="date", how="left")

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
