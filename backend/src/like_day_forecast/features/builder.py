"""Feature builder — orchestrator that pulls data, builds daily features, and merges.

Produces one row per date with all features needed for similarity matching.
"""
from datetime import date, timedelta

import pandas as pd
import numpy as np
import logging
from pathlib import Path

from src.like_day_forecast import configs
from src.utils.cache_utils import pull_with_cache
from src.data import (
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
        pull_kwargs={"schema": schema, "region": configs.LOAD_REGION},
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


# ── Synthetic reference row for rolling-reference strip forecast ─────


def build_synthetic_reference_row(
    df_features: pd.DataFrame,
    today: date,
    target_date: date,
    df_weather: pd.DataFrame | None = None,
    df_solar_forecast: pd.DataFrame | None = None,
    df_wind_forecast: pd.DataFrame | None = None,
    df_outage_forecast: pd.DataFrame | None = None,
) -> pd.Series:
    """Build a synthetic feature row for a future reference date.

    Clones today's actual features (LMP, gas, load, weather reference,
    composites, renewables, outage reference) and replaces:
      - Calendar columns → recomputed for ``target_date - 1``
      - Target-date columns (tgt_*) → from forecast data for ``target_date``

    This allows ``find_analogs()`` to run with correct DOW matching and
    forward-looking weather/renewable/outage signals for each strip day
    without modifying the engine.

    Args:
        df_features: Full daily feature matrix (from ``build_daily_features``).
        today: The actual current date (must exist in *df_features*).
        target_date: The forecast target date (D+N).
        df_weather: Hourly weather with forecast rows for *target_date*.
        df_solar_forecast: Hourly PJM solar forecast (``forecast_date``, ``solar_forecast``).
        df_wind_forecast: Hourly PJM wind forecast (``forecast_date``, ``wind_forecast``).
        df_outage_forecast: Daily outage forecast (``forecast_date``, outage MW columns).

    Returns:
        pd.Series with the same columns as *df_features*, dated at
        ``target_date - 1`` (the synthetic reference date).
    """
    today_mask = df_features["date"] == today
    if not today_mask.any():
        raise ValueError(f"Today ({today}) not found in feature matrix")

    row = df_features.loc[today_mask].iloc[0].copy()
    ref_date = target_date - timedelta(days=1)
    row["date"] = ref_date

    # ── 1. Calendar features ────────────────────────────────────────
    cal = calendar_features.compute_for_date(ref_date)
    for col, val in cal.items():
        if col in row.index:
            row[col] = val

    # ── 2. Target weather features ──────────────────────────────────
    if df_weather is not None and len(df_weather):
        _apply_target_weather(row, df_weather, target_date)

    # ── 3. Target renewable features ────────────────────────────────
    _apply_target_renewables(row, df_solar_forecast, df_wind_forecast, target_date)

    # ── 4. Target outage features ───────────────────────────────────
    if df_outage_forecast is not None and len(df_outage_forecast):
        _apply_target_outages(row, df_outage_forecast, target_date)

    return row


def _apply_target_weather(
    row: pd.Series,
    df_weather: pd.DataFrame,
    target_date: date,
) -> None:
    """Replace tgt_weather columns in *row* using forecast weather for *target_date*."""
    wf = df_weather[df_weather["date"] == target_date]
    if wf.empty:
        logger.warning(
            f"No weather forecast for {target_date}; keeping today's tgt_weather values"
        )
        return

    if "temp" in wf.columns:
        avg_temp = wf["temp"].mean()
        row["tgt_temp_daily_avg"] = avg_temp
        row["tgt_temp_daily_max"] = wf["temp"].max()
        row["tgt_temp_daily_min"] = wf["temp"].min()
        hdd_base, cdd_base = 65.0, 65.0
        row["tgt_hdd"] = max(0.0, hdd_base - avg_temp)
        row["tgt_cdd"] = max(0.0, avg_temp - cdd_base)
        if "temp_daily_avg" in row.index and pd.notna(row["temp_daily_avg"]):
            row["tgt_temp_change_vs_ref"] = avg_temp - row["temp_daily_avg"]

    if "feels_like_temp" in wf.columns and "tgt_feels_like_daily_avg" in row.index:
        row["tgt_feels_like_daily_avg"] = wf["feels_like_temp"].mean()


def _apply_target_renewables(
    row: pd.Series,
    df_solar: pd.DataFrame | None,
    df_wind: pd.DataFrame | None,
    target_date: date,
) -> None:
    """Replace tgt_renewable columns using solar/wind forecasts for *target_date*."""
    solar_avg = None
    wind_avg = None

    if df_solar is not None and len(df_solar):
        sf = df_solar[df_solar["forecast_date"] == target_date]
        if len(sf) and "solar_forecast" in sf.columns:
            solar_avg = sf["solar_forecast"].mean()
            row["tgt_solar_daily_avg"] = solar_avg

    if df_wind is not None and len(df_wind):
        wf = df_wind[df_wind["forecast_date"] == target_date]
        if len(wf) and "wind_forecast" in wf.columns:
            wind_avg = wf["wind_forecast"].mean()
            row["tgt_wind_daily_avg"] = wind_avg

    if solar_avg is None and wind_avg is None:
        logger.warning(
            f"No renewable forecast for {target_date}; keeping today's tgt_renewable values"
        )
        return

    s = solar_avg if solar_avg is not None else row.get("tgt_solar_daily_avg", 0.0)
    w = wind_avg if wind_avg is not None else row.get("tgt_wind_daily_avg", 0.0)
    s = float(s) if pd.notna(s) else 0.0
    w = float(w) if pd.notna(w) else 0.0
    row["tgt_renewable_daily_avg"] = s + w

    if "renewable_daily_avg" in row.index and pd.notna(row["renewable_daily_avg"]):
        row["tgt_renewable_change_vs_ref"] = (
            row["tgt_renewable_daily_avg"] - row["renewable_daily_avg"]
        )


def _apply_target_outages(
    row: pd.Series,
    df_outage_forecast: pd.DataFrame,
    target_date: date,
) -> None:
    """Replace tgt_outage columns using outage forecast for *target_date*."""
    # Use latest execution for the target date, RTO region
    of = df_outage_forecast.copy()
    if "region" in of.columns:
        of = of[of["region"] == "RTO"]
    of = of[of["forecast_date"] == target_date]
    if of.empty:
        logger.warning(
            f"No outage forecast for {target_date}; keeping today's tgt_outage values"
        )
        return

    # Pick latest execution
    if "forecast_execution_date" in of.columns:
        of = of.sort_values("forecast_execution_date", ascending=False)
    latest = of.iloc[0]

    if "total_outages_mw" in latest.index:
        row["tgt_outage_total_mw"] = latest["total_outages_mw"]
    if "forced_outages_mw" in latest.index:
        row["tgt_outage_forced_mw"] = latest["forced_outages_mw"]
    if (
        "outage_total_mw" in row.index
        and pd.notna(row["outage_total_mw"])
        and pd.notna(row.get("tgt_outage_total_mw"))
    ):
        row["tgt_outage_change_vs_ref"] = row["tgt_outage_total_mw"] - row["outage_total_mw"]
