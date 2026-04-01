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
    load_forecast_vintages,
    dates,
    weather_hourly,
    fuel_mix_hourly,
    outages_actual_daily,
    outages_forecast_daily,
    gas_prices_hourly,
    solar_forecast_vintages,
    wind_forecast_vintages,
)
from src.like_day_forecast.features import (
    lmp_features,
    load_features,
    calendar_features,
    composite,
    weather_features,
    renewable_features,
    outage_features,
    target_weather_features,
    target_renewable_features,
    target_outage_features,
    target_load_features,
    net_load_features,
    gas_hourly_features,
    nuclear_features,
    congestion_features,
    fuel_mix_shares_features,
)

logger = logging.getLogger(__name__)


def _normalize_renewable_mode(mode: str | None) -> str:
    """Normalize renewable forecast source mode."""
    mode_norm = (mode or "blend").strip().lower()
    if mode_norm in {"pjm", "meteologica", "blend"}:
        return mode_norm
    logger.warning(f"Unsupported renewable_mode='{mode}'; defaulting to 'blend'")
    return "blend"


def _coerce_blend_weight(weight: float | None) -> float:
    """Clamp blend weight to [0, 1]."""
    if weight is None:
        return 0.5
    return min(1.0, max(0.0, float(weight)))


def _standardize_hourly_forecast(
    df: pd.DataFrame | None,
    value_candidates: tuple[str, ...],
    output_value_col: str,
    output_date_col: str,
) -> pd.DataFrame | None:
    """Normalize forecast frames to [date_col, hour_ending, value_col]."""
    if df is None or len(df) == 0:
        return None

    date_col = None
    for c in ("date", "forecast_date"):
        if c in df.columns:
            date_col = c
            break
    if date_col is None or "hour_ending" not in df.columns:
        return None

    value_col = next((c for c in value_candidates if c in df.columns), None)
    if value_col is None:
        return None

    out = df[[date_col, "hour_ending", value_col]].copy()
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce").dt.date
    out["hour_ending"] = pd.to_numeric(out["hour_ending"], errors="coerce")
    out[value_col] = pd.to_numeric(out[value_col], errors="coerce")
    out = out.dropna(subset=[date_col, "hour_ending", value_col])
    out["hour_ending"] = out["hour_ending"].astype(int)
    out = out.rename(columns={date_col: output_date_col, value_col: output_value_col})
    out = out.groupby([output_date_col, "hour_ending"], as_index=False)[output_value_col].mean()
    return out.sort_values([output_date_col, "hour_ending"]).reset_index(drop=True)


def _merge_forecast_sources(
    df_pjm: pd.DataFrame | None,
    df_meteo: pd.DataFrame | None,
    mode: str,
    blend_weight_pjm: float,
    value_col: str,
    date_col: str,
) -> pd.DataFrame | None:
    """Select or blend two normalized hourly forecast frames."""
    mode_norm = _normalize_renewable_mode(mode)
    w_pjm = _coerce_blend_weight(blend_weight_pjm)

    if mode_norm == "pjm":
        if df_pjm is None and df_meteo is not None:
            logger.warning("Renewable mode 'pjm' unavailable; falling back to Meteologica")
            return df_meteo
        return df_pjm
    if mode_norm == "meteologica":
        if df_meteo is None and df_pjm is not None:
            logger.warning("Renewable mode 'meteologica' unavailable; falling back to PJM")
            return df_pjm
        return df_meteo

    # blend
    if df_pjm is None:
        return df_meteo
    if df_meteo is None:
        return df_pjm

    merged = df_pjm.merge(
        df_meteo,
        on=[date_col, "hour_ending"],
        how="outer",
        suffixes=("_pjm", "_meteo"),
    )
    p_col = f"{value_col}_pjm"
    m_col = f"{value_col}_meteo"
    both = merged[p_col].notna() & merged[m_col].notna()
    merged[value_col] = merged[p_col].where(
        ~both,
        w_pjm * merged[p_col] + (1.0 - w_pjm) * merged[m_col],
    )
    merged[value_col] = merged[value_col].fillna(merged[m_col])
    out = merged[[date_col, "hour_ending", value_col]].dropna(subset=[value_col])
    return out.sort_values([date_col, "hour_ending"]).reset_index(drop=True)


def _resolve_daily_avg(
    df_pjm: pd.DataFrame | None,
    df_meteo: pd.DataFrame | None,
    target_date: date,
    mode: str,
    blend_weight_pjm: float,
    pjm_value_candidates: tuple[str, ...],
    meteo_value_candidates: tuple[str, ...],
) -> float | None:
    """Resolve a daily mean value for one target date using source mode."""
    pjm_norm = _standardize_hourly_forecast(
        df=df_pjm,
        value_candidates=pjm_value_candidates,
        output_value_col="forecast_value",
        output_date_col="forecast_date",
    )
    meteo_norm = _standardize_hourly_forecast(
        df=df_meteo,
        value_candidates=meteo_value_candidates,
        output_value_col="forecast_value",
        output_date_col="forecast_date",
    )
    merged = _merge_forecast_sources(
        df_pjm=pjm_norm,
        df_meteo=meteo_norm,
        mode=mode,
        blend_weight_pjm=blend_weight_pjm,
        value_col="forecast_value",
        date_col="forecast_date",
    )
    if merged is None or len(merged) == 0:
        return None

    day = merged[merged["forecast_date"] == target_date]
    if day.empty:
        return None
    return float(day["forecast_value"].mean())


def build_daily_features(
    schema: str = configs.SCHEMA,
    hub: str = configs.HUB,
    renewable_mode: str = configs.RENEWABLE_FORECAST_MODE,
    renewable_region: str = configs.RENEWABLE_FORECAST_REGION,
    renewable_blend_pjm_weight: float = configs.RENEWABLE_BLEND_PJM_WEIGHT_D1,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> pd.DataFrame:
    """Pull all data and build the daily feature matrix for similarity matching.

    Returns:
        DataFrame with one row per date, containing all feature columns.
    """
    renewable_mode = _normalize_renewable_mode(renewable_mode)
    renewable_blend_pjm_weight = _coerce_blend_weight(renewable_blend_pjm_weight)
    logger.info(
        f"Building daily similarity features from schema '{schema}' for hub '{hub}' "
        f"(renewables={renewable_mode}, region={renewable_region}, "
        f"pjm_weight={renewable_blend_pjm_weight:.2f})"
    )

    cache_kwargs = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    # --- 1. Pull data (cached) ---
    logger.info(f"Pulling DA LMP data ({hub})...")
    df_lmp_da = pull_with_cache(
        source_name="pjm_lmps_hourly_da",
        pull_fn=lmps_hourly.pull,
        pull_kwargs={"schema": schema, "market": "da"},
        **cache_kwargs,
    )
    df_lmp_da = df_lmp_da[df_lmp_da["hub"] == hub].copy()

    logger.info(f"Pulling RT LMP data ({hub})...")
    df_lmp_rt = pull_with_cache(
        source_name="pjm_lmps_hourly_rt",
        pull_fn=lmps_hourly.pull,
        pull_kwargs={"schema": schema, "market": "rt"},
        **cache_kwargs,
    )
    df_lmp_rt = df_lmp_rt[df_lmp_rt["hub"] == hub].copy()

    logger.info("Pulling hourly gas prices...")
    df_gas_hourly = None
    try:
        df_gas_hourly = pull_with_cache(
            source_name="ice_gas_prices_hourly",
            pull_fn=gas_prices_hourly.pull,
            pull_kwargs={},
            **cache_kwargs,
        )
    except Exception as e:
        logger.warning(f"Could not pull hourly gas prices: {e}")


    logger.info("Pulling calendar data...")
    df_dates = pull_with_cache(
        source_name="pjm_dates_daily",
        pull_fn=dates.pull_daily,
        pull_kwargs={"schema": schema},
        **cache_kwargs,
    )

    logger.info("Pulling RT metered load...")
    df_rt_load = pull_with_cache(
        source_name="pjm_load_rt_metered_hourly",
        pull_fn=load_rt_metered_hourly.pull,
        pull_kwargs={},
        **cache_kwargs,
    )

    logger.info("Pulling observed weather (PJM aggregate)...")
    df_weather = None
    try:
        df_weather = pull_with_cache(
            source_name="wsi_weather_hourly",
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
            source_name="pjm_fuel_mix_hourly",
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
            source_name="pjm_outages_actual_daily",
            pull_fn=outages_actual_daily.pull,
            pull_kwargs={"schema": schema},
            **cache_kwargs,
        )
    except Exception as e:
        logger.warning(f"Outages pull failed: {e}")

    logger.info(f"Pulling Meteologica solar forecast ({renewable_region})...")
    df_meteo_solar = None
    try:
        df_solar_vintages = pull_with_cache(
            source_name="meteologica_solar_forecast_vintages",
            pull_fn=solar_forecast_vintages.pull_meteologica_vintages,
            pull_kwargs={},
            **cache_kwargs,
        )
        df_meteo_solar = df_solar_vintages[
            (df_solar_vintages["vintage_label"] == "Latest")
            & (df_solar_vintages["region"] == renewable_region)
        ].copy()
        df_meteo_solar = df_meteo_solar.rename(columns={"forecast_mw": "forecast_generation_mw"})
    except Exception as e:
        logger.warning(f"Meteologica solar forecast pull failed: {e}")

    logger.info(f"Pulling Meteologica wind forecast ({renewable_region})...")
    df_meteo_wind = None
    try:
        df_wind_vintages = pull_with_cache(
            source_name="meteologica_wind_forecast_vintages",
            pull_fn=wind_forecast_vintages.pull_meteologica_vintages,
            pull_kwargs={},
            **cache_kwargs,
        )
        df_meteo_wind = df_wind_vintages[
            (df_wind_vintages["vintage_label"] == "Latest")
            & (df_wind_vintages["region"] == renewable_region)
        ].copy()
        df_meteo_wind = df_meteo_wind.rename(columns={"forecast_mw": "forecast_generation_mw"})
    except Exception as e:
        logger.warning(f"Meteologica wind forecast pull failed: {e}")

    # --- 2. Build feature modules ---
    logger.info("Building LMP features...")
    df_feat_lmp = lmp_features.build(df_lmp_da=df_lmp_da, df_lmp_rt=df_lmp_rt)

    logger.info("Building hourly gas features...")
    df_feat_gas = gas_hourly_features.build(df_gas_hourly=df_gas_hourly)

    logger.info("Building load features...")
    df_feat_load = load_features.build(df_rt_load=df_rt_load)

    logger.info("Building calendar features...")
    df_feat_cal = calendar_features.build(df_dates=df_dates)

    logger.info("Building weather features...")
    df_feat_weather = weather_features.build(df_weather=df_weather)

    logger.info("Building renewable features...")
    df_feat_renewable = renewable_features.build(df_fuel_mix=df_fuel_mix)

    logger.info("Building nuclear features...")
    df_feat_nuclear = nuclear_features.build(df_fuel_mix=df_fuel_mix)

    logger.info("Building congestion features...")
    df_feat_congestion = congestion_features.build(df_lmp_da=df_lmp_da)

    logger.info("Building fuel mix share features...")
    df_feat_fuel_mix_shares = fuel_mix_shares_features.build(df_fuel_mix=df_fuel_mix)

    logger.info("Building net load features...")
    df_feat_net_load = net_load_features.build(df_rt_load=df_rt_load, df_fuel_mix=df_fuel_mix)

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
    df_pjm_target_solar = _standardize_hourly_forecast(
        df=None,
        value_candidates=("solar_forecast",),
        output_value_col="solar_forecast",
        output_date_col="date",
    )
    df_meteo_target_solar = _standardize_hourly_forecast(
        df=df_meteo_solar,
        value_candidates=("forecast_generation_mw",),
        output_value_col="solar_forecast",
        output_date_col="date",
    )
    df_target_solar = _merge_forecast_sources(
        df_pjm=df_pjm_target_solar,
        df_meteo=df_meteo_target_solar,
        mode=renewable_mode,
        blend_weight_pjm=renewable_blend_pjm_weight,
        value_col="solar_forecast",
        date_col="date",
    )

    df_pjm_target_wind = _standardize_hourly_forecast(
        df=None,
        value_candidates=("wind_forecast",),
        output_value_col="wind_forecast",
        output_date_col="date",
    )
    df_meteo_target_wind = _standardize_hourly_forecast(
        df=df_meteo_wind,
        value_candidates=("forecast_generation_mw",),
        output_value_col="wind_forecast",
        output_date_col="date",
    )
    df_target_wind = _merge_forecast_sources(
        df_pjm=df_pjm_target_wind,
        df_meteo=df_meteo_target_wind,
        mode=renewable_mode,
        blend_weight_pjm=renewable_blend_pjm_weight,
        value_col="wind_forecast",
        date_col="date",
    )
    logger.info(
        "Target renewables source mode=%s (solar_rows=%s, wind_rows=%s)",
        renewable_mode,
        len(df_target_solar) if df_target_solar is not None else 0,
        len(df_target_wind) if df_target_wind is not None else 0,
    )

    df_feat_target_renewable = target_renewable_features.build(
        df_fuel_mix=df_fuel_mix,
        df_solar_forecast=df_target_solar,
        df_wind_forecast=df_target_wind,
        df_ref_renewable_features=df_feat_renewable,
    )

    logger.info("Pulling PJM outage forecast...")
    df_outage_forecast = None
    try:
        df_outage_forecast = pull_with_cache(
            source_name="pjm_outages_forecast_daily",
            pull_fn=outages_forecast_daily.pull,
            pull_kwargs={"lookback_days": 14},
            **cache_kwargs,
        )
    except Exception as e:
        logger.warning(f"Could not pull outage forecast: {e}")

    logger.info("Building target-date outage features (forecast + shifted actuals)...")
    df_feat_target_outage = target_outage_features.build(
        df_outages=df_outages,
        df_outage_forecast=df_outage_forecast,
        df_ref_outage_features=df_feat_outage,
    )

    logger.info("Pulling PJM load forecast vintages...")
    df_load_forecast = None
    try:
        df_load_forecast = pull_with_cache(
            source_name="pjm_load_forecast_vintages",
            pull_fn=load_forecast_vintages.pull_combined_vintages,
            pull_kwargs={"source": "pjm"},
            **cache_kwargs,
        )
    except Exception as e:
        logger.warning(f"Could not pull PJM load forecast: {e}")

    logger.info("Pulling Meteologica load forecast vintages...")
    df_meteo_load_forecast = None
    try:
        df_meteo_load_forecast = pull_with_cache(
            source_name="meteologica_load_forecast_vintages",
            pull_fn=load_forecast_vintages.pull_combined_vintages,
            pull_kwargs={"source": "meteologica"},
            **cache_kwargs,
        )
    except Exception as e:
        logger.warning(f"Could not pull Meteologica load forecast: {e}")

    logger.info("Building target-date load features (PJM + Meteo forecast + shifted actuals)...")
    df_feat_target_load = target_load_features.build(
        df_rt_load=df_rt_load,
        df_load_forecast=df_load_forecast,
        df_meteo_load_forecast=df_meteo_load_forecast,
        df_ref_load_features=df_feat_load,
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
    result = result.merge(df_feat_target_load, on="date", how="left")
    result = result.merge(df_feat_nuclear, on="date", how="left")
    result = result.merge(df_feat_congestion, on="date", how="left")
    result = result.merge(df_feat_fuel_mix_shares, on="date", how="left")
    result = result.merge(df_feat_net_load, on="date", how="left")

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
    df_pjm_solar_forecast: pd.DataFrame | None = None,
    df_pjm_wind_forecast: pd.DataFrame | None = None,
    df_meteo_solar_forecast: pd.DataFrame | None = None,
    df_meteo_wind_forecast: pd.DataFrame | None = None,
    renewable_mode: str = configs.RENEWABLE_FORECAST_MODE,
    renewable_blend_weight_pjm: float = configs.RENEWABLE_BLEND_PJM_WEIGHT_D1,
    df_outage_forecast: pd.DataFrame | None = None,
    df_load_forecast: pd.DataFrame | None = None,
) -> pd.Series:
    """Build a synthetic feature row for a future reference date.

    Clones today's actual features (LMP, gas, load, weather reference,
    composites, renewables, outage reference) and replaces:
      - Calendar columns → recomputed for ``target_date - 1``
      - Target-date columns (tgt_*) → from forecast data for ``target_date``

    This allows ``find_analogs()`` to run with correct DOW matching and
    forward-looking weather/renewable/outage/load signals for each strip day
    without modifying the engine.

    Args:
        df_features: Full daily feature matrix (from ``build_daily_features``).
        today: The actual current date (must exist in *df_features*).
        target_date: The forecast target date (D+N).
        df_weather: Hourly weather with forecast rows for *target_date*.
        df_pjm_solar_forecast: Hourly PJM solar forecast.
        df_pjm_wind_forecast: Hourly PJM wind forecast.
        df_meteo_solar_forecast: Hourly Meteologica solar forecast.
        df_meteo_wind_forecast: Hourly Meteologica wind forecast.
        renewable_mode: Forecast source mode: ``pjm``, ``meteologica``, ``blend``.
        renewable_blend_weight_pjm: PJM weight when mode is ``blend``.
        df_outage_forecast: Daily outage forecast (``forecast_date``, outage MW columns).
        df_load_forecast: Hourly DA load forecast (``forecast_date``, ``forecast_load_mw``).

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
    _apply_target_renewables(
        row=row,
        target_date=target_date,
        mode=renewable_mode,
        blend_weight_pjm=renewable_blend_weight_pjm,
        df_pjm_solar=df_pjm_solar_forecast,
        df_pjm_wind=df_pjm_wind_forecast,
        df_meteo_solar=df_meteo_solar_forecast,
        df_meteo_wind=df_meteo_wind_forecast,
    )

    # ── 4. Target outage features ───────────────────────────────────
    if df_outage_forecast is not None and len(df_outage_forecast):
        _apply_target_outages(row, df_outage_forecast, target_date)

    # ── 5. Target load features ──────────────────────────────────────
    if df_load_forecast is not None and len(df_load_forecast):
        _apply_target_loads(row, df_load_forecast, target_date)

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
    target_date: date,
    mode: str,
    blend_weight_pjm: float,
    df_pjm_solar: pd.DataFrame | None,
    df_pjm_wind: pd.DataFrame | None,
    df_meteo_solar: pd.DataFrame | None,
    df_meteo_wind: pd.DataFrame | None,
) -> None:
    """Replace tgt_renewable columns using configured forecast source(s)."""
    solar_avg = _resolve_daily_avg(
        df_pjm=df_pjm_solar,
        df_meteo=df_meteo_solar,
        target_date=target_date,
        mode=mode,
        blend_weight_pjm=blend_weight_pjm,
        pjm_value_candidates=("solar_forecast",),
        meteo_value_candidates=("forecast_generation_mw",),
    )
    wind_avg = _resolve_daily_avg(
        df_pjm=df_pjm_wind,
        df_meteo=df_meteo_wind,
        target_date=target_date,
        mode=mode,
        blend_weight_pjm=blend_weight_pjm,
        pjm_value_candidates=("wind_forecast",),
        meteo_value_candidates=("forecast_generation_mw",),
    )

    if solar_avg is not None:
        row["tgt_solar_daily_avg"] = solar_avg
    if wind_avg is not None:
        row["tgt_wind_daily_avg"] = wind_avg

    if solar_avg is None and wind_avg is None:
        logger.warning(
            f"No renewable forecast for {target_date} in mode={_normalize_renewable_mode(mode)}; "
            "keeping today's tgt_renewable values"
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


def _apply_target_loads(
    row: pd.Series,
    df_load_forecast: pd.DataFrame,
    target_date: date,
) -> None:
    """Replace tgt_load columns in *row* using DA load forecast for *target_date*."""
    lf = df_load_forecast.copy()
    date_col = "forecast_date" if "forecast_date" in lf.columns else "date"
    value_col = "forecast_load_mw" if "forecast_load_mw" in lf.columns else "da_load_mw"

    lf = lf[lf[date_col] == target_date]
    if lf.empty:
        logger.warning(
            f"No load forecast for {target_date}; keeping today's tgt_load values"
        )
        return

    avg_load = lf[value_col].mean()
    peak_load = lf[value_col].max()
    valley_load = lf[value_col].min()

    row["tgt_load_daily_avg"] = avg_load
    row["tgt_load_daily_peak"] = peak_load
    row["tgt_load_daily_valley"] = valley_load

    if avg_load and avg_load > 0:
        row["tgt_load_peak_ratio"] = peak_load / avg_load

    ramps = lf.sort_values("hour_ending")[value_col].diff()
    if not ramps.empty:
        row["tgt_load_ramp_max"] = ramps.max()

    if "load_daily_avg" in row.index and pd.notna(row["load_daily_avg"]):
        row["tgt_load_change_vs_ref"] = avg_load - row["load_daily_avg"]
