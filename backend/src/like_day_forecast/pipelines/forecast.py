"""Like-day forecast pipeline — predict DA LMP for a target date.

Finds analog days based on the reference date's features, then builds a
probabilistic forecast from those analogs' next-day actual DA LMP profiles.

Output table format (matches da-model for comparability):
  Date | Type | HE1 | HE2 | ... | HE24 | OnPeak | OffPeak | Flat
"""
import logging
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from colorama import Fore, Style, init as colorama_init
from tabulate import tabulate

colorama_init()
_HL_FORECAST = Style.BRIGHT + Fore.RED
_HL_QUARTILE = Fore.CYAN                  # P25 / P75
_HL_INNER = Fore.YELLOW                   # P37.5 / P62.5
_RS = Style.RESET_ALL
_ROW_STYLES = {
    "Forecast": _HL_FORECAST,
    "P25": _HL_QUARTILE, "P75": _HL_QUARTILE,
    "P37.5": _HL_INNER, "P62.5": _HL_INNER,
}

from src.like_day_forecast import configs
from src.like_day_forecast.features.builder import build_daily_features
from src.like_day_forecast.similarity.engine import find_analogs
from src.data import lmps_hourly, ice_power_intraday
from src.like_day_forecast.evaluation.metrics import evaluate_forecast
from src.utils.cache_utils import pull_with_cache

logger = logging.getLogger(__name__)

ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = list(range(1, 8)) + [24]
DAY_ABBR = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}


def weighted_quantile(values: np.ndarray, weights: np.ndarray, q: float) -> float:
    """Compute weighted quantile using linear interpolation."""
    sorted_idx = np.argsort(values)
    sorted_values = values[sorted_idx]
    sorted_weights = weights[sorted_idx]
    cumulative = np.cumsum(sorted_weights)
    cumulative /= cumulative[-1]
    return float(np.interp(q, cumulative, sorted_values))


def _add_summary_cols(row_dict: dict) -> dict:
    """Add OnPeak, OffPeak, Flat averages to an hourly row dict."""
    onpeak_vals = [row_dict.get(f"HE{h}") for h in ONPEAK_HOURS if row_dict.get(f"HE{h}") is not None]
    offpeak_vals = [row_dict.get(f"HE{h}") for h in OFFPEAK_HOURS if row_dict.get(f"HE{h}") is not None]
    all_vals = [row_dict.get(f"HE{h}") for h in range(1, 25) if row_dict.get(f"HE{h}") is not None]

    row_dict["OnPeak"] = np.mean(onpeak_vals) if onpeak_vals else np.nan
    row_dict["OffPeak"] = np.mean(offpeak_vals) if offpeak_vals else np.nan
    row_dict["Flat"] = np.mean(all_vals) if all_vals else np.nan
    return row_dict


def _next_business_day(d: date) -> date:
    """Return the next business day after *d* (skip weekends)."""
    dt = d + timedelta(days=1)
    while dt.weekday() >= 5:
        dt += timedelta(days=1)
    return dt


def _get_ice_forward_price(
    target_date: date,
    pull_cache_kwargs: dict,
) -> float | None:
    """Get ICE NxtDay DA on-peak price scalar for a delivery date.

    Returns settle when available; falls back to WAP.
    """
    session = _get_ice_session_info(target_date, pull_cache_kwargs)
    if session is None:
        return None
    return session["ice_onpeak_price"]


def _get_ice_session_info(
    target_date: date,
    pull_cache_kwargs: dict,
) -> dict | None:
    """Get latest ICE NxtDay DA session context for a delivery date."""
    ice_cache_kwargs = {k: v for k, v in pull_cache_kwargs.items() if k != "ttl_hours"}
    try:
        df_settles = pull_with_cache(
            source_name="ice_power_settles",
            pull_fn=ice_power_intraday.pull_settles,
            pull_kwargs={"lookback_days": 7},
            ttl_hours=0.083,
            **ice_cache_kwargs,
        )
    except Exception as e:
        logger.warning("Could not pull ICE settles for ICE context: %s", e)
        return None

    if df_settles is None or len(df_settles) == 0:
        return None

    df_da = df_settles[df_settles["product"] == "NxtDay DA"].copy()
    if len(df_da) == 0:
        return None

    # Compute delivery date for each trade_date (D1 = next business day)
    df_da["trade_date"] = pd.to_datetime(df_da["trade_date"]).dt.date
    df_da["delivery_date"] = df_da["trade_date"].apply(_next_business_day)

    df_match = df_da[df_da["delivery_date"] == target_date]
    if len(df_match) == 0:
        logger.warning("No ICE NxtDay DA settle for delivery %s", target_date)
        return None

    latest = df_match.sort_values("trade_date", ascending=False).iloc[0]
    settle = latest.get("settle")
    vwap = latest.get("vwap")
    high = latest.get("high")
    low = latest.get("low")

    settle_value = float(settle) if pd.notna(settle) else None
    wap_value = float(vwap) if pd.notna(vwap) else None
    high_value = float(high) if pd.notna(high) else None
    low_value = float(low) if pd.notna(low) else None

    ice_onpeak_price = settle_value if settle_value is not None else wap_value
    if ice_onpeak_price is None:
        return None

    return {
        "trade_date": latest.get("trade_date"),
        "delivery_date": latest.get("delivery_date"),
        "ice_onpeak_price": ice_onpeak_price,
        "settle": settle_value,
        "high": high_value,
        "low": low_value,
        "wap": wap_value,
    }


def _format_price(value: float | None) -> str:
    """Format optional ICE price for display."""
    if value is None:
        return "n/a"
    return f"${value:.2f}/MWh"


def _quantile_label(q: float) -> str:
    """Format quantile label for table output (e.g., P90, P37.5)."""
    q_pct = q * 100
    if float(q_pct).is_integer():
        return f"P{int(q_pct):02d}"
    return f"P{q_pct:.1f}".rstrip("0").rstrip(".")


def _apply_ice_level_adjustment(
    df_forecast: pd.DataFrame,
    ice_onpeak_price: float,
) -> pd.DataFrame:
    """Scale on-peak forecast hours to match ICE forward price level.

    Preserves the hourly shape from like-day analogs.  On-peak hours
    (HE8-23) are uniformly scaled so their average matches the ICE
    NxtDay DA settlement.  Off-peak hours are unchanged (no off-peak
    ICE product available).
    """
    onpeak_mask = df_forecast["hour_ending"].isin(ONPEAK_HOURS)
    current_onpeak = df_forecast.loc[onpeak_mask, "point_forecast"].mean()

    if current_onpeak <= 0 or np.isnan(current_onpeak):
        logger.warning("Cannot apply ICE level adjustment: on-peak avg is zero/NaN")
        return df_forecast

    scale = ice_onpeak_price / current_onpeak
    logger.info(
        "ICE level adjustment: scaling on-peak by %.3f "
        "(like-day=%.2f → ICE=%.2f)",
        scale, current_onpeak, ice_onpeak_price,
    )

    df_out = df_forecast.copy()
    q_cols = [c for c in df_out.columns if c.startswith("q_")]
    scale_cols = ["point_forecast"] + q_cols
    df_out.loc[onpeak_mask, scale_cols] *= scale
    return df_out


def run(
    forecast_date: str | None = None,
    n_analogs: int = configs.DEFAULT_N_ANALOGS,
    weight_method: str = "inverse_distance",
    config: configs.ScenarioConfig | None = None,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
    df_features: pd.DataFrame | None = None,
) -> dict:
    """Run the like-day forecast pipeline for a single target date.

    Approach:
    1. Build daily feature matrix from database
    2. Use the day BEFORE forecast_date as the reference for analog matching
    3. Find N most similar historical days to the reference date
    4. For each analog, get the NEXT day's actual DA LMP hourly profile
    5. Build weighted probabilistic forecast from those next-day profiles
    6. Compare against actual DA LMP for the forecast date (if available)

    Args:
        forecast_date: Date to forecast (YYYY-MM-DD). Defaults to tomorrow.
        n_analogs: Number of analog days to find.
        weight_method: Weighting method for analogs.
        config: Optional ScenarioConfig that overrides all other args.
        cache_dir: Directory for parquet cache files.
        cache_enabled: Master cache switch.
        cache_ttl_hours: Hours before cached data is considered stale.
        force_refresh: If True, bypass cache and pull fresh.

    Returns:
        Dict with output_table, quantiles_table, analogs, metrics.
    """
    # Build config from keyword args if not provided (backward compat)
    if config is None:
        config = configs.ScenarioConfig(
            forecast_date=forecast_date,
            n_analogs=n_analogs,
            weight_method=weight_method,
        )

    if config.forecast_date is None:
        target_date = date.today() + timedelta(days=1)
    else:
        target_date = pd.to_datetime(config.forecast_date).date()
    config, day_type = config.with_day_type_overrides(target_date)
    reference_date = target_date - timedelta(days=1)

    logger.info("=" * 60)
    logger.info(f"Like-Day Forecast: DA LMP for {target_date} — {config.hub}")
    logger.info(f"Reference date (analog matching): {reference_date}")
    logger.info(f"Day-type profile: {day_type}")
    logger.info(f"Config: n_analogs={config.n_analogs} weight_method={config.weight_method} "
                f"season_window={config.season_window_days} dow_filter={config.same_dow_group}")
    logger.info("=" * 60)

    builder_cache_kwargs = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        cache_ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )
    pull_cache_kwargs = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    # 1. Build daily feature matrix (skip if pre-built matrix provided)
    if df_features is None:
        logger.info("Building daily feature matrix...")
        df_features = build_daily_features(
            schema=config.schema,
            hub=config.hub,
            renewable_mode=config.resolved_renewable_mode(),
            renewable_region=config.renewable_forecast_region,
            renewable_blend_pjm_weight=config.renewable_blend_weight(offset=1),
            include_ice_forward=config.include_ice_forward,
            **builder_cache_kwargs,
        )
    else:
        logger.info("Using pre-built feature matrix (%d rows)", len(df_features))

    available_dates = sorted(df_features["date"].unique())
    logger.info(f"Feature matrix: {len(available_dates):,} days "
                f"({available_dates[0]} to {available_dates[-1]})")

    if reference_date not in available_dates:
        logger.error(f"Reference date {reference_date} not in feature matrix. "
                     f"Latest available: {available_dates[-1]}")
        return {"error": f"Reference date {reference_date} not available"}

    # 2. Find analog days for the reference date
    logger.info(f"Finding {config.n_analogs} analogs for {reference_date}...")
    # Parse exclude_dates strings to date objects
    parsed_exclude_dates = []
    for d in config.exclude_dates:
        parsed_exclude_dates.append(pd.to_datetime(d).date())

    # NOTE: reference_date is D-1 (e.g. Friday for Saturday delivery).
    # We pass delivery_date=target_date so that calendar filtering (DOW
    # group matching and season-window centering) uses the delivery day's
    # characteristics — ensuring Saturday forecasts match Saturday analogs,
    # not Friday analogs.
    analogs_df = find_analogs(
        target_date=reference_date,
        df_features=df_features,
        n_analogs=config.n_analogs,
        feature_weights=config.resolved_weights(),
        apply_calendar_filter=config.apply_calendar_filter,
        apply_regime_filter=config.apply_regime_filter,
        apply_outage_regime_filter=config.apply_outage_regime_filter,
        outage_tolerance_std=config.outage_tolerance_std,
        season_window_days=config.season_window_days,
        same_dow_group=config.same_dow_group,
        weight_method=config.weight_method,
        adaptive_filter_enabled=config.adaptive_filter_enabled,
        adaptive_extreme_threshold_std=config.adaptive_extreme_threshold_std,
        adaptive_season_window_days=config.adaptive_season_window_days,
        adaptive_same_dow_group=config.adaptive_same_dow_group,
        adaptive_lmp_tolerance_std=config.adaptive_lmp_tolerance_std,
        adaptive_gas_tolerance_std=config.adaptive_gas_tolerance_std,
        adaptive_n_analogs=config.adaptive_n_analogs,
        adaptive_weight_method=config.adaptive_weight_method,
        adaptive_softmax_temperature=config.adaptive_softmax_temperature,
        exclude_holidays=config.exclude_holidays,
        exclude_dates=parsed_exclude_dates,
        delivery_date=target_date,
    )

    # 3. Pull raw hourly DA LMP data (cache hit if builder already cached it)
    logger.info("Pulling hourly DA LMP data for analog next-days...")
    df_lmp_all = pull_with_cache(
        source_name="pjm_lmps_hourly_da",
        pull_fn=lmps_hourly.pull,
        pull_kwargs={"schema": config.schema, "market": "da"},
        **pull_cache_kwargs,
    )
    df_lmp_all = df_lmp_all[df_lmp_all["hub"] == config.hub].copy()
    # Safety: parquet round-trip may convert date to datetime64
    df_lmp_all["date"] = pd.to_datetime(df_lmp_all["date"]).dt.date

    # For each analog date, we want the NEXT DAY's hourly LMP profile
    analog_next_dates = [d + timedelta(days=1) for d in analogs_df["date"]]
    df_analog_next_lmps = df_lmp_all[df_lmp_all["date"].isin(analog_next_dates)].copy()

    # Map next-day back to the analog date for weight lookup
    df_analog_next_lmps["analog_date"] = df_analog_next_lmps["date"] - timedelta(days=1)
    df_analog_next_lmps = df_analog_next_lmps.merge(
        analogs_df[["date", "weight", "rank", "distance"]],
        left_on="analog_date", right_on="date", suffixes=("", "_analog"),
    )

    # Check how many analogs have next-day data
    n_with_data = df_analog_next_lmps["analog_date"].nunique()
    logger.info(f"{n_with_data}/{len(analogs_df)} analogs have next-day LMP data")

    if n_with_data == 0:
        logger.error("No next-day LMP data found for any analog day")
        return {"error": "No next-day LMP data for analogs"}

    # 4. Build probabilistic forecast — weighted quantiles by hour
    forecast_rows = []
    for h in configs.HOURS:
        hour_data = df_analog_next_lmps[df_analog_next_lmps["hour_ending"] == h]
        if len(hour_data) == 0:
            continue

        values = hour_data["lmp_total"].values
        weights = hour_data["weight"].values

        # Re-normalize weights for available analogs
        weights = weights / weights.sum()

        # Weighted point forecast
        point = np.average(values, weights=weights)

        row = {"hour_ending": h, "point_forecast": point}

        # Weighted quantiles
        for q in config.quantiles:
            row[f"q_{q:.2f}"] = weighted_quantile(values, weights, q)

        forecast_rows.append(row)

    df_forecast = pd.DataFrame(forecast_rows)

    # 4b. Apply ICE level adjustment (optional)
    ice_session_info: dict | None = None
    ice_info: dict | None = None
    if config.ice_level_adjustment:
        ice_session_info = _get_ice_session_info(target_date, pull_cache_kwargs)
        ice_price = (
            ice_session_info["ice_onpeak_price"]
            if ice_session_info is not None
            else None
        )
        if ice_price is not None:
            onpeak_before = df_forecast.loc[
                df_forecast["hour_ending"].isin(ONPEAK_HOURS), "point_forecast"
            ].mean()
            df_forecast = _apply_ice_level_adjustment(df_forecast, ice_price)
            ice_info = {
                "ice_onpeak_price": ice_price,
                "ice_settle": (
                    ice_session_info["settle"] if ice_session_info is not None else None
                ),
                "ice_high": (
                    ice_session_info["high"] if ice_session_info is not None else None
                ),
                "ice_low": (
                    ice_session_info["low"] if ice_session_info is not None else None
                ),
                "ice_wap": (
                    ice_session_info["wap"] if ice_session_info is not None else None
                ),
                "onpeak_before_adjustment": round(onpeak_before, 2),
                "scale_factor": round(ice_price / onpeak_before, 4)
                if onpeak_before > 0 else None,
            }
        else:
            logger.warning(
                "ICE level adjustment enabled but no ICE price found for %s",
                target_date,
            )

    # 5. Build pivoted output table
    forecast_hourly = dict(zip(df_forecast["hour_ending"].astype(int), df_forecast["point_forecast"]))

    # Pull actuals for the forecast date
    actuals_hourly = None
    df_actuals = df_lmp_all[df_lmp_all["date"] == target_date].sort_values("hour_ending")
    has_actuals = len(df_actuals) >= 24

    if has_actuals:
        actuals_hourly = dict(zip(df_actuals["hour_ending"].astype(int), df_actuals["lmp_total"]))
        logger.info(f"Actual DA LMP available for {target_date}")
    else:
        logger.warning(f"No actual DA LMP data for {target_date}")

    output_table = _build_output_table(target_date, forecast_hourly, actuals_hourly)

    # Build quantile table
    quantile_rows = []
    for q in config.quantiles:
        col = f"q_{q:.2f}"
        if col in df_forecast.columns:
            q_row = {"Date": target_date, "Type": _quantile_label(q)}
            q_hourly = dict(zip(df_forecast["hour_ending"].astype(int), df_forecast[col]))
            for h in range(1, 25):
                q_row[f"HE{h}"] = q_hourly.get(h)
            q_row = _add_summary_cols(q_row)
            quantile_rows.append(q_row)

    q_cols = ["Date", "Type"] + [f"HE{h}" for h in range(1, 25)] + ["OnPeak", "OffPeak", "Flat"]
    quantiles_table = pd.DataFrame(quantile_rows, columns=q_cols)

    # Insert Forecast row into quantile bands after P50
    fc_rows = output_table[output_table["Type"] == "Forecast"].iloc[0:1].copy()
    p50_idx = quantiles_table[quantiles_table["Type"] == "P50"].index
    if len(fc_rows) > 0 and len(p50_idx) > 0:
        pos = p50_idx[0] + 1
        quantiles_table = pd.concat([
            quantiles_table.iloc[:pos], fc_rows, quantiles_table.iloc[pos:],
        ]).reset_index(drop=True)

    # 6. Evaluate forecast against actuals
    metrics = None
    if has_actuals:
        y_true = df_actuals.sort_values("hour_ending")["lmp_total"].values

        # Naive forecast = same day last week
        naive_date = target_date - timedelta(days=7)
        df_naive = df_lmp_all[df_lmp_all["date"] == naive_date].sort_values("hour_ending")
        y_naive = df_naive["lmp_total"].values if len(df_naive) >= 24 else None

        metrics = evaluate_forecast(
            y_true=y_true,
            y_pred_df=df_forecast,
            quantiles=config.quantiles,
            y_naive=y_naive,
        )

    # 6b. Print ICE forward market context (any ICE scenario)
    if config.include_ice_forward or config.ice_level_adjustment:
        if ice_session_info is None:
            ice_session_info = _get_ice_session_info(target_date, pull_cache_kwargs)

        if ice_session_info is not None:
            ice_display_price = ice_session_info["ice_onpeak_price"]
            print(f"\n  ICE NxtDay DA On-Peak for {target_date}: "
                  f"${ice_display_price:.2f}/MWh")
            print(
                "    Session: "
                f"settle={_format_price(ice_session_info['settle'])}  "
                f"high={_format_price(ice_session_info['high'])}  "
                f"low={_format_price(ice_session_info['low'])}  "
                f"WAP={_format_price(ice_session_info['wap'])}"
            )
        else:
            print(f"\n  ICE NxtDay DA: no settlement found for {target_date}")

    # 7. Print results
    _print_config(config, target_date, reference_date, day_type)
    _print_analogs(analogs_df, target_date, reference_date)
    _print_table(output_table, metrics)
    _print_quantiles(quantiles_table)

    result = {
        "output_table": output_table,
        "quantiles_table": quantiles_table,
        "analogs": analogs_df,
        "metrics": metrics,
        "forecast_date": str(target_date),
        "reference_date": str(reference_date),
        "day_type": day_type,
        "has_actuals": has_actuals,
        "n_analogs_used": n_with_data,
        "df_forecast": df_forecast,
        "scenario": config.name,
        "include_ice_forward": config.include_ice_forward,
        "ice_level_adjustment": config.ice_level_adjustment,
    }
    if ice_info is not None:
        result["ice_info"] = ice_info
    return result


def _build_output_table(
    target_date: date,
    forecast_hourly: dict[int, float],
    actuals_hourly: dict[int, float] | None = None,
) -> pd.DataFrame:
    """Build the pivoted output table with Actual/Forecast rows."""
    rows = []

    if actuals_hourly is not None:
        actual_row = {"Date": target_date, "Type": "Actual"}
        for h in range(1, 25):
            actual_row[f"HE{h}"] = actuals_hourly.get(h)
        actual_row = _add_summary_cols(actual_row)
        rows.append(actual_row)

    forecast_row = {"Date": target_date, "Type": "Forecast"}
    for h in range(1, 25):
        forecast_row[f"HE{h}"] = forecast_hourly.get(h)
    forecast_row = _add_summary_cols(forecast_row)
    rows.append(forecast_row)

    if actuals_hourly is not None:
        error_row = {"Date": target_date, "Type": "Error"}
        for h in range(1, 25):
            a = actuals_hourly.get(h)
            f = forecast_hourly.get(h)
            error_row[f"HE{h}"] = (f - a) if (a is not None and f is not None) else None
        error_row = _add_summary_cols(error_row)
        rows.append(error_row)

    cols = ["Date", "Type"] + [f"HE{h}" for h in range(1, 25)] + ["OnPeak", "OffPeak", "Flat"]
    return pd.DataFrame(rows, columns=cols)


def _print_config(
    config: configs.ScenarioConfig,
    target_date: date,
    reference_date: date,
    day_type: str,
) -> None:
    """Print the resolved forecast configuration."""
    target_dow = DAY_ABBR[target_date.weekday()]
    ref_dow = DAY_ABBR[reference_date.weekday()]
    weights = config.resolved_weights()

    # Season window date range
    from datetime import timedelta as _td
    window = config.season_window_days
    win_start = target_date - _td(days=window)
    win_end = target_date + _td(days=window)

    print("\n" + "=" * 90)
    print("  FORECAST CONFIGURATION")
    print("=" * 90)

    print(f"\n  Target        {target_date} ({target_dow})")
    print(f"  Reference     {reference_date} ({ref_dow})")
    print(f"  Day-type      {day_type}")
    print(f"  Hub           {config.hub}")
    print(f"  Scenario      {config.name}")

    print(f"\n  --- Analog Selection {'-' * 28}")
    print(f"  N analogs          {config.n_analogs}")
    print(f"  Weight method      {config.weight_method}")

    print(f"\n  --- Pre-Filtering {'-' * 30}")
    print(f"  Season window      +/-{window}d  ({win_start.strftime('%b %d')} - {win_end.strftime('%b %d')})")
    print(f"  DOW group filter   {config.same_dow_group}")
    print(f"  Calendar filter    {config.apply_calendar_filter}")
    print(f"  Regime filter      {config.apply_regime_filter}")
    print(f"  Outage regime      {config.apply_outage_regime_filter}  (tol={config.outage_tolerance_std} std)")
    print(f"  Exclude holidays   {config.exclude_holidays}")
    if config.exclude_dates:
        print(f"  Exclude dates      {', '.join(config.exclude_dates)}")

    print(f"\n  --- Adaptive Filter {'-' * 28}")
    print(f"  Enabled            {config.adaptive_filter_enabled}")
    if config.adaptive_filter_enabled:
        print(f"  Extreme threshold  {config.adaptive_extreme_threshold_std} std")
        print(f"  Adaptive window    +/-{config.adaptive_season_window_days}d")
        print(f"  Adaptive DOW       {config.adaptive_same_dow_group}")
        print(f"  Adaptive method    {config.adaptive_weight_method} (T={config.adaptive_softmax_temperature})")

    # Feature weights — group by category, skip zero-weight groups
    active = {k: v for k, v in sorted(weights.items()) if v > 0}
    disabled = [k for k, v in sorted(weights.items()) if v == 0]

    print(f"\n  --- Feature Weights (active) {'-' * 19}")
    # Print in descending weight order for quick scanning
    for name, w in sorted(active.items(), key=lambda x: -x[1]):
        bar = "#" * int(w * 4)
        print(f"  {name:<32} {w:>5.2f}  {bar}")

    if disabled:
        print(f"\n  --- Disabled Groups {'-' * 28}")
        print(f"  {', '.join(disabled)}")

    print("\n" + "=" * 90)


def _print_analogs(analogs_df: pd.DataFrame, target_date: date, reference_date: date) -> None:
    """Print the top analog days table."""
    target_dow = DAY_ABBR[target_date.weekday()]
    ref_dow = DAY_ABBR[reference_date.weekday()]
    print("\n" + "=" * 90)
    print(f"  LIKE-DAY ANALOG DAYS")
    print(
        f"  Forecast: {target_date} ({target_dow})  |  "
        f"Reference: {reference_date} ({ref_dow})  |  Hub: Western Hub"
    )
    print("=" * 90)

    display = analogs_df.head(configs.DEFAULT_N_DISPLAY).copy()
    display["date"] = pd.to_datetime(display["date"]).dt.strftime("%a %b-%d %Y")
    display["distance"] = display["distance"].map("{:.4f}".format)
    display["similarity"] = display["similarity"].map("{:.2%}".format)
    display["weight"] = display["weight"].map("{:.4f}".format)

    print(tabulate(display, headers="keys", tablefmt="simple", showindex=False))
    print(f"\n  Total analogs: {len(analogs_df)} | "
          f"Top-5 weight sum: {analogs_df.head(5)['weight'].sum():.2%} | "
          f"Distance range: {analogs_df['distance'].min():.4f} — {analogs_df['distance'].max():.4f}")


def _print_table(table: pd.DataFrame, metrics: dict | None) -> None:
    """Print the Actual/Forecast/Error table."""
    print("\n" + "=" * 120)
    print("  DA LMP LIKE-DAY FORECAST — Western Hub ($/MWh)")
    print("=" * 120)

    header = f"{'Date':<12} {'Type':<10}"
    for h in range(1, 25):
        header += f" {h:>6}"
    header += f" {'OnPk':>7} {'OffPk':>7} {'Flat':>7}"
    print(header)
    print("-" * len(header))

    for _, row in table.iterrows():
        line = f"{str(row['Date']):<12} {row['Type']:<10}"
        for h in range(1, 25):
            val = row[f"HE{h}"]
            line += f" {val:>6.1f}" if pd.notna(val) else f" {'':>6}"
        line += f" {row['OnPeak']:>7.2f}" if pd.notna(row['OnPeak']) else f" {'':>7}"
        line += f" {row['OffPeak']:>7.2f}" if pd.notna(row['OffPeak']) else f" {'':>7}"
        line += f" {row['Flat']:>7.2f}" if pd.notna(row['Flat']) else f" {'':>7}"
        style = _ROW_STYLES.get(row["Type"])
        if style:
            line = f"{style}{line}{_RS}"
        print(line)

    print("-" * len(header))

    if metrics:
        print(f"  MAE: ${metrics.get('mae', 0):.2f}/MWh  |  RMSE: ${metrics.get('rmse', 0):.2f}/MWh"
              f"  |  MAPE: {metrics.get('mape', 0):.1f}%")
        if "rmae" in metrics:
            print(f"  rMAE vs naive (last week): {metrics['rmae']:.3f} "
                  f"({'better' if metrics['rmae'] < 1 else 'worse'} than naive)")
        cov80 = metrics.get("coverage_80pct")
        cov90 = metrics.get("coverage_90pct")
        cov98 = metrics.get("coverage_98pct")
        parts = []
        if cov80 is not None:
            parts.append(f"80%PI={cov80:.0%}")
        if cov90 is not None:
            parts.append(f"90%PI={cov90:.0%}")
        if cov98 is not None:
            parts.append(f"98%PI={cov98:.0%}")
        if parts:
            print(f"  Coverage: {' | '.join(parts)}")
        sharp90 = metrics.get("sharpness_90pct")
        if sharp90 is not None:
            print(f"  Sharpness (90%PI width): ${sharp90:.2f}/MWh")
        if "mean_pinball" in metrics:
            print(f"  Mean Pinball Loss: {metrics['mean_pinball']:.4f}")
        if "crps" in metrics:
            print(f"  CRPS: {metrics['crps']:.4f}")

    print("=" * 120 + "\n")


def _print_quantiles(table: pd.DataFrame) -> None:
    """Print the quantile band table."""
    print("  Quantile Bands ($/MWh)")
    print("-" * 100)

    header = f"{'Date':<12} {'Band':<10}"
    for h in range(1, 25):
        header += f" {h:>6}"
    header += f" {'OnPk':>7} {'OffPk':>7} {'Flat':>7}"
    print(header)
    print("-" * len(header))

    for _, row in table.iterrows():
        line = f"{str(row['Date']):<12} {row['Type']:<10}"
        for h in range(1, 25):
            val = row[f"HE{h}"]
            line += f" {val:>6.1f}" if pd.notna(val) else f" {'':>6}"
        line += f" {row['OnPeak']:>7.2f}" if pd.notna(row['OnPeak']) else f" {'':>7}"
        line += f" {row['OffPeak']:>7.2f}" if pd.notna(row['OffPeak']) else f" {'':>7}"
        line += f" {row['Flat']:>7.2f}" if pd.notna(row['Flat']) else f" {'':>7}"
        style = _ROW_STYLES.get(row["Type"])
        if style:
            line = f"{style}{line}{_RS}"
        print(line)

    print("-" * len(header) + "\n")


def main():
    """Entry point — initialize settings and run single-day forecast."""
    import src.like_day_forecast.settings
    run()


if __name__ == "__main__":
    main()
