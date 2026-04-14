"""Multi-day strip forecast using LASSO Quantile Regression.

For D+1 uses today's features directly.  For D+2+ builds synthetic feature
rows by overwriting ``tgt_*`` columns with target-date forecast values, reusing
the like-day module's ``build_synthetic_reference_row()``.

Key difference from the like-day strip: the regression model **generalises**
from training data — it does not need matching analog days, so a 105 GW load
forecast maps to the learned price-load curve even if no recent analog had
that load level.
"""
from __future__ import annotations

import copy
import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd

from src.lasso_quantile_regression.configs import (
    HOURS,
    OFFPEAK_HOURS,
    ONPEAK_HOURS,
    LassoQRConfig,
)
from src.lasso_quantile_regression.features.builder import build_regression_features
from src.lasso_quantile_regression.training.trainer import (
    load_latest_model,
    train_models,
)
from src.like_day_forecast.features.builder import build_synthetic_reference_row
from src.like_day_forecast import configs as ld_configs
from src.data import (
    weather_hourly,
    pjm_solar_forecast_hourly,
    pjm_wind_forecast_hourly,
    solar_forecast_vintages,
    wind_forecast_vintages,
    outages_forecast_daily,
    pjm_load_forecast_hourly,
)
from src.utils.cache_utils import pull_with_cache

logger = logging.getLogger(__name__)

_Q_LABELS = {
    0.10: "P10", 0.25: "P25", 0.50: "P50", 0.75: "P75", 0.90: "P90",
}


def run_strip(
    horizon: int = 7,
    config: LassoQRConfig | None = None,
    **kwargs,
) -> dict:
    """Run D+1 through D+horizon strip forecast.

    Returns dict with strip_table, quantiles_table, reference_date,
    forecast_dates, per_day, model_info — matching the like-day strip contract.
    """
    if config is None:
        config = LassoQRConfig(**kwargs)

    ref_date = date.today()
    forecast_dates = [ref_date + timedelta(days=d) for d in range(1, horizon + 1)]

    logger.info(
        f"LASSO QR strip: {forecast_dates[0]} to {forecast_dates[-1]} "
        f"(ref: {ref_date})"
    )
    artifacts_by_day_type: dict[str, dict] = {}

    # Build full feature matrix (for reference row + actuals lookup).
    # Use a superset config so weekend profiles can request lagged/interaction
    # columns without rebuilding the matrix per day.
    feature_build_cfg = copy.deepcopy(config)
    feature_build_cfg.include_lagged_lmp = True
    feature_build_cfg.include_interaction_terms = True
    df, _ = build_regression_features(feature_build_cfg)

    # Pre-pull forecast data for synthetic reference rows
    cache_kw = dict(
        cache_dir=config.cache_dir,
        cache_enabled=config.cache_enabled,
        ttl_hours=config.cache_ttl_hours,
        force_refresh=config.force_refresh,
    )
    forecast_data = _pull_forecast_data(cache_kw)

    strip_rows: list[dict] = []
    quantile_rows: list[dict] = []
    per_day: dict[str, dict] = {}
    predicted_delivery_prices: dict[date, dict[int, float]] = {}

    for offset, target_date in enumerate(forecast_dates, start=1):
        day_cfg, day_type = config.with_day_type_overrides(target_date)
        if day_type not in artifacts_by_day_type:
            artifact = load_latest_model(day_cfg)
            if artifact is None:
                artifact = train_models(
                    day_cfg, reference_date=ref_date - timedelta(days=1),
                )
            artifacts_by_day_type[day_type] = {
                "artifact": artifact,
                "models": artifact["models"],
                "feature_cols": artifact["feature_columns"],
            }
        model_bundle = artifacts_by_day_type[day_type]
        models = model_bundle["models"]
        feature_cols: list[str] = model_bundle["feature_cols"]

        fd_str = str(target_date)
        synthetic_ref = target_date - timedelta(days=1)

        # Build feature vector for this target date
        if synthetic_ref == ref_date:
            ref_row = df[df["date"] == ref_date]
        else:
            ref_row = _build_synthetic_row(
                df, ref_date, target_date, day_cfg, forecast_data,
            )

        if ref_row is None or (isinstance(ref_row, pd.DataFrame) and len(ref_row) == 0):
            logger.warning(f"No features for {fd_str}, skipping")
            continue

        # If Series, convert to single-row DataFrame
        if isinstance(ref_row, pd.Series):
            ref_row = ref_row.to_frame().T

        ref_row = ref_row.copy()
        _refresh_lagged_lmp_features(
            ref_row=ref_row,
            target_date=target_date,
            predicted_delivery_prices=predicted_delivery_prices,
            df_features=df,
        )
        X_pred = _build_X(ref_row, feature_cols)

        # Predict all hours x quantiles (inverse-transform if asinh was used)
        use_asinh = model_bundle["artifact"].get("use_asinh_transform", False)
        forecasts: dict[int, dict[float, float]] = {}
        for h in HOURS:
            forecasts[h] = {}
            for q in config.quantiles:
                key = (h, q)
                if key in models:
                    raw = float(models[key].predict(X_pred)[0])
                    forecasts[h][q] = float(np.sinh(raw)) if use_asinh else raw
        _enforce_monotonic_quantiles(forecasts, config.quantiles)

        # Forecast row (median)
        fc_row: dict = {"Date": target_date, "Type": "Forecast"}
        for h in HOURS:
            fc_row[f"HE{h}"] = forecasts[h].get(0.50)
        _add_summary(fc_row)
        strip_rows.append(fc_row)
        predicted_delivery_prices[target_date] = {
            h: forecasts[h].get(0.50) for h in HOURS if forecasts[h].get(0.50) is not None
        }

        # Actual row
        has_actuals = False
        act_ref_date = target_date - timedelta(days=1)
        act_data = df[df["date"] == act_ref_date]
        if len(act_data) > 0:
            act_row: dict = {"Date": target_date, "Type": "Actual"}
            all_present = True
            for h in HOURS:
                col = f"target_HE{h}"
                val = act_data[col].iloc[0] if col in act_data.columns else None
                if val is None or (isinstance(val, float) and np.isnan(val)):
                    all_present = False
                    act_row[f"HE{h}"] = None
                else:
                    act_row[f"HE{h}"] = float(val)
            if all_present:
                has_actuals = True
                _add_summary(act_row)
                strip_rows.append(act_row)

        # Quantile rows
        for q in config.quantiles:
            label = _Q_LABELS.get(q, f"P{int(q * 100):02d}")
            q_row: dict = {"Date": target_date, "Type": label}
            for h in HOURS:
                q_row[f"HE{h}"] = forecasts[h].get(q)
            _add_summary(q_row)
            quantile_rows.append(q_row)

        # Per-hour forecast DataFrame (matches like-day per_day contract)
        forecast_rows = []
        for h in HOURS:
            row_data = {"hour_ending": h, "point_forecast": forecasts[h].get(0.50)}
            for q in config.quantiles:
                row_data[f"q_{q:.2f}"] = forecasts[h].get(q)
            forecast_rows.append(row_data)

        per_day[fd_str] = {
            "df_forecast": pd.DataFrame(forecast_rows),
            "offset": offset,
            "day_type": day_type,
            "has_actuals": has_actuals,
        }

    cols = ["Date", "Type"] + [f"HE{h}" for h in HOURS] + ["OnPeak", "OffPeak", "Flat"]
    strip_table = pd.DataFrame(strip_rows, columns=cols)
    quantiles_table = pd.DataFrame(quantile_rows, columns=cols)

    _print_strip_table(strip_table)
    _print_strip_quantiles(quantiles_table)

    return {
        "strip_table": strip_table,
        "quantiles_table": quantiles_table,
        "reference_date": str(ref_date),
        "forecast_dates": [str(d) for d in forecast_dates],
        "per_day": per_day,
        "model_info": {
            "day_type_models": {
                k: {
                    "alpha": v["artifact"]["alpha"],
                    "n_train_samples": v["artifact"]["n_samples"],
                    "train_end": str(v["artifact"]["train_end"]),
                    "n_features": len(v["feature_cols"]),
                }
                for k, v in artifacts_by_day_type.items()
            },
        },
    }


# ── Helpers ────────────────────────────────────────────────────────


def _build_X(ref_row: pd.DataFrame, feature_cols: list[str]) -> np.ndarray:
    X = np.zeros((1, len(feature_cols)))
    for i, col in enumerate(feature_cols):
        if col in ref_row.columns:
            val = ref_row[col].iloc[0]
            X[0, i] = val if pd.notna(val) else 0.0
    return X


def _enforce_monotonic_quantiles(
    forecasts: dict[int, dict[float, float]],
    quantiles: list[float],
) -> None:
    """Apply monotonic rearrangement so lower quantiles never exceed higher ones."""
    q_sorted = sorted(quantiles)
    for h in HOURS:
        if h not in forecasts:
            continue
        preds = [forecasts[h].get(q) for q in q_sorted]
        if any(v is None for v in preds):
            continue
        monotone = np.maximum.accumulate(np.array(preds, dtype=float))
        for i, q in enumerate(q_sorted):
            forecasts[h][q] = float(monotone[i])


def _historical_delivery_lmp(
    df_features: pd.DataFrame,
    delivery_date: date,
    hour_ending: int,
) -> float | None:
    """Get delivery-day DA LMP from shifted target columns.

    Delivery day ``d`` is stored at reference row ``date=d-1``.
    """
    ref_date = delivery_date - timedelta(days=1)
    row = df_features[df_features["date"] == ref_date]
    if len(row) == 0:
        return None
    val = row.iloc[0].get(f"target_HE{hour_ending}")
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    return float(val)


def _refresh_lagged_lmp_features(
    ref_row: pd.DataFrame,
    target_date: date,
    predicted_delivery_prices: dict[date, dict[int, float]],
    df_features: pd.DataFrame,
) -> None:
    """Refresh lagged LMP predictors for strip horizons.

    For each lag feature (1,2,7), populate from already-predicted delivery days
    when available; otherwise fall back to historical realized delivery prices.
    """
    if len(ref_row) == 0:
        return

    idx = ref_row.index[0]
    for lag in (1, 2, 7):
        lagged_delivery = target_date - timedelta(days=lag)
        for h in HOURS:
            col = f"lmp_lag{lag}_HE{h}"
            if col not in ref_row.columns:
                continue

            val = predicted_delivery_prices.get(lagged_delivery, {}).get(h)
            if val is None:
                val = _historical_delivery_lmp(df_features, lagged_delivery, h)
            if val is not None:
                ref_row.at[idx, col] = float(val)


def _period_avg(row: dict, hours: list[int]) -> float | None:
    vals = [row.get(f"HE{h}") for h in hours]
    vals = [v for v in vals if v is not None and not (isinstance(v, float) and np.isnan(v))]
    return round(float(np.mean(vals)), 2) if vals else None


def _add_summary(row: dict) -> dict:
    row["OnPeak"] = _period_avg(row, ONPEAK_HOURS)
    row["OffPeak"] = _period_avg(row, OFFPEAK_HOURS)
    row["Flat"] = _period_avg(row, HOURS)
    return row


def _pull_forecast_data(cache_kw: dict) -> dict:
    """Pre-pull all forecast data sources needed for synthetic reference rows."""
    data: dict = {}
    try:
        data["weather"] = pull_with_cache(
            source_name="wsi_weather_hourly",
            pull_fn=weather_hourly.pull,
            pull_kwargs={},
            **cache_kw,
        )
    except Exception:
        data["weather"] = None

    try:
        data["pjm_solar"] = pull_with_cache(
            source_name="pjm_solar_forecast_hourly",
            pull_fn=pjm_solar_forecast_hourly.pull,
            pull_kwargs={},
            **cache_kw,
        )
    except Exception:
        data["pjm_solar"] = None

    try:
        data["pjm_wind"] = pull_with_cache(
            source_name="pjm_wind_forecast_hourly",
            pull_fn=pjm_wind_forecast_hourly.pull,
            pull_kwargs={},
            **cache_kw,
        )
    except Exception:
        data["pjm_wind"] = None

    try:
        df_solar_vintages = pull_with_cache(
            source_name="meteologica_solar_forecast_vintages",
            pull_fn=solar_forecast_vintages.pull_meteologica_vintages,
            pull_kwargs={},
            **cache_kw,
        )
        data["meteo_solar"] = df_solar_vintages[
            (df_solar_vintages["vintage_label"] == "Latest")
            & (df_solar_vintages["region"] == ld_configs.RENEWABLE_FORECAST_REGION)
        ].copy()
        data["meteo_solar"] = data["meteo_solar"].rename(
            columns={"forecast_mw": "forecast_generation_mw"},
        )
    except Exception:
        data["meteo_solar"] = None

    try:
        df_wind_vintages = pull_with_cache(
            source_name="meteologica_wind_forecast_vintages",
            pull_fn=wind_forecast_vintages.pull_meteologica_vintages,
            pull_kwargs={},
            **cache_kw,
        )
        data["meteo_wind"] = df_wind_vintages[
            (df_wind_vintages["vintage_label"] == "Latest")
            & (df_wind_vintages["region"] == ld_configs.RENEWABLE_FORECAST_REGION)
        ].copy()
        data["meteo_wind"] = data["meteo_wind"].rename(
            columns={"forecast_mw": "forecast_generation_mw"},
        )
    except Exception:
        data["meteo_wind"] = None

    try:
        data["outage_forecast"] = pull_with_cache(
            source_name="pjm_outages_forecast_daily",
            pull_fn=outages_forecast_daily.pull,
            pull_kwargs={"lookback_days": 14},
            **cache_kw,
        )
    except Exception:
        data["outage_forecast"] = None

    try:
        data["load_forecast"] = pull_with_cache(
            source_name="pjm_load_forecast_hourly",
            pull_fn=pjm_load_forecast_hourly.pull,
            pull_kwargs={},
            **cache_kw,
        )
    except Exception:
        data["load_forecast"] = None

    return data


def _build_synthetic_row(
    df: pd.DataFrame,
    ref_date: date,
    target_date: date,
    config: LassoQRConfig,
    forecast_data: dict,
) -> pd.DataFrame | None:
    """Build synthetic feature row for D+2+ by cloning today and
    injecting target-date forecasts.  Falls back to today's row on failure.
    """
    try:
        offset = (target_date - ref_date).days
        blend_weight = min(0.50 + (offset - 1) * 0.25 / 6, 0.75)

        row = build_synthetic_reference_row(
            df_features=df,
            today=ref_date,
            target_date=target_date,
            df_weather=forecast_data.get("weather"),
            df_pjm_solar_forecast=forecast_data.get("pjm_solar"),
            df_pjm_wind_forecast=forecast_data.get("pjm_wind"),
            df_meteo_solar_forecast=forecast_data.get("meteo_solar"),
            df_meteo_wind_forecast=forecast_data.get("meteo_wind"),
            renewable_mode=ld_configs.RENEWABLE_FORECAST_MODE,
            renewable_blend_weight_pjm=blend_weight,
            df_outage_forecast=forecast_data.get("outage_forecast"),
            df_load_forecast=forecast_data.get("load_forecast"),
        )
        if isinstance(row, pd.Series):
            return row.to_frame().T
        return row
    except Exception as e:
        logger.warning(f"Synthetic row failed for {target_date}: {e}")
        ref_row = df[df["date"] == ref_date]
        return ref_row if len(ref_row) > 0 else None


# ── Display helpers ──────────────────────────────────────────────────────

DAY_ABBR = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}


def _print_strip_table(table: pd.DataFrame) -> None:
    """Print the combined strip forecast table."""
    print("\n" + "=" * 130)
    print("  DA LMP LASSO QR STRIP FORECAST — Western Hub ($/MWh)")
    print("=" * 130)

    header = f"{'Date':<12} {'Type':<10}"
    for h in range(1, 25):
        header += f" {h:>6}"
    header += f" {'OnPk':>7} {'OffPk':>7} {'Flat':>7}"
    print(header)
    print("-" * len(header))

    prev_date = None
    for _, row in table.iterrows():
        if prev_date is not None and row["Date"] != prev_date:
            print("-" * len(header))
        prev_date = row["Date"]

        line = f"{str(row['Date']):<12} {row['Type']:<10}"
        for h in range(1, 25):
            val = row[f"HE{h}"]
            line += f" {val:>6.1f}" if pd.notna(val) else f" {'':>6}"
        line += f" {row['OnPeak']:>7.2f}" if pd.notna(row["OnPeak"]) else f" {'':>7}"
        line += f" {row['OffPeak']:>7.2f}" if pd.notna(row["OffPeak"]) else f" {'':>7}"
        line += f" {row['Flat']:>7.2f}" if pd.notna(row["Flat"]) else f" {'':>7}"
        print(line)

    print("=" * 130 + "\n")


def _print_strip_quantiles(table: pd.DataFrame) -> None:
    """Print quantile bands for all strip days."""
    if table.empty:
        return

    print("  Quantile Bands ($/MWh)")
    print("-" * 100)

    header = f"{'Date':<12} {'Band':<10}"
    for h in range(1, 25):
        header += f" {h:>6}"
    header += f" {'OnPk':>7} {'OffPk':>7} {'Flat':>7}"
    print(header)
    print("-" * len(header))

    prev_date = None
    for _, row in table.iterrows():
        if prev_date is not None and row["Date"] != prev_date:
            print("-" * len(header))
        prev_date = row["Date"]

        line = f"{str(row['Date']):<12} {row['Type']:<10}"
        for h in range(1, 25):
            val = row[f"HE{h}"]
            line += f" {val:>6.1f}" if pd.notna(val) else f" {'':>6}"
        line += f" {row['OnPeak']:>7.2f}" if pd.notna(row["OnPeak"]) else f" {'':>7}"
        line += f" {row['OffPeak']:>7.2f}" if pd.notna(row["OffPeak"]) else f" {'':>7}"
        line += f" {row['Flat']:>7.2f}" if pd.notna(row["Flat"]) else f" {'':>7}"
        print(line)

    print("-" * len(header) + "\n")


if __name__ == "__main__":
    import src.settings  # noqa: F401

    logging.basicConfig(level=logging.INFO)

    today = date.today()
    wd = today.weekday()
    if wd <= 3:
        horizon = 4 - wd
    elif wd == 4:
        horizon = 5
    else:
        horizon = 5

    result = run_strip(horizon=horizon)
    if "error" in result:
        print(f"ERROR: {result['error']}")
