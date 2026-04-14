"""Multi-day strip forecast using LightGBM quantile regression."""
from __future__ import annotations

import copy
import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd

from src.lightgbm_quantile.configs import (
    HOURS,
    LGBMQRConfig,
)
from src.lightgbm_quantile.features.builder import build_regression_features
from src.lightgbm_quantile.training.trainer import (
    load_latest_model,
    train_models,
)
from src.lightgbm_quantile.utils import (
    add_summary,
    build_X,
    enforce_monotonic_quantiles,
    expected_value_from_quantiles,
)
from src.like_day_forecast.features.builder import build_synthetic_reference_row
from src.like_day_forecast import configs as ld_configs
from src.data import (
    outages_forecast_daily,
    pjm_load_forecast_hourly,
    pjm_solar_forecast_hourly,
    pjm_wind_forecast_hourly,
    solar_forecast_vintages,
    weather_hourly,
    wind_forecast_vintages,
)
from src.utils.cache_utils import pull_with_cache

logger = logging.getLogger(__name__)

_Q_LABELS = {
    0.01: "P01",
    0.05: "P05",
    0.10: "P10",
    0.25: "P25",
    0.50: "P50",
    0.75: "P75",
    0.90: "P90",
    0.95: "P95",
    0.99: "P99",
}


def run_strip(
    horizon: int = 7,
    config: LGBMQRConfig | None = None,
    **kwargs,
) -> dict:
    """Run D+1 through D+horizon strip forecast."""
    if config is None:
        config = LGBMQRConfig(**kwargs)

    ref_date = date.today()
    forecast_dates = [ref_date + timedelta(days=d) for d in range(1, horizon + 1)]

    logger.info(
        "LGBM QR strip: %s to %s (ref: %s)",
        forecast_dates[0],
        forecast_dates[-1],
        ref_date,
    )
    artifacts_by_day_type: dict[str, dict] = {}

    feature_build_cfg = copy.deepcopy(config)
    feature_build_cfg.include_lagged_lmp = True
    feature_build_cfg.include_interaction_terms = True
    df, _ = build_regression_features(feature_build_cfg)

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
                    day_cfg,
                    reference_date=ref_date - timedelta(days=1),
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

        if synthetic_ref == ref_date:
            ref_row = df[df["date"] == ref_date]
        else:
            ref_row = _build_synthetic_row(
                df=df,
                ref_date=ref_date,
                target_date=target_date,
                config=day_cfg,
                forecast_data=forecast_data,
            )

        if ref_row is None or (isinstance(ref_row, pd.DataFrame) and len(ref_row) == 0):
            logger.warning("No features for %s, skipping", fd_str)
            continue

        if isinstance(ref_row, pd.Series):
            ref_row = ref_row.to_frame().T

        ref_row = ref_row.copy()
        _refresh_lagged_lmp_features(
            ref_row=ref_row,
            target_date=target_date,
            predicted_delivery_prices=predicted_delivery_prices,
            df_features=df,
        )
        feature_medians = model_bundle["artifact"].get("feature_medians")
        X_pred = build_X(ref_row, feature_cols, feature_medians=feature_medians)

        vst = model_bundle["artifact"].get("vst")
        forecasts: dict[int, dict[float, float]] = {}
        for h in HOURS:
            forecasts[h] = {}
            for q in config.quantiles:
                key = (h, q)
                if key in models:
                    pred = float(models[key].predict(X_pred)[0])
                    if vst == "arcsinh":
                        pred = float(np.sinh(pred))
                    forecasts[h][q] = pred
        enforce_monotonic_quantiles(forecasts, config.quantiles)

        fc_row: dict = {"Date": target_date, "Type": "Forecast"}
        for h in HOURS:
            fc_row[f"HE{h}"] = expected_value_from_quantiles(forecasts[h]) or forecasts[h].get(0.50)
        add_summary(fc_row)
        strip_rows.append(fc_row)
        predicted_delivery_prices[target_date] = {
            h: expected_value_from_quantiles(forecasts[h]) or forecasts[h].get(0.50)
            for h in HOURS
            if forecasts[h]
        }

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
                add_summary(act_row)
                strip_rows.append(act_row)

        for q in config.quantiles:
            label = _Q_LABELS.get(q, f"P{int(q * 100):02d}")
            q_row: dict = {"Date": target_date, "Type": label}
            for h in HOURS:
                q_row[f"HE{h}"] = forecasts[h].get(q)
            add_summary(q_row)
            quantile_rows.append(q_row)

        # Per-hour forecast DataFrame (matches like-day per_day contract)
        forecast_rows = []
        for h in HOURS:
            row_data = {"hour_ending": h, "point_forecast": expected_value_from_quantiles(forecasts[h]) or forecasts[h].get(0.50)}
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
                    "best_params": v["artifact"].get("best_params", {}),
                    "hyperparam_search": v["artifact"].get("hyperparam_search"),
                    "n_train_samples": v["artifact"]["n_samples"],
                    "train_end": str(v["artifact"]["train_end"]),
                    "n_features": len(v["feature_cols"]),
                }
                for k, v in artifacts_by_day_type.items()
            },
        },
    }


def _historical_delivery_lmp(
    df_features: pd.DataFrame,
    delivery_date: date,
    hour_ending: int,
) -> float | None:
    """Get delivery-day DA LMP from shifted target columns."""
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
    """Refresh lagged LMP predictors for strip horizons."""
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
    config: LGBMQRConfig,
    forecast_data: dict,
) -> pd.DataFrame | None:
    """Build synthetic feature row for D+2+."""
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
        logger.warning("Synthetic row failed for %s: %s", target_date, e)
        ref_row = df[df["date"] == ref_date]
        return ref_row if len(ref_row) > 0 else None


# ── Display helpers ──────────────────────────────────────────────────────

DAY_ABBR = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}


def _print_strip_table(table: pd.DataFrame) -> None:
    """Print the combined strip forecast table."""
    print("\n" + "=" * 130)
    print("  DA LMP LIGHTGBM QR STRIP FORECAST — Western Hub ($/MWh)")
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
