"""Single-day LightGBM quantile regression forecast."""
from __future__ import annotations

import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd
from colorama import Fore, Style, init as colorama_init

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

from src.lightgbm_quantile.configs import (
    HOURS,
    OFFPEAK_HOURS,
    ONPEAK_HOURS,
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
    period_avg,
)

logger = logging.getLogger(__name__)

_Q_LABELS = {
    0.01: "P01", 0.05: "P05", 0.10: "P10", 0.25: "P25", 0.375: "P37.5",
    0.50: "P50", 0.625: "P62.5", 0.75: "P75", 0.90: "P90", 0.95: "P95", 0.99: "P99",
}


def run(
    config: LGBMQRConfig | None = None,
    **kwargs,
) -> dict:
    """Run single-day LightGBM quantile forecast."""
    if config is None:
        config = LGBMQRConfig(**kwargs)

    forecast_date = _resolve_forecast_date(config)
    config, day_type = config.with_day_type_overrides(forecast_date)
    reference_date = forecast_date - timedelta(days=1)

    logger.info(
        "LGBM QR forecast for %s (ref: %s, profile=%s)",
        forecast_date,
        reference_date,
        day_type,
    )

    artifact = load_latest_model(config)
    if artifact is None:
        logger.info("No fresh model found, training...")
        artifact = train_models(config, reference_date=reference_date)

    models = artifact["models"]
    feature_cols: list[str] = artifact["feature_columns"]

    df, _ = build_regression_features(config)

    ref_row = df[df["date"] == reference_date]
    if len(ref_row) == 0:
        return {"error": f"No feature data for reference date {reference_date}"}

    X_pred = build_X(ref_row, feature_cols, feature_medians=artifact.get("feature_medians"))

    forecasts = _predict_all(models, X_pred, config.quantiles, vst=artifact.get("vst"))
    enforce_monotonic_quantiles(forecasts, config.quantiles)

    output_table = _build_output_table(df, forecast_date, forecasts)
    quantiles_table = _build_quantiles_table(forecast_date, forecasts, config.quantiles)

    # Insert Forecast row into quantile bands after P50
    fc_rows = output_table[output_table["Type"] == "Forecast"].iloc[0:1].copy()
    p50_idx = quantiles_table[quantiles_table["Type"] == "P50"].index
    if len(fc_rows) > 0 and len(p50_idx) > 0:
        pos = p50_idx[0] + 1
        quantiles_table = pd.concat([
            quantiles_table.iloc[:pos], fc_rows, quantiles_table.iloc[pos:],
        ]).reset_index(drop=True)

    has_actuals = "Actual" in output_table["Type"].values

    metrics = _compute_metrics(output_table, quantiles_table) if has_actuals else None

    feature_importances = _extract_importances(models, feature_cols, config.quantiles)
    shap_importances = _extract_shap_importances(
        models=models,
        feature_cols=feature_cols,
        quantiles=config.quantiles,
        X_pred=X_pred,
    )

    model_info = {
        "best_params": artifact.get("best_params", {}),
        "hyperparam_search": artifact.get("hyperparam_search"),
        "n_train_samples": artifact["n_samples"],
        "train_start": str(artifact["train_start"]),
        "train_end": str(artifact["train_end"]),
        "trained_at": artifact["trained_at"],
        "n_features": len(feature_cols),
        "day_type": day_type,
        "feature_importances": feature_importances,
    }
    if shap_importances:
        model_info["shap_importances"] = shap_importances

    return {
        "output_table": output_table,
        "quantiles_table": quantiles_table,
        "forecast_date": str(forecast_date),
        "reference_date": str(reference_date),
        "has_actuals": has_actuals,
        "metrics": metrics,
        "model_info": model_info,
    }


def _resolve_forecast_date(config: LGBMQRConfig) -> date:
    if config.forecast_date:
        return pd.to_datetime(config.forecast_date).date()
    return date.today() + timedelta(days=1)


def _predict_all(
    models: dict,
    X: np.ndarray,
    quantiles: list[float],
    vst: str | None = None,
) -> dict[int, dict[float, float]]:
    """Predict all hours x quantiles -> nested dict."""
    forecasts: dict[int, dict[float, float]] = {}
    for h in HOURS:
        forecasts[h] = {}
        for q in quantiles:
            key = (h, q)
            if key in models:
                pred = float(models[key].predict(X)[0])
                if vst == "arcsinh":
                    pred = float(np.sinh(pred))
                forecasts[h][q] = pred
    return forecasts


def _build_output_table(
    df: pd.DataFrame,
    forecast_date: date,
    forecasts: dict,
) -> pd.DataFrame:
    rows = []

    act_ref_date = forecast_date - timedelta(days=1)
    act = df[df["date"] == act_ref_date]
    if len(act) > 0:
        act_row: dict = {"Date": forecast_date, "Type": "Actual"}
        all_present = True
        for h in HOURS:
            col = f"target_HE{h}"
            val = act[col].iloc[0] if col in act.columns else None
            if val is None or (isinstance(val, float) and np.isnan(val)):
                all_present = False
                act_row[f"HE{h}"] = None
            else:
                act_row[f"HE{h}"] = float(val)
        if all_present:
            rows.append(add_summary(act_row))

    fc_row: dict = {"Date": forecast_date, "Type": "Forecast"}
    for h in HOURS:
        fc_row[f"HE{h}"] = expected_value_from_quantiles(forecasts[h]) or forecasts[h].get(0.50)
    rows.append(add_summary(fc_row))

    if rows and rows[0]["Type"] == "Actual":
        err_row: dict = {"Date": forecast_date, "Type": "Error"}
        for h in HOURS:
            a = rows[0].get(f"HE{h}")
            f = fc_row.get(f"HE{h}")
            err_row[f"HE{h}"] = round(f - a, 2) if (a is not None and f is not None) else None
        rows.append(add_summary(err_row))

    cols = ["Date", "Type"] + [f"HE{h}" for h in HOURS] + ["OnPeak", "OffPeak", "Flat"]
    return pd.DataFrame(rows, columns=cols)


def _build_quantiles_table(
    forecast_date: date,
    forecasts: dict,
    quantiles: list[float],
) -> pd.DataFrame:
    rows = []
    for q in quantiles:
        label = _Q_LABELS.get(q, f"P{int(q * 100):02d}")
        row: dict = {"Date": forecast_date, "Type": label}
        for h in HOURS:
            row[f"HE{h}"] = forecasts[h].get(q)
        rows.append(add_summary(row))

    cols = ["Date", "Type"] + [f"HE{h}" for h in HOURS] + ["OnPeak", "OffPeak", "Flat"]
    return pd.DataFrame(rows, columns=cols)


def _coverage(
    quantiles_table: pd.DataFrame,
    act: pd.Series,
    lo_label: str,
    hi_label: str,
) -> float | None:
    """Fraction of hours where actual falls within [lo_label, hi_label] band."""
    lo = quantiles_table[quantiles_table["Type"] == lo_label]
    hi = quantiles_table[quantiles_table["Type"] == hi_label]
    if len(lo) == 0 or len(hi) == 0:
        return None
    lo_r, hi_r = lo.iloc[0], hi.iloc[0]
    in_range = sum(
        1
        for h in HOURS
        if (
            lo_r[f"HE{h}"] is not None
            and hi_r[f"HE{h}"] is not None
            and act[f"HE{h}"] is not None
            and lo_r[f"HE{h}"] <= act[f"HE{h}"] <= hi_r[f"HE{h}"]
        )
    )
    return in_range / 24


def _sharpness(
    quantiles_table: pd.DataFrame,
    lo_label: str,
    hi_label: str,
) -> float | None:
    """Average width of the [lo_label, hi_label] prediction interval."""
    lo = quantiles_table[quantiles_table["Type"] == lo_label]
    hi = quantiles_table[quantiles_table["Type"] == hi_label]
    if len(lo) == 0 or len(hi) == 0:
        return None
    lo_r, hi_r = lo.iloc[0], hi.iloc[0]
    widths = []
    for h in HOURS:
        l_val, h_val = lo_r[f"HE{h}"], hi_r[f"HE{h}"]
        if l_val is not None and h_val is not None:
            widths.append(h_val - l_val)
    return float(np.mean(widths)) if widths else None


def _compute_metrics(
    output_table: pd.DataFrame,
    quantiles_table: pd.DataFrame,
) -> dict:
    fc = output_table[output_table["Type"] == "Forecast"].iloc[0]
    act = output_table[output_table["Type"] == "Actual"].iloc[0]

    errors = []
    actuals = []
    for h in HOURS:
        f_val, a_val = fc[f"HE{h}"], act[f"HE{h}"]
        if f_val is not None and a_val is not None:
            errors.append(f_val - a_val)
            actuals.append(a_val)

    errors_arr = np.array(errors)
    actuals_arr = np.array(actuals)
    mae = float(np.mean(np.abs(errors_arr)))
    rmse = float(np.sqrt(np.mean(errors_arr**2)))
    mape = float(np.mean(np.abs(errors_arr) / np.maximum(np.abs(actuals_arr), 1.0)) * 100)

    return {
        "mae": mae,
        "rmse": rmse,
        "mape": mape,
        "coverage_80pct": _coverage(quantiles_table, act, "P10", "P90"),
        "coverage_90pct": _coverage(quantiles_table, act, "P05", "P95"),
        "coverage_98pct": _coverage(quantiles_table, act, "P01", "P99"),
        "sharpness_90pct": _sharpness(quantiles_table, "P05", "P95"),
    }


def _extract_importances(
    models: dict,
    feature_cols: list[str],
    quantiles: list[float],
) -> list[dict]:
    if 0.50 not in quantiles:
        return []

    gain_sum = np.zeros(len(feature_cols), dtype=float)
    n_models = 0
    for h in HOURS:
        key = (h, 0.50)
        if key not in models:
            continue
        gain = models[key].booster_.feature_importance(importance_type="gain")
        gain_sum += gain
        n_models += 1

    if n_models == 0:
        return []

    avg_gain = gain_sum / n_models
    ranked = sorted(zip(feature_cols, avg_gain), key=lambda x: -x[1])
    return [{"feature": n, "importance": round(float(v), 4)} for n, v in ranked[:15]]


def _extract_shap_importances(
    models: dict,
    feature_cols: list[str],
    quantiles: list[float],
    X_pred: np.ndarray,
) -> list[dict]:
    """Compute average absolute SHAP values for P50 models when shap is installed."""
    if 0.50 not in quantiles:
        return []

    try:
        import shap  # type: ignore
    except Exception:
        return []

    shap_sum = np.zeros(len(feature_cols), dtype=float)
    n_models = 0
    for h in HOURS:
        key = (h, 0.50)
        if key not in models:
            continue
        try:
            explainer = shap.TreeExplainer(models[key])
            shap_values = explainer.shap_values(X_pred)
            if isinstance(shap_values, list):
                shap_values = shap_values[0]
            shap_sum += np.abs(np.asarray(shap_values)[0])
            n_models += 1
        except Exception:
            continue

    if n_models == 0:
        return []

    avg_abs_shap = shap_sum / n_models
    ranked = sorted(zip(feature_cols, avg_abs_shap), key=lambda x: -x[1])
    return [{"feature": n, "importance": round(float(v), 4)} for n, v in ranked[:15]]


DAY_ABBR = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}


def _print_model_info(result: dict) -> None:
    """Print model training summary."""
    mi = result.get("model_info", {})
    best = mi.get("best_params", {})
    print("\n" + "=" * 90)
    print("  LIGHTGBM QUANTILE REGRESSION - MODEL INFO")
    print("=" * 90)
    print(
        "  Params: "
        f"n_estimators={best.get('n_estimators', '?')}, "
        f"max_depth={best.get('max_depth', '?')}, "
        f"learning_rate={best.get('learning_rate', '?')}  |  "
        f"Features: {mi.get('n_features', '?')}  |  "
        f"Train samples: {mi.get('n_train_samples', '?')}"
    )
    print(
        f"  Train window: {mi.get('train_start', '?')} to {mi.get('train_end', '?')} "
        f"| Day-type: {mi.get('day_type', '?')}"
    )
    importances = mi.get("feature_importances", [])
    if importances:
        print("\n  Top Features (by gain):")
        for i, f in enumerate(importances[:10], 1):
            print(f"    {i:>2}. {f['feature']:<35s} {f['importance']:.4f}")
    print()


def _print_table(table: pd.DataFrame, metrics: dict | None) -> None:
    """Print the Actual/Forecast/Error table."""
    print("=" * 120)
    print("  DA LMP LIGHTGBM QR FORECAST - Western Hub ($/MWh)")
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
        line += f" {row['OnPeak']:>7.2f}" if pd.notna(row["OnPeak"]) else f" {'':>7}"
        line += f" {row['OffPeak']:>7.2f}" if pd.notna(row["OffPeak"]) else f" {'':>7}"
        line += f" {row['Flat']:>7.2f}" if pd.notna(row["Flat"]) else f" {'':>7}"
        style = _ROW_STYLES.get(row["Type"])
        if style:
            line = f"{style}{line}{_RS}"
        print(line)

    print("-" * len(header))

    if metrics:
        print(
            f"  MAE: ${metrics.get('mae', 0):.2f}/MWh  |  "
            f"RMSE: ${metrics.get('rmse', 0):.2f}/MWh  |  "
            f"MAPE: {metrics.get('mape', 0):.1f}%"
        )
        cov80 = metrics.get("coverage_80pct")
        if cov80 is not None:
            print(f"  Coverage: 80%PI={cov80:.0%}")

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
        line += f" {row['OnPeak']:>7.2f}" if pd.notna(row["OnPeak"]) else f" {'':>7}"
        line += f" {row['OffPeak']:>7.2f}" if pd.notna(row["OffPeak"]) else f" {'':>7}"
        line += f" {row['Flat']:>7.2f}" if pd.notna(row["Flat"]) else f" {'':>7}"
        style = _ROW_STYLES.get(row["Type"])
        if style:
            line = f"{style}{line}{_RS}"
        print(line)

    print("-" * len(header) + "\n")


if __name__ == "__main__":
    import src.settings  # noqa: F401

    logging.basicConfig(level=logging.INFO)

    result = run(config=LGBMQRConfig())
    if "error" in result:
        print(f"ERROR: {result['error']}")
    else:
        forecast_date = pd.to_datetime(result["forecast_date"]).date()
        reference_date = pd.to_datetime(result["reference_date"]).date()
        target_dow = DAY_ABBR[forecast_date.weekday()]
        ref_dow = DAY_ABBR[reference_date.weekday()]

        print("\n" + "=" * 90)
        print("  LIGHTGBM QR FORECAST")
        print(
            f"  Forecast: {forecast_date} ({target_dow})  |  "
            f"Reference: {reference_date} ({ref_dow})  |  Hub: Western Hub"
        )
        print("=" * 90)

        _print_model_info(result)
        _print_table(result["output_table"], result.get("metrics"))
        _print_quantiles(result["quantiles_table"])
