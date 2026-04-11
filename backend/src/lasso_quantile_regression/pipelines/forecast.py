"""Single-day LASSO Quantile Regression forecast.

Follows the same output contract as ``like_day_forecast/pipelines/forecast.py``
so existing view models and API patterns can be reused.
"""
from __future__ import annotations

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

logger = logging.getLogger(__name__)

_Q_LABELS = {
    0.01: "P01", 0.05: "P05", 0.10: "P10", 0.25: "P25", 0.50: "P50",
    0.75: "P75", 0.90: "P90", 0.95: "P95", 0.99: "P99",
}


def run(
    config: LassoQRConfig | None = None,
    **kwargs,
) -> dict:
    """Run single-day LASSO QR forecast.

    Returns dict matching the like-day forecast output contract:
        output_table, quantiles_table, forecast_date, reference_date,
        has_actuals, metrics, model_info.
    """
    if config is None:
        config = LassoQRConfig(**kwargs)

    forecast_date = _resolve_forecast_date(config)
    config, day_type = config.with_day_type_overrides(forecast_date)
    reference_date = forecast_date - timedelta(days=1)

    logger.info(
        f"LASSO QR forecast for {forecast_date} (ref: {reference_date}, profile={day_type})"
    )

    # 1. Load or train model
    artifact = load_latest_model(config)
    if artifact is None:
        logger.info("No fresh model found, training...")
        artifact = train_models(config, reference_date=reference_date)

    models = artifact["models"]
    feature_cols: list[str] = artifact["feature_columns"]

    # 2. Build features
    df, _ = build_regression_features(config)

    ref_row = df[df["date"] == reference_date]
    if len(ref_row) == 0:
        return {"error": f"No feature data for reference date {reference_date}"}

    X_pred = _build_X(ref_row, feature_cols)

    # 3. Generate forecasts
    forecasts = _predict_all(models, X_pred, config.quantiles)
    _enforce_monotonic_quantiles(forecasts, config.quantiles)

    # 4. Build output tables
    output_table = _build_output_table(df, forecast_date, forecasts)
    quantiles_table = _build_quantiles_table(forecast_date, forecasts, config.quantiles)
    has_actuals = "Actual" in output_table["Type"].values

    # 5. Metrics
    metrics = _compute_metrics(output_table, quantiles_table) if has_actuals else None

    # 6. Feature importances
    feature_importances = _extract_importances(models, feature_cols, config.quantiles)

    return {
        "output_table": output_table,
        "quantiles_table": quantiles_table,
        "forecast_date": str(forecast_date),
        "reference_date": str(reference_date),
        "has_actuals": has_actuals,
        "metrics": metrics,
        "model_info": {
            "alpha": artifact["alpha"],
            "n_train_samples": artifact["n_samples"],
            "train_start": str(artifact["train_start"]),
            "train_end": str(artifact["train_end"]),
            "trained_at": artifact["trained_at"],
            "n_features": len(feature_cols),
            "day_type": day_type,
            "feature_importances": feature_importances,
        },
    }


# ── Helpers ────────────────────────────────────────────────────────


def _resolve_forecast_date(config: LassoQRConfig) -> date:
    if config.forecast_date:
        return pd.to_datetime(config.forecast_date).date()
    return date.today() + timedelta(days=1)


def _build_X(ref_row: pd.DataFrame, feature_cols: list[str]) -> np.ndarray:
    """Extract a (1, n_features) array from a single-row DataFrame."""
    X = np.zeros((1, len(feature_cols)))
    for i, col in enumerate(feature_cols):
        if col in ref_row.columns:
            val = ref_row[col].iloc[0]
            X[0, i] = val if pd.notna(val) else 0.0
    return X


def _predict_all(
    models: dict, X: np.ndarray, quantiles: list[float],
) -> dict[int, dict[float, float]]:
    """Predict all hours x quantiles → nested dict."""
    forecasts: dict[int, dict[float, float]] = {}
    for h in HOURS:
        forecasts[h] = {}
        for q in quantiles:
            key = (h, q)
            if key in models:
                forecasts[h][q] = float(models[key].predict(X)[0])
    return forecasts


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


def _period_avg(row: dict, hours: list[int]) -> float | None:
    vals = [row.get(f"HE{h}") for h in hours]
    vals = [v for v in vals if v is not None and not (isinstance(v, float) and np.isnan(v))]
    return round(float(np.mean(vals)), 2) if vals else None


def _add_summary(row: dict) -> dict:
    row["OnPeak"] = _period_avg(row, ONPEAK_HOURS)
    row["OffPeak"] = _period_avg(row, OFFPEAK_HOURS)
    row["Flat"] = _period_avg(row, HOURS)
    return row


def _build_output_table(
    df: pd.DataFrame, forecast_date: date, forecasts: dict,
) -> pd.DataFrame:
    rows = []

    # Actual row: delivery day = forecast_date is indexed at reference date D=forecast_date-1.
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
            rows.append(_add_summary(act_row))

    # Forecast row (median)
    fc_row: dict = {"Date": forecast_date, "Type": "Forecast"}
    for h in HOURS:
        fc_row[f"HE{h}"] = forecasts[h].get(0.50)
    rows.append(_add_summary(fc_row))

    # Error row
    if rows and rows[0]["Type"] == "Actual":
        err_row: dict = {"Date": forecast_date, "Type": "Error"}
        for h in HOURS:
            a = rows[0].get(f"HE{h}")
            f = fc_row.get(f"HE{h}")
            err_row[f"HE{h}"] = round(f - a, 2) if (a is not None and f is not None) else None
        rows.append(_add_summary(err_row))

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
        rows.append(_add_summary(row))

    cols = ["Date", "Type"] + [f"HE{h}" for h in HOURS] + ["OnPeak", "OffPeak", "Flat"]
    return pd.DataFrame(rows, columns=cols)


def _compute_metrics(
    output_table: pd.DataFrame, quantiles_table: pd.DataFrame,
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
    rmse = float(np.sqrt(np.mean(errors_arr ** 2)))
    mape = float(np.mean(np.abs(errors_arr) / np.maximum(np.abs(actuals_arr), 1.0)) * 100)

    # Coverage (P10-P90)
    coverage_80 = None
    p10 = quantiles_table[quantiles_table["Type"] == "P10"]
    p90 = quantiles_table[quantiles_table["Type"] == "P90"]
    if len(p10) > 0 and len(p90) > 0:
        p10_r, p90_r = p10.iloc[0], p90.iloc[0]
        in_range = sum(
            1 for h in HOURS
            if (p10_r[f"HE{h}"] is not None and p90_r[f"HE{h}"] is not None
                and act[f"HE{h}"] is not None
                and p10_r[f"HE{h}"] <= act[f"HE{h}"] <= p90_r[f"HE{h}"])
        )
        coverage_80 = in_range / 24

    return {"mae": mae, "rmse": rmse, "mape": mape, "coverage_80pct": coverage_80}


def _extract_importances(
    models: dict, feature_cols: list[str], quantiles: list[float],
) -> list[dict]:
    if 0.50 not in quantiles:
        return []

    coef_sum = np.zeros(len(feature_cols))
    n_models = 0
    for h in HOURS:
        key = (h, 0.50)
        if key not in models:
            continue
        coefs = models[key].named_steps["qr"].coef_
        coef_sum += np.abs(coefs)
        n_models += 1

    if n_models == 0:
        return []

    avg = coef_sum / n_models
    ranked = sorted(zip(feature_cols, avg), key=lambda x: -x[1])
    return [{"feature": n, "importance": round(float(v), 4)} for n, v in ranked[:15]]


DAY_ABBR = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}


def _print_model_info(result: dict) -> None:
    """Print model training summary."""
    mi = result.get("model_info", {})
    print("\n" + "=" * 90)
    print("  LASSO QUANTILE REGRESSION — MODEL INFO")
    print("=" * 90)
    print(f"  Alpha: {mi.get('alpha', '?')}  |  Features: {mi.get('n_features', '?')}"
          f"  |  Train samples: {mi.get('n_train_samples', '?')}")
    print(f"  Train window: {mi.get('train_start', '?')} to {mi.get('train_end', '?')}"
          f"  |  Day-type: {mi.get('day_type', '?')}")
    importances = mi.get("feature_importances", [])
    if importances:
        print("\n  Top Features (by |coefficient|):")
        for i, f in enumerate(importances[:10], 1):
            print(f"    {i:>2}. {f['feature']:<35s} {f['importance']:.4f}")
    print()


def _print_table(table: pd.DataFrame, metrics: dict | None) -> None:
    """Print the Actual/Forecast/Error table."""
    print("=" * 120)
    print("  DA LMP LASSO QR FORECAST — Western Hub ($/MWh)")
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
        print(line)

    print("-" * len(header))

    if metrics:
        print(f"  MAE: ${metrics.get('mae', 0):.2f}/MWh  |  RMSE: ${metrics.get('rmse', 0):.2f}/MWh"
              f"  |  MAPE: {metrics.get('mape', 0):.1f}%")
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
        line += f" {row['OnPeak']:>7.2f}" if pd.notna(row['OnPeak']) else f" {'':>7}"
        line += f" {row['OffPeak']:>7.2f}" if pd.notna(row['OffPeak']) else f" {'':>7}"
        line += f" {row['Flat']:>7.2f}" if pd.notna(row['Flat']) else f" {'':>7}"
        print(line)

    print("-" * len(header) + "\n")


if __name__ == "__main__":
    import logging

    import src.settings  # noqa: F401 — load env vars

    logging.basicConfig(level=logging.INFO)

    result = run(config=LassoQRConfig())
    if "error" in result:
        print(f"ERROR: {result['error']}")
    else:
        forecast_date = pd.to_datetime(result["forecast_date"]).date()
        reference_date = pd.to_datetime(result["reference_date"]).date()
        target_dow = DAY_ABBR[forecast_date.weekday()]
        ref_dow = DAY_ABBR[reference_date.weekday()]

        print("\n" + "=" * 90)
        print(f"  LASSO QR FORECAST")
        print(f"  Forecast: {forecast_date} ({target_dow})  |  "
              f"Reference: {reference_date} ({ref_dow})  |  Hub: Western Hub")
        print("=" * 90)

        _print_model_info(result)
        _print_table(result["output_table"], result.get("metrics"))
        _print_quantiles(result["quantiles_table"])
