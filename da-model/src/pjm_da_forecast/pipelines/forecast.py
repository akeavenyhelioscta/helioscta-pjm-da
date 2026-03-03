"""Forecast pipeline — predict DA LMP for a target date.

Trains on all available data prior to the forecast date, then generates
hourly point and probabilistic forecasts for the target day.

Output table format:
  Date | Type | HE1 | HE2 | ... | HE24 | OnPeak | OffPeak | Flat
  Each date gets two rows: Actual (if available) and Forecast.
  OnPeak = HE 8-23, OffPeak = HE 1-7 + HE 24, Flat = all 24h average.
"""
import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd

from src.pjm_da_forecast import configs
from src.pjm_da_forecast.features.builder import build_features
from src.pjm_da_forecast.features.preprocessing import asinh_inverse
from src.pjm_da_forecast.models.lightgbm_quantile import LightGBMQuantile
from src.pjm_da_forecast.evaluation.metrics import evaluate_forecast

logger = logging.getLogger(__name__)

NON_FEATURE_COLS = {"date", "hour_ending", "lmp_total_target"}

# PJM NERC peak definition
ONPEAK_HOURS = list(range(8, 24))   # HE 8-23
OFFPEAK_HOURS = list(range(1, 8)) + [24]  # HE 1-7, HE 24


def _get_feature_cols(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if c not in NON_FEATURE_COLS]


def _add_summary_cols(row_dict: dict) -> dict:
    """Add OnPeak, OffPeak, Flat averages to an hourly row dict."""
    onpeak_vals = [row_dict.get(f"HE{h}") for h in ONPEAK_HOURS if row_dict.get(f"HE{h}") is not None]
    offpeak_vals = [row_dict.get(f"HE{h}") for h in OFFPEAK_HOURS if row_dict.get(f"HE{h}") is not None]
    all_vals = [row_dict.get(f"HE{h}") for h in range(1, 25) if row_dict.get(f"HE{h}") is not None]

    row_dict["OnPeak"] = np.mean(onpeak_vals) if onpeak_vals else np.nan
    row_dict["OffPeak"] = np.mean(offpeak_vals) if offpeak_vals else np.nan
    row_dict["Flat"] = np.mean(all_vals) if all_vals else np.nan
    return row_dict


def _build_output_table(
    target_date: date,
    forecast_hourly: dict[int, float],
    actuals_hourly: dict[int, float] | None = None,
) -> pd.DataFrame:
    """Build the pivoted output table with Actual/Forecast rows per date.

    Args:
        target_date: The forecast date.
        forecast_hourly: {hour_ending: predicted_price} for hours 1-24.
        actuals_hourly: {hour_ending: actual_price} for hours 1-24, or None.

    Returns:
        DataFrame with columns [Date, Type, HE1..HE24, OnPeak, OffPeak, Flat].
    """
    rows = []

    # Actual row (first)
    if actuals_hourly is not None:
        actual_row = {"Date": target_date, "Type": "Actual"}
        for h in range(1, 25):
            actual_row[f"HE{h}"] = actuals_hourly.get(h)
        actual_row = _add_summary_cols(actual_row)
        rows.append(actual_row)

    # Forecast row
    forecast_row = {"Date": target_date, "Type": "Forecast"}
    for h in range(1, 25):
        forecast_row[f"HE{h}"] = forecast_hourly.get(h)
    forecast_row = _add_summary_cols(forecast_row)
    rows.append(forecast_row)

    # Column ordering
    cols = ["Date", "Type"] + [f"HE{h}" for h in range(1, 25)] + ["OnPeak", "OffPeak", "Flat"]
    return pd.DataFrame(rows, columns=cols)


def run(
    forecast_date: str = "2026-02-27",
    mode: str = "full_feature",
) -> dict:
    """Run the forecast pipeline for a single target date.

    Args:
        forecast_date: Date to forecast (YYYY-MM-DD). Can be a future date
            where DA LMP actuals are not yet known.
        mode: "full_feature" (2020+) or "extended" (2014+).

    Returns:
        Dict with output_table, quantiles_table, metrics, and model info.
    """
    target_date = pd.to_datetime(forecast_date).date()
    logger.info("=" * 60)
    logger.info(f"Forecasting DA LMP for {target_date} — Western Hub")
    logger.info("=" * 60)

    # 1. Build full feature matrix (with scaffold for forecast date if needed)
    logger.info("Building features...")
    df = build_features(mode=mode, forecast_date=target_date)

    # 2. Check actuals availability
    df_target = df[df["date"] == target_date]
    if len(df_target) == 0:
        logger.error(f"Failed to build features for {target_date}")
        return {"error": f"Could not build feature rows for {target_date}"}

    has_actuals = df_target["lmp_total_target"].notna().all()
    if has_actuals:
        logger.info(f"Actuals available for {target_date}")
    else:
        logger.info(f"No actuals for {target_date} — producing forecast only")

    # 3. Split: train on everything before forecast date
    df_train = df[df["date"] < target_date].copy()
    df_forecast = df[df["date"] == target_date].copy()

    logger.info(f"Training data: {len(df_train):,} rows ({df_train['date'].min()} to {df_train['date'].max()})")
    logger.info(f"Forecast rows: {len(df_forecast)} (24 hours for {target_date})")

    # 4. Train model on all available history
    feature_cols = _get_feature_cols(df_train)
    X_train = df_train[feature_cols].astype(float)
    y_train = df_train["lmp_total_target"].astype(float)
    X_forecast = df_forecast[feature_cols].astype(float)

    logger.info(f"Training LightGBM on {len(X_train):,} samples, {len(feature_cols)} features...")
    model = LightGBMQuantile()
    model.fit(X_train, y_train)

    # 5. Predict (in asinh space) and inverse transform
    preds_asinh = model.predict(X_forecast)
    preds = preds_asinh.copy()
    q_cols = [c for c in preds.columns if c.startswith("q_")]
    for col in q_cols:
        preds[col] = asinh_inverse(preds_asinh[col])
    if "point_forecast" in preds.columns:
        preds["point_forecast"] = asinh_inverse(preds_asinh["point_forecast"])

    # 6. Build hourly dicts for the output table
    hours = df_forecast["hour_ending"].astype(int).values
    forecast_hourly = dict(zip(hours, preds["point_forecast"].values))

    actuals_hourly = None
    actuals_raw = None
    if has_actuals:
        actuals_asinh = df_forecast["lmp_total_target"].values
        actuals_raw = asinh_inverse(actuals_asinh)
        actuals_hourly = dict(zip(hours, actuals_raw))

    # 7. Build pivoted output table
    output_table = _build_output_table(target_date, forecast_hourly, actuals_hourly)

    # 8. Build quantile table (same pivot shape, one row per quantile)
    quantile_rows = []
    for q in configs.QUANTILES:
        col = f"q_{q:.2f}"
        if col in preds.columns:
            q_row = {"Date": target_date, "Type": f"P{int(q*100):02d}"}
            q_hourly = dict(zip(hours, preds[col].values))
            for h in range(1, 25):
                q_row[f"HE{h}"] = q_hourly.get(h)
            q_row = _add_summary_cols(q_row)
            quantile_rows.append(q_row)

    q_table_cols = ["Date", "Type"] + [f"HE{h}" for h in range(1, 25)] + ["OnPeak", "OffPeak", "Flat"]
    quantiles_table = pd.DataFrame(quantile_rows, columns=q_table_cols)

    # 9. Evaluate if actuals available
    metrics = None
    if actuals_raw is not None:
        naive_date = target_date - timedelta(days=7)
        df_naive = df[df["date"] == naive_date]
        y_naive = None
        if len(df_naive) == 24:
            y_naive = asinh_inverse(df_naive.sort_values("hour_ending")["lmp_total_target"].values)

        metrics = evaluate_forecast(
            y_true=actuals_raw,
            y_pred_df=preds,
            quantiles=configs.QUANTILES,
            y_naive=y_naive,
        )

    # 10. Print results
    _print_table(output_table, metrics)
    _print_quantiles(quantiles_table)

    return {
        "output_table": output_table,
        "quantiles_table": quantiles_table,
        "metrics": metrics,
        "target_date": str(target_date),
        "has_actuals": has_actuals,
        "n_train": len(df_train),
        "train_range": f"{df_train['date'].min()} to {df_train['date'].max()}",
    }


def _print_table(table: pd.DataFrame, metrics: dict | None) -> None:
    """Print the Actual/Forecast table."""
    # Build header
    he_cols = [f"HE{h}" for h in range(1, 25)]
    summary_cols = ["OnPeak", "OffPeak", "Flat"]

    print("\n" + "=" * 120)
    print("  DA LMP FORECAST — Western Hub ($/MWh)")
    print("=" * 120)

    # Column header
    header = f"{'Date':<12} {'Type':<10}"
    for h in range(1, 25):
        header += f" {h:>6}"
    header += f" {'OnPk':>7} {'OffPk':>7} {'Flat':>7}"
    print(header)
    print("-" * len(header))

    # Data rows
    for _, row in table.iterrows():
        line = f"{str(row['Date']):<12} {row['Type']:<10}"
        for h in range(1, 25):
            val = row[f"HE{h}"]
            line += f" {val:>6.1f}" if pd.notna(val) else f" {'':>6}"
        line += f" {row['OnPeak']:>7.2f} {row['OffPeak']:>7.2f} {row['Flat']:>7.2f}"
        print(line)

    print("-" * len(header))

    # Metrics summary
    if metrics:
        print(f"  MAE: ${metrics.get('mae', 0):.2f}/MWh  |  MAPE: {metrics.get('mape', 0):.1f}%"
              f"  |  rMAE: {metrics.get('rmae', 0):.3f}")
        cov80 = metrics.get("coverage_80pct", 0)
        cov90 = metrics.get("coverage_90pct", 0)
        print(f"  Coverage: 80%PI={cov80:.0%}  90%PI={cov90:.0%}")

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
        line += f" {row['OnPeak']:>7.2f} {row['OffPeak']:>7.2f} {row['Flat']:>7.2f}"
        print(line)

    print("-" * len(header) + "\n")


if __name__ == "__main__":
    import src.settings
    result = run(forecast_date="2026-02-27")
