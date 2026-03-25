"""Run preflight data validation for the like-day forecast model.

Pulls all raw data sources and the assembled feature matrix, then runs
Evidently 0.7 drift + data-summary checks.  Outputs HTML and JSON reports.

Usage (from backend/):
    python -m src.like_day_forecast.scripts.run_preflight
"""
from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from evidently import Report
from evidently.presets import DataDriftPreset, DataSummaryPreset

from src.like_day_forecast import configs

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)

REPORTS_DIR = Path(__file__).resolve().parent.parent / "reports"


# ─── Result containers ───────────────────────────────────────────────────


@dataclass
class SourceCheckResult:
    name: str
    passed: bool
    row_count: int = 0
    date_range: str = ""
    issues: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


# ─── Source-level validators ─────────────────────────────────────────────


def _recent_coverage(
    df: pd.DataFrame, date_col: str, expected_per_day: int, recent_days: int
) -> tuple[list[str], list[str]]:
    issues, warnings = [], []
    today = date.today()
    max_date = df[date_col].max()
    staleness = (today - max_date).days if hasattr(max_date, "day") else None
    if staleness is not None and staleness > 2:
        issues.append(f"Data is {staleness}d stale (latest: {max_date})")

    recent = df[df[date_col] >= today - timedelta(days=recent_days)]
    if recent.empty:
        issues.append(f"No data in last {recent_days} days")
        return issues, warnings

    daily_counts = recent.groupby(date_col).size()
    incomplete = daily_counts[daily_counts < expected_per_day]
    for d, cnt in incomplete.items():
        warnings.append(f"{d}: {cnt}/{expected_per_day} rows")
    return issues, warnings


def validate_lmp(df: pd.DataFrame) -> SourceCheckResult:
    res = SourceCheckResult(name="LMP (DA)", passed=True, row_count=len(df))
    if df.empty:
        return SourceCheckResult(name="LMP (DA)", passed=False, issues=["No data"])
    dates = sorted(df["date"].unique())
    res.date_range = f"{dates[0]} to {dates[-1]}"
    iss, wrn = _recent_coverage(df, "date", 24, 3)
    res.issues.extend(iss)
    res.warnings.extend(wrn)

    lmp = df["lmp_total"].dropna()
    if lmp.empty:
        res.issues.append("lmp_total entirely null")
    else:
        if (lmp < -500).sum():
            res.warnings.append(f"{(lmp < -500).sum()} rows < -$500")
        if (lmp > 3000).sum():
            res.warnings.append(f"{(lmp > 3000).sum()} rows > $3,000")
    res.passed = len(res.issues) == 0
    return res


def validate_load(df_da: pd.DataFrame | None, df_rt: pd.DataFrame) -> SourceCheckResult:
    res = SourceCheckResult(name="Load", passed=True)
    df = df_da if df_da is not None and not df_da.empty else df_rt
    if df is None or df.empty:
        return SourceCheckResult(name="Load", passed=False, issues=["No load data"])
    res.name = "Load (DA)" if (df_da is not None and not df_da.empty) else "Load (RT fallback)"
    res.row_count = len(df)
    dates = sorted(df["date"].unique())
    res.date_range = f"{dates[0]} to {dates[-1]}"
    iss, wrn = _recent_coverage(df, "date", 24, 3)
    res.issues.extend(iss)
    res.warnings.extend(wrn)
    if df_da is None or df_da.empty:
        res.warnings.append("DA load unavailable, using RT metered")
    res.passed = len(res.issues) == 0
    return res


def validate_gas(df: pd.DataFrame) -> SourceCheckResult:
    res = SourceCheckResult(name="Gas Prices", passed=True, row_count=len(df))
    if df.empty:
        return SourceCheckResult(name="Gas Prices", passed=False, issues=["No data"])
    dates = sorted(df["date"].unique())
    res.date_range = f"{dates[0]} to {dates[-1]}"
    staleness = (date.today() - dates[-1]).days
    if staleness > 3:
        res.issues.append(f"Gas data is {staleness}d stale (latest: {dates[-1]})")
    for col in ["gas_m3_price", "gas_hh_price"]:
        if col not in df.columns:
            res.issues.append(f"Missing column: {col}")
        elif df[col].isna().all():
            res.issues.append(f"{col} entirely null")
        elif (df[col].dropna() < 0).sum():
            res.warnings.append(f"{col}: {(df[col].dropna() < 0).sum()} negative vals")
    res.passed = len(res.issues) == 0
    return res


def validate_weather(df: pd.DataFrame | None) -> SourceCheckResult:
    res = SourceCheckResult(name="Weather", passed=True)
    if df is None or df.empty:
        return SourceCheckResult(name="Weather", passed=False, issues=["No data"])
    res.row_count = len(df)
    dates = sorted(df["date"].unique())
    res.date_range = f"{dates[0]} to {dates[-1]}"
    iss, wrn = _recent_coverage(df, "date", 24, 3)
    res.issues.extend(iss)
    res.warnings.extend(wrn)
    if "temperature" in df.columns:
        t = df["temperature"].dropna()
        if len(t) and t.min() < -40:
            res.warnings.append(f"Temp below -40F: min={t.min():.1f}")
        if len(t) and t.max() > 130:
            res.warnings.append(f"Temp above 130F: max={t.max():.1f}")
    res.passed = len(res.issues) == 0
    return res


# ─── Evidently drift / quality report ────────────────────────────────────


def build_evidently_report(
    df_features: pd.DataFrame,
    drift_ref_days: int = 365,
    drift_threshold: float = 0.25,
    output_dir: Path = REPORTS_DIR,
) -> tuple[SourceCheckResult, float | None, str | None, str | None]:
    """Run DataDrift + DataSummary report via Evidently 0.7.

    Returns (check_result, drift_share, html_path, json_path).
    """
    res = SourceCheckResult(name="Feature Matrix", passed=True, row_count=len(df_features))
    if df_features.empty:
        res.passed = False
        res.issues.append("Feature matrix is empty")
        return res, None, None, None

    dates = sorted(df_features["date"].unique())
    res.date_range = f"{dates[0]} to {dates[-1]}"

    feature_cols = [c for c in df_features.columns if c != "date"]

    # Split reference / current windows
    cutoff = dates[-1] - timedelta(days=drift_ref_days)
    ref_mask = df_features["date"] < cutoff
    cur_mask = df_features["date"] >= cutoff

    drift_share = None
    html_path = None
    json_path = None

    if ref_mask.sum() < 30 or cur_mask.sum() < 7:
        res.warnings.append(
            f"Insufficient data for drift (ref={ref_mask.sum()}, cur={cur_mask.sum()})"
        )
        return res, None, None, None

    ref_data = df_features.loc[ref_mask, feature_cols]
    cur_data = df_features.loc[cur_mask, feature_cols]

    logger.info(
        f"Running Evidently report: ref={len(ref_data)} days, cur={len(cur_data)} days"
    )

    report = Report(metrics=[DataSummaryPreset(), DataDriftPreset()])
    snapshot = report.run(current_data=cur_data, reference_data=ref_data)

    # Extract drift share from result dict
    result_dict = snapshot.dump_dict()
    for _fp, metric_res in result_dict.get("metric_results", {}).items():
        if not isinstance(metric_res, dict):
            continue
        share_entry = metric_res.get("share")
        if isinstance(share_entry, dict) and "Share of Drifted" in share_entry.get("display_name", ""):
            drift_share = share_entry.get("value")
            break

    if drift_share is not None and drift_share > drift_threshold:
        res.issues.append(
            f"Feature drift: {drift_share:.1%} of columns drifted (threshold {drift_threshold:.0%})"
        )

    # Missing values summary
    null_rate = cur_data.isnull().mean()
    high_null = null_rate[null_rate > 0.10]
    if not high_null.empty:
        for col, rate in high_null.items():
            res.warnings.append(f"{col}: {rate:.1%} missing in current window")

    # Constant columns
    const_cols = [c for c in feature_cols if cur_data[c].nunique() <= 1]
    if const_cols:
        res.issues.append(f"Constant columns in current window: {const_cols}")

    # Save artifacts
    output_dir.mkdir(parents=True, exist_ok=True)
    today_str = date.today().isoformat()
    html_path = str(output_dir / f"preflight_report_{today_str}.html")
    json_path = str(output_dir / f"preflight_report_{today_str}.json")

    snapshot.save_html(html_path)
    with open(json_path, "w") as f:
        json.dump(result_dict, f, indent=2, default=str)

    logger.info(f"HTML report: {html_path}")
    logger.info(f"JSON report: {json_path}")

    res.passed = len(res.issues) == 0
    return res, drift_share, html_path, json_path


# ─── Orchestrator ────────────────────────────────────────────────────────


def main() -> int:
    from src.like_day_forecast.data import (
        lmps_hourly,
        load_da_hourly,
        load_rt_metered_hourly,
        gas_prices,
        weather_hourly,
    )
    from src.like_day_forecast.features.builder import build_daily_features

    schema = configs.SCHEMA

    logger.info("=" * 60)
    logger.info("PREFLIGHT DATA VALIDATION — Like-Day Forecast")
    logger.info("=" * 60)

    checks: list[SourceCheckResult] = []

    # 1. Raw source checks
    logger.info("Validating LMP (DA)...")
    try:
        df_lmp = lmps_hourly.pull(schema=schema, hub=configs.HUB, market="da")
        checks.append(validate_lmp(df_lmp))
    except Exception as e:
        checks.append(SourceCheckResult(name="LMP (DA)", passed=False, issues=[str(e)]))

    logger.info("Validating Load...")
    df_da_load = None
    try:
        df_da_load = load_da_hourly.pull(schema=schema)
    except Exception:
        pass
    try:
        df_rt_load = load_rt_metered_hourly.pull(schema=schema)
        checks.append(validate_load(df_da_load, df_rt_load))
    except Exception as e:
        checks.append(SourceCheckResult(name="Load", passed=False, issues=[str(e)]))

    logger.info("Validating Gas Prices...")
    try:
        df_gas = gas_prices.pull()
        checks.append(validate_gas(df_gas))
    except Exception as e:
        checks.append(SourceCheckResult(name="Gas Prices", passed=False, issues=[str(e)]))

    logger.info("Validating Weather...")
    try:
        df_weather = weather_hourly.pull()
        checks.append(validate_weather(df_weather))
    except Exception as e:
        checks.append(SourceCheckResult(name="Weather", passed=False, issues=[str(e)]))

    # 2. Feature matrix + Evidently
    logger.info("Building feature matrix...")
    feat_check = None
    drift_share = None
    html_path = None
    json_path = None
    try:
        df_features = build_daily_features(schema=schema)
        feat_check, drift_share, html_path, json_path = build_evidently_report(df_features)
    except Exception as e:
        feat_check = SourceCheckResult(
            name="Feature Matrix", passed=False, issues=[f"Build failed: {e}"]
        )

    # 3. Print summary
    all_passed = all(c.passed for c in checks)
    if feat_check:
        all_passed = all_passed and feat_check.passed

    lines = ["\nPREFLIGHT VALIDATION SUMMARY", "=" * 50]
    for c in checks:
        tag = "PASS" if c.passed else "FAIL"
        lines.append(f"  [{tag}] {c.name}: {c.row_count:,} rows")
        if c.date_range:
            lines.append(f"         Range: {c.date_range}")
        for i in c.issues:
            lines.append(f"         ISSUE: {i}")
        for w in c.warnings:
            lines.append(f"         WARN:  {w}")

    if feat_check:
        tag = "PASS" if feat_check.passed else "FAIL"
        lines.append(f"  [{tag}] {feat_check.name}: {feat_check.row_count:,} rows")
        if feat_check.date_range:
            lines.append(f"         Range: {feat_check.date_range}")
        if drift_share is not None:
            lines.append(f"         Drift share: {drift_share:.1%}")
        for i in feat_check.issues:
            lines.append(f"         ISSUE: {i}")
        for w in feat_check.warnings:
            lines.append(f"         WARN:  {w}")

    lines.append("=" * 50)
    lines.append(f"OVERALL: {'PASS' if all_passed else 'FAIL'}")
    if html_path:
        lines.append(f"HTML report: {html_path}")
    if json_path:
        lines.append(f"JSON report: {json_path}")

    print("\n".join(lines))
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
