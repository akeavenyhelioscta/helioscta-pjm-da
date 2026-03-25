"""Pre-sweep data validation using Evidently.

Validates raw data sources and the assembled feature matrix before running
grid search or backtest sweeps. Catches stale feeds, missing data, distribution
drift, and degenerate features early — before wasting compute on 200+ runs.

Usage:
    from src.like_day_forecast.validation.preflight import run_preflight

    result = run_preflight()
    if not result.passed:
        print(result.summary)
        sys.exit(1)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from evidently import ColumnMapping
from evidently.report import Report
from evidently.metric_preset import DataQualityPreset, DataDriftPreset
from evidently.test_suite import TestSuite
from evidently.tests import (
    TestShareOfMissingValues,
    TestNumberOfConstantColumns,
    TestNumberOfDuplicatedRows,
    TestNumberOfColumnsWithMissingValues,
)

from src.like_day_forecast import configs

logger = logging.getLogger(__name__)


# ─── Result container ────────────────────────────────────────────────────


@dataclass
class SourceCheckResult:
    """Result of a single data source validation."""
    name: str
    passed: bool
    row_count: int = 0
    date_range: str = ""
    issues: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class PreflightResult:
    """Aggregated result of all preflight checks."""
    passed: bool
    source_checks: list[SourceCheckResult] = field(default_factory=list)
    feature_check: SourceCheckResult | None = None
    drift_share: float | None = None
    report_path: str | None = None
    summary: str = ""


# ─── Data source validators ─────────────────────────────────────────────


def _check_recent_coverage(
    df: pd.DataFrame,
    date_col: str,
    expected_rows_per_day: int,
    recent_days: int,
    source_name: str,
) -> tuple[list[str], list[str]]:
    """Check that recent days have expected row counts."""
    issues: list[str] = []
    warnings: list[str] = []

    today = date.today()
    recent_start = today - timedelta(days=recent_days)

    dates = df[date_col].unique()
    max_date = max(dates)
    staleness_days = (today - max_date).days if hasattr(max_date, "day") else None

    if staleness_days is not None and staleness_days > 2:
        issues.append(f"Data is {staleness_days} days stale (latest: {max_date})")

    recent = df[df[date_col] >= recent_start]
    if len(recent) == 0:
        issues.append(f"No data in last {recent_days} days")
        return issues, warnings

    daily_counts = recent.groupby(date_col).size()
    incomplete = daily_counts[daily_counts < expected_rows_per_day]
    if len(incomplete) > 0:
        for d, cnt in incomplete.items():
            warnings.append(
                f"{d}: {cnt}/{expected_rows_per_day} rows"
            )

    return issues, warnings


def validate_lmp(df: pd.DataFrame, recent_days: int = 3) -> SourceCheckResult:
    """Validate LMP hourly data: completeness, ranges, staleness."""
    result = SourceCheckResult(name="LMP (DA)", passed=True, row_count=len(df))

    if df.empty:
        result.passed = False
        result.issues.append("No LMP data returned")
        return result

    dates = sorted(df["date"].unique())
    result.date_range = f"{dates[0]} to {dates[-1]}"

    # Recent coverage (expect 24 hours per day)
    issues, warnings = _check_recent_coverage(df, "date", 24, recent_days, "LMP")
    result.issues.extend(issues)
    result.warnings.extend(warnings)

    # Value range: PJM DA LMP typically -$500 to $3,000
    lmp_vals = df["lmp_total"].dropna()
    if len(lmp_vals) == 0:
        result.issues.append("lmp_total is entirely null")
    else:
        extreme_low = (lmp_vals < -500).sum()
        extreme_high = (lmp_vals > 3000).sum()
        if extreme_low > 0:
            result.warnings.append(f"{extreme_low} rows with lmp_total < -$500")
        if extreme_high > 0:
            result.warnings.append(f"{extreme_high} rows with lmp_total > $3,000")

    # Null rate in recent data
    recent = df[df["date"] >= (date.today() - timedelta(days=recent_days))]
    if len(recent) > 0:
        null_rate = recent["lmp_total"].isna().mean()
        if null_rate > 0.05:
            result.issues.append(f"Recent null rate: {null_rate:.1%}")

    result.passed = len(result.issues) == 0
    return result


def validate_load(
    df_da: pd.DataFrame | None,
    df_rt: pd.DataFrame,
    recent_days: int = 3,
) -> SourceCheckResult:
    """Validate load data: DA preferred, RT as fallback."""
    result = SourceCheckResult(name="Load", passed=True)

    if df_da is not None and not df_da.empty:
        result.name = "Load (DA)"
        result.row_count = len(df_da)
        dates = sorted(df_da["date"].unique())
        result.date_range = f"{dates[0]} to {dates[-1]}"
        issues, warnings = _check_recent_coverage(
            df_da, "date", 24, recent_days, "DA Load"
        )
        result.issues.extend(issues)
        result.warnings.extend(warnings)
    elif not df_rt.empty:
        result.name = "Load (RT fallback)"
        result.row_count = len(df_rt)
        dates = sorted(df_rt["date"].unique())
        result.date_range = f"{dates[0]} to {dates[-1]}"
        result.warnings.append("DA load unavailable, using RT metered fallback")
        issues, warnings = _check_recent_coverage(
            df_rt, "date", 24, recent_days, "RT Load"
        )
        result.issues.extend(issues)
        result.warnings.extend(warnings)
    else:
        result.passed = False
        result.issues.append("No load data returned (neither DA nor RT)")
        return result

    result.passed = len(result.issues) == 0
    return result


def validate_gas(df: pd.DataFrame, recent_bdays: int = 5) -> SourceCheckResult:
    """Validate gas price data: M3 and HH present, no recent gaps."""
    result = SourceCheckResult(name="Gas Prices", passed=True, row_count=len(df))

    if df.empty:
        result.passed = False
        result.issues.append("No gas price data returned")
        return result

    dates = sorted(df["date"].unique())
    result.date_range = f"{dates[0]} to {dates[-1]}"

    # Staleness check
    max_date = dates[-1]
    staleness = (date.today() - max_date).days if hasattr(max_date, "day") else None
    if staleness is not None and staleness > 3:
        result.issues.append(f"Gas data is {staleness} days stale (latest: {max_date})")

    # Check for required columns
    for col in ["gas_m3_price", "gas_hh_price"]:
        if col not in df.columns:
            result.issues.append(f"Missing column: {col}")
        elif df[col].isna().all():
            result.issues.append(f"{col} is entirely null")
        else:
            recent = df.tail(recent_bdays)
            null_rate = recent[col].isna().mean()
            if null_rate > 0.2:
                result.warnings.append(
                    f"{col}: {null_rate:.0%} null in last {recent_bdays} rows"
                )

    # Negative prices are unusual but possible for gas
    for col in ["gas_m3_price", "gas_hh_price"]:
        if col in df.columns:
            neg = (df[col].dropna() < 0).sum()
            if neg > 0:
                result.warnings.append(f"{col}: {neg} negative values")

    result.passed = len(result.issues) == 0
    return result


def validate_weather(
    df: pd.DataFrame | None, recent_days: int = 3
) -> SourceCheckResult:
    """Validate weather data: temperature ranges, recent coverage."""
    result = SourceCheckResult(name="Weather", passed=True)

    if df is None or df.empty:
        result.passed = False
        result.issues.append("No weather data returned")
        return result

    result.row_count = len(df)
    dates = sorted(df["date"].unique())
    result.date_range = f"{dates[0]} to {dates[-1]}"

    issues, warnings = _check_recent_coverage(df, "date", 24, recent_days, "Weather")
    result.issues.extend(issues)
    result.warnings.extend(warnings)

    # Temperature sanity: PJM region reasonable range
    if "temperature" in df.columns:
        temps = df["temperature"].dropna()
        if len(temps) > 0:
            if temps.min() < -40:
                result.warnings.append(f"Temp below -40F: min={temps.min():.1f}")
            if temps.max() > 130:
                result.warnings.append(f"Temp above 130F: max={temps.max():.1f}")

    result.passed = len(result.issues) == 0
    return result


# ─── Feature matrix validation ──────────────────────────────────────────


def validate_feature_matrix(
    df_features: pd.DataFrame,
    drift_reference_days: int = 365,
    drift_threshold: float = 0.25,
    max_missing_share: float = 0.10,
    output_dir: str | None = None,
) -> tuple[SourceCheckResult, float | None, str | None]:
    """Validate the assembled feature matrix using Evidently.

    Runs two Evidently analyses:
      1. TestSuite — hard pass/fail checks (missing values, constants, duplicates)
      2. Report — DataQuality + DataDrift for diagnostics and HTML output

    Args:
        df_features: Daily feature matrix from build_daily_features().
        drift_reference_days: Number of historical days used as reference window.
        drift_threshold: Fail if share of drifted columns exceeds this.
        max_missing_share: Fail if overall missing value share exceeds this.
        output_dir: Directory to save HTML report. None = no save.

    Returns:
        (check_result, drift_share, report_path)
    """
    result = SourceCheckResult(
        name="Feature Matrix",
        passed=True,
        row_count=len(df_features),
    )

    if df_features.empty:
        result.passed = False
        result.issues.append("Feature matrix is empty")
        return result, None, None

    dates = sorted(df_features["date"].unique())
    result.date_range = f"{dates[0]} to {dates[-1]}"

    # Separate numeric feature columns from date
    feature_cols = [c for c in df_features.columns if c != "date"]
    df_numeric = df_features[feature_cols].copy()

    # ── 1. TestSuite — hard checks ──
    logger.info("Running Evidently TestSuite on feature matrix...")
    test_suite = TestSuite(
        tests=[
            TestShareOfMissingValues(lt=max_missing_share),
            TestNumberOfConstantColumns(eq=0),
            TestNumberOfDuplicatedRows(eq=0),
            TestNumberOfColumnsWithMissingValues(
                lt=int(len(feature_cols) * 0.3)
            ),
        ]
    )
    test_suite.run(current_data=df_numeric)

    suite_results = test_suite.as_dict()
    all_passed = suite_results.get("summary", {}).get("all_passed", False)
    if not all_passed:
        for test in suite_results.get("tests", []):
            if test.get("status") == "FAIL":
                desc = test.get("name", "Unknown test")
                result.issues.append(f"TestSuite FAIL: {desc}")

    # ── 2. DataDrift + DataQuality report ──
    drift_share = None
    report_path = None

    # Split into reference (older) and current (recent) windows
    cutoff_date = dates[-1] - timedelta(days=drift_reference_days)
    ref_mask = df_features["date"] < cutoff_date
    cur_mask = df_features["date"] >= cutoff_date

    if ref_mask.sum() >= 30 and cur_mask.sum() >= 7:
        logger.info(
            f"Running Evidently drift report: ref={ref_mask.sum()} days, "
            f"cur={cur_mask.sum()} days (cutoff={cutoff_date})"
        )

        ref_data = df_features.loc[ref_mask, feature_cols]
        cur_data = df_features.loc[cur_mask, feature_cols]

        column_mapping = ColumnMapping(
            numerical_features=feature_cols,
        )

        report = Report(
            metrics=[DataQualityPreset(), DataDriftPreset()],
        )
        report.run(
            reference_data=ref_data,
            current_data=cur_data,
            column_mapping=column_mapping,
        )

        # Extract drift share
        report_dict = report.as_dict()
        for metric_result in report_dict.get("metrics", []):
            metric_data = metric_result.get("result", {})
            if "share_of_drifted_columns" in metric_data:
                drift_share = metric_data["share_of_drifted_columns"]
                break

        if drift_share is not None and drift_share > drift_threshold:
            result.issues.append(
                f"Feature drift: {drift_share:.1%} of columns drifted "
                f"(threshold: {drift_threshold:.0%})"
            )

        # Save HTML report
        if output_dir:
            report_dir = Path(output_dir)
            report_dir.mkdir(parents=True, exist_ok=True)
            report_path = str(
                report_dir / f"preflight_report_{date.today().isoformat()}.html"
            )
            report.save_html(report_path)
            logger.info(f"Evidently report saved to {report_path}")
    else:
        result.warnings.append(
            f"Not enough data for drift analysis "
            f"(ref={ref_mask.sum()}, cur={cur_mask.sum()})"
        )

    result.passed = len(result.issues) == 0
    return result, drift_share, report_path


# ─── Orchestrator ────────────────────────────────────────────────────────


def run_preflight(
    schema: str = configs.SCHEMA,
    output_dir: str | None = None,
    drift_threshold: float = 0.25,
    max_missing_share: float = 0.10,
) -> PreflightResult:
    """Run all preflight validation checks.

    Pulls data from the database, validates each source, builds the feature
    matrix, and runs Evidently drift/quality analysis.

    Args:
        schema: Database schema to pull from.
        output_dir: Directory for HTML report. None = no report saved.
        drift_threshold: Max share of drifted feature columns before failing.
        max_missing_share: Max overall missing value share before failing.

    Returns:
        PreflightResult with pass/fail status and details.
    """
    from src.like_day_forecast.data import (
        lmps_hourly,
        load_da_hourly,
        load_rt_metered_hourly,
        gas_prices,
        weather_hourly,
    )
    from src.like_day_forecast.features.builder import build_daily_features

    logger.info("=" * 60)
    logger.info("PREFLIGHT DATA VALIDATION")
    logger.info("=" * 60)

    source_checks: list[SourceCheckResult] = []

    # ── 1. Validate raw data sources ──

    # LMP DA
    logger.info("Validating LMP (DA)...")
    try:
        df_lmp_da = lmps_hourly.pull(schema=schema, hub=configs.HUB, market="da")
        source_checks.append(validate_lmp(df_lmp_da))
    except Exception as e:
        source_checks.append(
            SourceCheckResult(name="LMP (DA)", passed=False, issues=[str(e)])
        )

    # Load (DA + RT)
    logger.info("Validating Load...")
    df_da_load = None
    try:
        df_da_load = load_da_hourly.pull(schema=schema)
    except Exception:
        pass

    try:
        df_rt_load = load_rt_metered_hourly.pull(schema=schema)
        source_checks.append(validate_load(df_da_load, df_rt_load))
    except Exception as e:
        source_checks.append(
            SourceCheckResult(name="Load", passed=False, issues=[str(e)])
        )

    # Gas
    logger.info("Validating Gas Prices...")
    try:
        df_gas = gas_prices.pull()
        source_checks.append(validate_gas(df_gas))
    except Exception as e:
        source_checks.append(
            SourceCheckResult(name="Gas Prices", passed=False, issues=[str(e)])
        )

    # Weather
    logger.info("Validating Weather...")
    try:
        df_weather = weather_hourly.pull()
        source_checks.append(validate_weather(df_weather))
    except Exception as e:
        source_checks.append(
            SourceCheckResult(name="Weather", passed=False, issues=[str(e)])
        )

    # ── 2. Build & validate feature matrix ──
    logger.info("Building feature matrix for validation...")
    feature_check = None
    drift_share = None
    report_path = None

    try:
        df_features = build_daily_features(schema=schema)
        feature_check, drift_share, report_path = validate_feature_matrix(
            df_features,
            drift_threshold=drift_threshold,
            max_missing_share=max_missing_share,
            output_dir=output_dir,
        )
    except Exception as e:
        feature_check = SourceCheckResult(
            name="Feature Matrix", passed=False, issues=[f"Build failed: {e}"]
        )

    # ── 3. Aggregate results ──
    all_passed = all(c.passed for c in source_checks)
    if feature_check:
        all_passed = all_passed and feature_check.passed

    summary_lines = ["PREFLIGHT VALIDATION SUMMARY", "=" * 40]
    for check in source_checks:
        status = "PASS" if check.passed else "FAIL"
        summary_lines.append(f"  [{status}] {check.name}: {check.row_count:,} rows")
        if check.date_range:
            summary_lines.append(f"         Range: {check.date_range}")
        for issue in check.issues:
            summary_lines.append(f"         ISSUE: {issue}")
        for warn in check.warnings:
            summary_lines.append(f"         WARN:  {warn}")

    if feature_check:
        status = "PASS" if feature_check.passed else "FAIL"
        summary_lines.append(
            f"  [{status}] {feature_check.name}: {feature_check.row_count:,} rows"
        )
        if feature_check.date_range:
            summary_lines.append(f"         Range: {feature_check.date_range}")
        if drift_share is not None:
            summary_lines.append(f"         Drift share: {drift_share:.1%}")
        for issue in feature_check.issues:
            summary_lines.append(f"         ISSUE: {issue}")
        for warn in feature_check.warnings:
            summary_lines.append(f"         WARN:  {warn}")

    summary_lines.append("=" * 40)
    summary_lines.append(f"OVERALL: {'PASS' if all_passed else 'FAIL'}")
    if report_path:
        summary_lines.append(f"Report: {report_path}")

    summary = "\n".join(summary_lines)
    logger.info("\n" + summary)

    return PreflightResult(
        passed=all_passed,
        source_checks=source_checks,
        feature_check=feature_check,
        drift_share=drift_share,
        report_path=report_path,
        summary=summary,
    )
