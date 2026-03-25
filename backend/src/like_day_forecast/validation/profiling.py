"""ydata-profiling report generation for like-day forecast data sources.

Generates rich HTML reports with distributions, correlations, missing values,
and dataset comparison for EDA and data quality monitoring.

Usage:
    from src.like_day_forecast.validation.profiling import profile_source
    path = profile_source(df, "lmp_hourly")
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from ydata_profiling import ProfileReport

logger = logging.getLogger(__name__)

REPORTS_DIR = Path(__file__).parent.parent / "reports" / "profiling"


def profile_source(
    df: pd.DataFrame,
    source_name: str,
    recent_days: int = 90,
    output_dir: Path | None = None,
) -> Path:
    """Generate a ydata-profiling report for a single raw data source.

    Filters to recent window for speed, then saves an HTML report.

    Args:
        df: Raw source DataFrame.
        source_name: Name used in the report title and filename.
        recent_days: Filter to this many recent days (0 = use all data).
        output_dir: Override output directory (default: reports/profiling/).

    Returns:
        Path to the saved HTML report.
    """
    out_dir = output_dir or REPORTS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    # Filter to recent window if date column exists
    if "date" in df.columns and recent_days > 0:
        cutoff = date.today() - timedelta(days=recent_days)
        df_recent = df[df["date"] >= cutoff].copy()
        if len(df_recent) == 0:
            logger.warning(
                f"{source_name}: no data in last {recent_days} days, "
                "profiling all data"
            )
            df_recent = df.copy()
    else:
        df_recent = df.copy()

    logger.info(f"Profiling {source_name}: {len(df_recent):,} rows")

    profile = ProfileReport(
        df_recent,
        title=f"{source_name} — {date.today().isoformat()}",
        minimal=True,
    )

    report_path = out_dir / f"{source_name}_{date.today().isoformat()}.html"
    profile.to_file(report_path)
    logger.info(f"Saved profile report: {report_path}")
    return report_path


def profile_feature_matrix(
    df_features: pd.DataFrame,
    output_dir: Path | None = None,
) -> Path:
    """Generate a ydata-profiling report for the full feature matrix.

    Args:
        df_features: Daily feature matrix from build_daily_features().
        output_dir: Override output directory.

    Returns:
        Path to the saved HTML report.
    """
    out_dir = output_dir or REPORTS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        f"Profiling feature matrix: {len(df_features):,} rows, "
        f"{len(df_features.columns)} cols"
    )

    profile = ProfileReport(
        df_features,
        title=f"Feature Matrix — {date.today().isoformat()}",
        minimal=True,
    )

    report_path = out_dir / f"feature_matrix_{date.today().isoformat()}.html"
    profile.to_file(report_path)
    logger.info(f"Saved feature matrix profile: {report_path}")
    return report_path


def profile_feature_matrix_comparison(
    df_features: pd.DataFrame,
    reference_days: int = 365,
    output_dir: Path | None = None,
) -> Path:
    """Generate a comparison profile: reference window vs. recent data.

    Splits the feature matrix into a reference window (older data) and a
    current window (recent data), then uses ProfileReport.compare() for
    side-by-side comparison.

    Args:
        df_features: Full daily feature matrix.
        reference_days: Days of older data used as reference window.
        output_dir: Override output directory.

    Returns:
        Path to the saved HTML comparison report.
    """
    out_dir = output_dir or REPORTS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    max_date = pd.Series(df_features["date"]).max()
    if hasattr(max_date, "date"):
        max_date = max_date.date()

    cutoff = max_date - timedelta(days=reference_days)
    df_ref = df_features[df_features["date"] < cutoff].copy()
    df_cur = df_features[df_features["date"] >= cutoff].copy()

    logger.info(
        f"Comparison profile: ref={len(df_ref):,} rows (before {cutoff}), "
        f"current={len(df_cur):,} rows (after {cutoff})"
    )

    profile_ref = ProfileReport(
        df_ref,
        title=f"Reference (before {cutoff})",
        minimal=True,
    )
    profile_cur = ProfileReport(
        df_cur,
        title=f"Current (after {cutoff})",
        minimal=True,
    )

    comparison = profile_ref.compare(profile_cur)

    report_path = (
        out_dir / f"feature_matrix_comparison_{date.today().isoformat()}.html"
    )
    comparison.to_file(report_path)
    logger.info(f"Saved comparison report: {report_path}")
    return report_path
