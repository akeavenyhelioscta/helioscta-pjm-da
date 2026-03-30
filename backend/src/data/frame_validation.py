"""Shared DataFrame contract checks for source pull modules."""
from __future__ import annotations

import logging
from typing import Iterable

import pandas as pd

logger = logging.getLogger(__name__)


def validate_source_frame(
    df: pd.DataFrame,
    source_name: str,
    required_columns: Iterable[str] | None = None,
    unique_key_columns: Iterable[str] | None = None,
    hourly_coverage_date_col: str | None = None,
    hourly_coverage_hour_col: str | None = None,
    expected_hours_per_day: int = 24,
    drop_duplicate_keys: bool = False,
) -> pd.DataFrame:
    """Validate and optionally clean source pull output.

    Args:
        df: Source output DataFrame.
        source_name: Human-readable source label for logs/errors.
        required_columns: Columns that must exist in *df*.
        unique_key_columns: Columns that should uniquely identify rows.
        hourly_coverage_date_col: Date column for per-day hour coverage checks.
        hourly_coverage_hour_col: Hour column for per-day hour coverage checks.
        expected_hours_per_day: Expected distinct hour count per day.
        drop_duplicate_keys: If True, drop duplicate rows by unique keys.

    Returns:
        DataFrame (possibly de-duplicated when enabled).
    """
    result = df

    if required_columns:
        missing = [c for c in required_columns if c not in result.columns]
        if missing:
            raise ValueError(f"{source_name}: missing required columns: {missing}")

    if unique_key_columns:
        key_cols = list(unique_key_columns)
        dup_mask = result.duplicated(subset=key_cols, keep="first")
        dup_count = int(dup_mask.sum())
        if dup_count > 0:
            logger.warning(
                f"{source_name}: found {dup_count:,} duplicate rows on keys {key_cols}"
            )
            if drop_duplicate_keys:
                result = result.loc[~dup_mask].copy()
                logger.warning(
                    f"{source_name}: dropped duplicates, rows now {len(result):,}"
                )

    if hourly_coverage_date_col and hourly_coverage_hour_col:
        coverage = (
            result.groupby(hourly_coverage_date_col)[hourly_coverage_hour_col]
            .nunique()
            .reset_index(name="hour_count")
        )
        incomplete = coverage[coverage["hour_count"] != expected_hours_per_day]
        if not incomplete.empty:
            n_days = len(incomplete)
            sample = incomplete.head(3).to_dict("records")
            logger.warning(
                f"{source_name}: {n_days:,} days with != {expected_hours_per_day} "
                f"hours; sample={sample}"
            )

    return result

