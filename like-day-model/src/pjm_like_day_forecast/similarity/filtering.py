"""Pre-filtering for candidate reduction before distance computation.

Two-stage approach:
1. Calendar filter: same DOW group, seasonal proximity
2. Regime filter: exclude dates in fundamentally different price/gas regimes
"""
import pandas as pd
import numpy as np
import logging
from datetime import date

from pjm_like_day_forecast import configs

logger = logging.getLogger(__name__)


def calendar_filter(
    df: pd.DataFrame,
    target_date: date,
    same_dow_group: bool = configs.FILTER_SAME_DOW_GROUP,
    season_window_days: int = configs.FILTER_SEASON_WINDOW_DAYS,
) -> pd.DataFrame:
    """Pre-filter historical dates by calendar proximity.

    Args:
        df: Daily feature DataFrame (must include 'date', 'dow_group').
        target_date: The target forecast date.
        same_dow_group: If True, only match within the same DOW group.
        season_window_days: +/- days from target's day-of-year.

    Returns:
        Filtered DataFrame.
    """
    filtered = df.copy()

    # Exclude the target date itself
    filtered = filtered[filtered["date"] != target_date]

    # Exclude future dates
    filtered = filtered[filtered["date"] < target_date]

    # DOW group filter
    if same_dow_group and "dow_group" in filtered.columns:
        target_row = df[df["date"] == target_date]
        if len(target_row) > 0:
            target_dow_group = target_row["dow_group"].iloc[0]
            filtered = filtered[filtered["dow_group"] == target_dow_group]

    # Seasonal proximity: +/- N days from target's day-of-year
    if season_window_days > 0:
        target_doy = pd.Timestamp(target_date).dayofyear
        candidate_doy = pd.to_datetime(filtered["date"]).dt.dayofyear

        # Handle wrap-around (e.g., target DOY 10, window 30 should include DOY 345-365 + 1-40)
        diff = np.abs(candidate_doy - target_doy)
        diff = np.minimum(diff, 365 - diff)
        filtered = filtered[diff <= season_window_days]

    before = len(df)
    after = len(filtered)
    logger.info(f"Calendar filter: {before:,} -> {after:,} candidates "
                f"(DOW group={same_dow_group}, season_window={season_window_days}d)")

    return filtered


def regime_filter(
    df: pd.DataFrame,
    target_date: date,
    lmp_col: str = "lmp_daily_flat",
    gas_col: str = "gas_m3_price",
    lmp_tolerance_std: float = 1.5,
    gas_tolerance_std: float = 1.5,
    df_full: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Filter out dates in fundamentally different price/gas regimes.

    A $20 avg LMP day (2020 COVID) is never a good analog for a $120 winter spike.

    Args:
        df: Daily feature DataFrame (candidate pool, may not contain target).
        target_date: The target forecast date.
        lmp_col: Column for daily LMP level.
        gas_col: Column for gas price level.
        lmp_tolerance_std: Number of standard deviations tolerance for LMP.
        gas_tolerance_std: Number of standard deviations tolerance for gas.
        df_full: Full unfiltered feature matrix (used to look up target features
                 when target has been removed from df by prior filtering).

    Returns:
        Filtered DataFrame.
    """
    # Look up target features — try df first, then fall back to df_full
    target_row = df[df["date"] == target_date]
    if len(target_row) == 0 and df_full is not None:
        target_row = df_full[df_full["date"] == target_date]
    if len(target_row) == 0:
        logger.warning(f"Target date {target_date} not found in feature matrix")
        return df[df["date"] != target_date]

    filtered = df[df["date"] != target_date].copy()
    filtered = filtered[filtered["date"] < target_date]

    # LMP regime filter
    if lmp_col in filtered.columns and lmp_col in target_row.columns:
        target_lmp = target_row[lmp_col].iloc[0]
        if not np.isnan(target_lmp):
            lmp_mean = filtered[lmp_col].mean()
            lmp_std = filtered[lmp_col].std()
            if lmp_std > 0:
                z_target = abs(target_lmp - lmp_mean) / lmp_std
                z_candidates = np.abs(filtered[lmp_col] - lmp_mean) / lmp_std
                # Keep candidates within tolerance of the target's regime
                z_diff = np.abs(z_candidates - z_target)
                filtered = filtered[z_diff <= lmp_tolerance_std]

    # Gas regime filter
    if gas_col in filtered.columns and gas_col in target_row.columns:
        target_gas = target_row[gas_col].iloc[0]
        if not np.isnan(target_gas):
            gas_mean = filtered[gas_col].mean()
            gas_std = filtered[gas_col].std()
            if gas_std > 0:
                z_target = abs(target_gas - gas_mean) / gas_std
                z_candidates = np.abs(filtered[gas_col] - gas_mean) / gas_std
                z_diff = np.abs(z_candidates - z_target)
                filtered = filtered[z_diff <= gas_tolerance_std]

    logger.info(f"Regime filter: {len(df):,} -> {len(filtered):,} candidates")
    return filtered


def ensure_minimum_pool(
    df_filtered: pd.DataFrame,
    df_full: pd.DataFrame,
    target_date: date,
    min_size: int = configs.FILTER_MIN_POOL_SIZE,
) -> pd.DataFrame:
    """If filtering reduced the pool below min_size, relax constraints.

    Progressively relaxes filters by returning more of the original pool,
    sorted by date proximity to the target.

    Args:
        df_filtered: The filtered candidate pool.
        df_full: The full unfiltered historical data.
        target_date: The target forecast date.
        min_size: Minimum number of candidates required.

    Returns:
        DataFrame with at least min_size candidates (if available in df_full).
    """
    if len(df_filtered) >= min_size:
        return df_filtered

    logger.warning(f"Pool too small ({len(df_filtered)} < {min_size}), relaxing filters")

    # Fall back to full historical pool (excluding target and future dates)
    fallback = df_full[
        (df_full["date"] != target_date) &
        (df_full["date"] < target_date)
    ].copy()

    # Sort by date proximity to target
    fallback["_date_diff"] = np.abs(
        (pd.to_datetime(fallback["date"]) - pd.Timestamp(target_date)).dt.days
    )
    fallback = fallback.sort_values("_date_diff")

    # Take at least min_size
    result = fallback.head(max(min_size, len(df_filtered)))
    result = result.drop(columns=["_date_diff"])

    logger.info(f"Relaxed pool: {len(result):,} candidates")
    return result
