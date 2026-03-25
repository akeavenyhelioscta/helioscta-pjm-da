"""Great Expectations validation suites for like-day forecast data sources.

Uses GX Ephemeral mode — no YAML, no filesystem Data Context.
Each suite function takes a DataFrame, returns a CheckpointResult.

Usage:
    from src.like_day_forecast.validation.expectations import validate_lmp_hourly
    result = validate_lmp_hourly(df)
    print(result.success)
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

import pandas as pd
import great_expectations as gx
from great_expectations.expectations import (
    ExpectColumnToExist,
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToNotBeNull,
    ExpectTableColumnCountToBeBetween,
    ExpectTableRowCountToBeBetween,
)

logger = logging.getLogger(__name__)


# ─── Helper ──────────────────────────────────────────────────────────────


def _validate_df(
    df: pd.DataFrame,
    suite_name: str,
    expectations: list,
) -> Any:
    """Run a GX expectation suite against a DataFrame in ephemeral mode.

    Returns a CheckpointResult with .success (bool) and detailed results.
    """
    context = gx.get_context(mode="ephemeral")

    # Build suite
    suite = gx.ExpectationSuite(name=suite_name)
    for exp in expectations:
        suite.add_expectation(exp)
    suite = context.suites.add(suite)

    # Wire up: data source → asset → batch definition
    datasource = context.data_sources.add_pandas(f"{suite_name}_ds")
    asset = datasource.add_dataframe_asset(f"{suite_name}_asset")
    batch_def = asset.add_batch_definition_whole_dataframe(f"{suite_name}_batch")

    # Validation definition + checkpoint
    validation_def = context.validation_definitions.add(
        gx.ValidationDefinition(
            name=f"{suite_name}_val",
            data=batch_def,
            suite=suite,
        )
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name=f"{suite_name}_cp",
            validation_definitions=[validation_def],
        )
    )

    result = checkpoint.run(batch_parameters={"dataframe": df})
    return result


# ─── Raw source suites ───────────────────────────────────────────────────


def validate_lmp_hourly(df: pd.DataFrame) -> Any:
    """GX suite for LMP hourly data.

    Columns: date, hour_ending, hub, market, lmp_total,
             lmp_system_energy_price, lmp_congestion_price,
             lmp_marginal_loss_price
    """
    expectations = [
        # All 8 columns exist
        ExpectColumnToExist(column="date"),
        ExpectColumnToExist(column="hour_ending"),
        ExpectColumnToExist(column="hub"),
        ExpectColumnToExist(column="market"),
        ExpectColumnToExist(column="lmp_total"),
        ExpectColumnToExist(column="lmp_system_energy_price"),
        ExpectColumnToExist(column="lmp_congestion_price"),
        ExpectColumnToExist(column="lmp_marginal_loss_price"),
        # Value ranges
        ExpectColumnValuesToBeBetween(
            column="lmp_total", min_value=-500, max_value=3000
        ),
        ExpectColumnValuesToBeBetween(
            column="lmp_congestion_price", min_value=-500, max_value=3000
        ),
        ExpectColumnValuesToBeBetween(
            column="lmp_marginal_loss_price", min_value=-200, max_value=200
        ),
        ExpectColumnValuesToBeBetween(
            column="hour_ending", min_value=1, max_value=24
        ),
        # Null checks
        ExpectColumnValuesToNotBeNull(column="lmp_total", mostly=0.95),
        # Row count
        ExpectTableRowCountToBeBetween(min_value=1000, max_value=200000),
    ]
    logger.info("Running GX suite: lmp_hourly")
    return _validate_df(df, "lmp_hourly", expectations)


def validate_load_da_hourly(df: pd.DataFrame) -> Any:
    """GX suite for DA load hourly data.

    Columns: date, hour_ending, region, da_load_mw
    """
    expectations = [
        ExpectColumnToExist(column="date"),
        ExpectColumnToExist(column="hour_ending"),
        ExpectColumnToExist(column="region"),
        ExpectColumnToExist(column="da_load_mw"),
        ExpectColumnValuesToBeBetween(
            column="da_load_mw", min_value=0, max_value=250000
        ),
        ExpectColumnValuesToBeBetween(
            column="hour_ending", min_value=1, max_value=24
        ),
        ExpectColumnValuesToNotBeNull(column="da_load_mw", mostly=0.95),
    ]
    logger.info("Running GX suite: load_da_hourly")
    return _validate_df(df, "load_da_hourly", expectations)


def validate_load_rt_metered_hourly(df: pd.DataFrame) -> Any:
    """GX suite for RT metered load hourly data.

    Columns: date, hour_ending, region, rt_load_mw
    """
    expectations = [
        ExpectColumnToExist(column="date"),
        ExpectColumnToExist(column="hour_ending"),
        ExpectColumnToExist(column="region"),
        ExpectColumnToExist(column="rt_load_mw"),
        ExpectColumnValuesToBeBetween(
            column="rt_load_mw", min_value=0, max_value=250000
        ),
        ExpectColumnValuesToNotBeNull(column="rt_load_mw", mostly=0.95),
    ]
    logger.info("Running GX suite: load_rt_metered_hourly")
    return _validate_df(df, "load_rt_metered_hourly", expectations)


def validate_gas_prices(df: pd.DataFrame) -> Any:
    """GX suite for gas price data.

    Columns: date, gas_m3_price, gas_hh_price
    """
    expectations = [
        ExpectColumnToExist(column="date"),
        ExpectColumnToExist(column="gas_m3_price"),
        ExpectColumnToExist(column="gas_hh_price"),
        ExpectColumnValuesToBeBetween(
            column="gas_m3_price", min_value=-5, max_value=100, mostly=0.999
        ),
        ExpectColumnValuesToBeBetween(
            column="gas_hh_price", min_value=-5, max_value=100, mostly=0.999
        ),
        ExpectColumnValuesToNotBeNull(column="gas_m3_price", mostly=0.90),
        ExpectColumnValuesToNotBeNull(column="gas_hh_price", mostly=0.90),
    ]
    logger.info("Running GX suite: gas_prices")
    return _validate_df(df, "gas_prices", expectations)


def validate_weather_hourly(df: pd.DataFrame) -> Any:
    """GX suite for weather hourly data.

    Columns: date, hour_ending, station_name, temp
    (SQL aliases 'temperature' → 'temp')
    """
    expectations = [
        ExpectColumnToExist(column="date"),
        ExpectColumnToExist(column="hour_ending"),
        ExpectColumnToExist(column="station_name"),
        ExpectColumnToExist(column="temp"),
        ExpectColumnValuesToBeBetween(
            column="temp", min_value=-40, max_value=130
        ),
        ExpectColumnValuesToNotBeNull(column="temp", mostly=0.90),
    ]
    logger.info("Running GX suite: weather_hourly")
    return _validate_df(df, "weather_hourly", expectations)


def validate_dates_daily(df: pd.DataFrame) -> Any:
    """GX suite for dates/calendar daily data.

    Columns: date, day_of_week_number, is_weekend, is_nerc_holiday, summer_winter
    """
    expectations = [
        ExpectColumnToExist(column="date"),
        ExpectColumnToExist(column="day_of_week_number"),
        ExpectColumnToExist(column="is_weekend"),
        ExpectColumnToExist(column="is_nerc_holiday"),
        ExpectColumnToExist(column="summer_winter"),
        ExpectColumnValuesToBeBetween(
            column="day_of_week_number", min_value=0, max_value=6
        ),
    ]
    logger.info("Running GX suite: dates_daily")
    return _validate_df(df, "dates_daily", expectations)


# ─── Feature matrix suite ────────────────────────────────────────────────


def validate_feature_matrix(df: pd.DataFrame) -> Any:
    """GX suite for the assembled daily feature matrix.

    Checks column/row counts, key columns, LMP profile columns,
    and null rates across feature columns.
    """
    key_cols = [
        "date",
        "gas_m3_price",
        "load_daily_avg",
        "temp_daily_avg",
        "hdd",
        "cdd",
        "implied_heat_rate",
    ]
    profile_cols = [f"lmp_profile_h{h}" for h in range(1, 25)]

    expectations = [
        # Shape
        ExpectTableColumnCountToBeBetween(min_value=80, max_value=120),
        ExpectTableRowCountToBeBetween(min_value=500, max_value=5000),
    ]

    # Key columns exist
    for col in key_cols:
        expectations.append(ExpectColumnToExist(column=col))

    # LMP profile columns exist
    for col in profile_cols:
        expectations.append(ExpectColumnToExist(column=col))

    # Not-null checks for all listed non-date columns
    for col in key_cols[1:] + profile_cols:
        expectations.append(
            ExpectColumnValuesToNotBeNull(column=col, mostly=0.80)
        )

    logger.info("Running GX suite: feature_matrix")
    return _validate_df(df, "feature_matrix", expectations)


# ─── Freshness helper ────────────────────────────────────────────────────


def check_freshness(
    df: pd.DataFrame,
    max_stale_days: int = 3,
) -> dict:
    """Check if a DataFrame's max date is within N days of today.

    Returns dict with keys: fresh (bool), max_date (str), stale_days (int).
    """
    if df.empty or "date" not in df.columns:
        return {"fresh": False, "max_date": None, "stale_days": None}

    max_date = pd.Series(df["date"]).max()
    if hasattr(max_date, "date"):
        max_date = max_date.date()

    stale_days = (date.today() - max_date).days
    return {
        "fresh": stale_days <= max_stale_days,
        "max_date": str(max_date),
        "stale_days": stale_days,
    }
