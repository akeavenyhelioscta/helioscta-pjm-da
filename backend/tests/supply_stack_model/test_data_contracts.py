"""Layer 1: Data contract tests for supply stack model sources."""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.supply_stack_model.data.fleet import REQUIRED_FLEET_COLUMNS, load_fleet
from src.supply_stack_model.data.sources import (
    align_gas_to_electric_day,
    build_hourly_inputs,
    select_outage_for_date,
)

REQUIRED_HOURLY_COLUMNS = {
    "date",
    "hour_ending",
    "load_mw",
    "solar_mw",
    "wind_mw",
    "net_load_mw",
    "gas_price_usd_mmbtu",
    "outages_mw",
}


# ---------------------------------------------------------------------------
# Fleet CSV contract
# ---------------------------------------------------------------------------


def test_fleet_csv_has_all_required_columns() -> None:
    df = load_fleet()
    assert REQUIRED_FLEET_COLUMNS.issubset(set(df.columns))


def test_fleet_csv_no_nan_in_numeric_columns() -> None:
    df = load_fleet()
    for col in ("capacity_mw", "heat_rate_mmbtu_mwh", "vom_usd_mwh"):
        assert not df[col].isna().any(), f"NaN found in fleet column {col}"


def test_fleet_csv_capacity_positive() -> None:
    df = load_fleet()
    assert (df["capacity_mw"] > 0).all(), "All fleet blocks must have capacity > 0"


def test_fleet_csv_heat_rate_non_negative() -> None:
    df = load_fleet()
    assert (df["heat_rate_mmbtu_mwh"] >= 0).all(), "Heat rates must be >= 0"


def test_fleet_csv_vom_non_negative() -> None:
    df = load_fleet()
    assert (df["vom_usd_mwh"] >= 0).all(), "VOM must be >= 0"


def test_fleet_csv_block_ids_unique() -> None:
    df = load_fleet()
    assert df["block_id"].nunique() == len(df), "Block IDs must be unique"


# ---------------------------------------------------------------------------
# build_hourly_inputs contract
# ---------------------------------------------------------------------------


def _make_test_inputs(target: date):
    prev = target - timedelta(days=1)
    df_load = pd.DataFrame(
        {
            "forecast_date": [target] * 24,
            "hour_ending": list(range(1, 25)),
            "forecast_load_mw": [90000.0 + h for h in range(1, 25)],
        }
    )
    df_solar = pd.DataFrame(
        {
            "date": [target] * 24,
            "hour_ending": list(range(1, 25)),
            "solar_forecast": [2000.0] * 24,
        }
    )
    df_wind = pd.DataFrame(
        {
            "date": [target] * 24,
            "hour_ending": list(range(1, 25)),
            "wind_forecast": [3000.0] * 24,
        }
    )
    gas_rows = []
    for d, px in [(prev, 3.0), (target, 4.0)]:
        for h in range(1, 25):
            gas_rows.append({"date": d, "hour_ending": h, "gas_m3": px})
    df_gas = pd.DataFrame(gas_rows)
    df_outages = pd.DataFrame(
        {
            "forecast_date": [target],
            "forecast_execution_date": [prev],
            "forecast_rank": [1],
            "region": ["RTO"],
            "total_outages_mw": [12000.0],
        }
    )
    return df_load, df_solar, df_wind, df_gas, df_outages


def test_hourly_inputs_returns_24_rows() -> None:
    target = date(2026, 4, 14)
    df_load, df_solar, df_wind, df_gas, df_outages = _make_test_inputs(target)
    out = build_hourly_inputs(
        target_date=target,
        df_load=df_load,
        df_solar=df_solar,
        df_wind=df_wind,
        df_gas=df_gas,
        df_outages=df_outages,
    )
    assert len(out) == 24


def test_hourly_inputs_has_all_required_columns() -> None:
    target = date(2026, 4, 14)
    df_load, df_solar, df_wind, df_gas, df_outages = _make_test_inputs(target)
    out = build_hourly_inputs(
        target_date=target,
        df_load=df_load,
        df_solar=df_solar,
        df_wind=df_wind,
        df_gas=df_gas,
        df_outages=df_outages,
    )
    assert REQUIRED_HOURLY_COLUMNS.issubset(set(out.columns))


def test_hourly_inputs_hour_endings_complete() -> None:
    target = date(2026, 4, 14)
    df_load, df_solar, df_wind, df_gas, df_outages = _make_test_inputs(target)
    out = build_hourly_inputs(
        target_date=target,
        df_load=df_load,
        df_solar=df_solar,
        df_wind=df_wind,
        df_gas=df_gas,
        df_outages=df_outages,
    )
    assert sorted(out["hour_ending"].tolist()) == list(range(1, 25))


def test_hourly_inputs_no_nulls_in_critical_columns() -> None:
    target = date(2026, 4, 14)
    df_load, df_solar, df_wind, df_gas, df_outages = _make_test_inputs(target)
    out = build_hourly_inputs(
        target_date=target,
        df_load=df_load,
        df_solar=df_solar,
        df_wind=df_wind,
        df_gas=df_gas,
        df_outages=df_outages,
    )
    for col in ("load_mw", "gas_price_usd_mmbtu", "net_load_mw", "outages_mw"):
        assert not out[col].isna().any(), f"Null found in {col}"


def test_hourly_inputs_numeric_dtypes() -> None:
    target = date(2026, 4, 14)
    df_load, df_solar, df_wind, df_gas, df_outages = _make_test_inputs(target)
    out = build_hourly_inputs(
        target_date=target,
        df_load=df_load,
        df_solar=df_solar,
        df_wind=df_wind,
        df_gas=df_gas,
        df_outages=df_outages,
    )
    for col in ("load_mw", "solar_mw", "wind_mw", "net_load_mw", "gas_price_usd_mmbtu"):
        assert pd.api.types.is_numeric_dtype(out[col]), f"{col} should be numeric"


# ---------------------------------------------------------------------------
# Gas HE alignment contract
# ---------------------------------------------------------------------------


def test_gas_alignment_produces_complete_24_hours_for_target_date() -> None:
    target = date(2026, 4, 14)
    prev = target - timedelta(days=1)
    rows = []
    for d in [prev, target]:
        for h in range(1, 25):
            rows.append({"date": d, "hour_ending": h, "gas_m3": 3.0})
    df = pd.DataFrame(rows)
    aligned = align_gas_to_electric_day(df)
    aligned["date"] = pd.to_datetime(aligned["date"]).dt.date
    day = aligned[aligned["date"] == target]
    assert sorted(day["hour_ending"].tolist()) == list(range(1, 25))


# ---------------------------------------------------------------------------
# Outage selection determinism
# ---------------------------------------------------------------------------


def test_outage_selection_is_deterministic_across_multiple_calls() -> None:
    target = date(2026, 4, 14)
    df = pd.DataFrame(
        {
            "forecast_date": [target] * 3,
            "forecast_execution_date": [
                date(2026, 4, 12),
                date(2026, 4, 13),
                date(2026, 4, 13),
            ],
            "forecast_rank": [1, 2, 3],
            "region": ["RTO"] * 3,
            "total_outages_mw": [10000, 11000, 12000],
        }
    )
    results = [select_outage_for_date(df, target, region="RTO") for _ in range(5)]
    assert len(set(results)) == 1, "Outage selection must be deterministic"
