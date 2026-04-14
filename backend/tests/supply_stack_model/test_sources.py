from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from src.supply_stack_model.data.sources import (
    _coerce_generation_forecast_frame,
    _resolve_input_scope,
    align_gas_to_electric_day,
    build_hourly_inputs,
    select_outage_for_date,
)


def test_align_gas_to_electric_day_shifts_he1_to_he9() -> None:
    df = pd.DataFrame(
        {
            "date": [date(2026, 4, 13)] * 4,
            "hour_ending": [1, 9, 10, 24],
            "gas_m3": [2.5, 2.6, 3.5, 3.6],
        }
    )

    out = align_gas_to_electric_day(df)

    he1_date = out.loc[out["hour_ending"] == 1, "date"].iloc[0]
    he9_date = out.loc[out["hour_ending"] == 9, "date"].iloc[0]
    he10_date = out.loc[out["hour_ending"] == 10, "date"].iloc[0]
    he24_date = out.loc[out["hour_ending"] == 24, "date"].iloc[0]

    assert he1_date == date(2026, 4, 14)
    assert he9_date == date(2026, 4, 14)
    assert he10_date == date(2026, 4, 13)
    assert he24_date == date(2026, 4, 13)


def test_select_outage_for_date_uses_latest_execution_then_rank() -> None:
    target = date(2026, 4, 14)
    df = pd.DataFrame(
        {
            "forecast_date": [target, target, target, target],
            "forecast_execution_date": [
                date(2026, 4, 12),
                date(2026, 4, 13),
                date(2026, 4, 13),
                date(2026, 4, 13),
            ],
            "forecast_rank": [1, 1, 3, 2],
            "region": ["RTO", "RTO", "RTO", "WEST"],
            "total_outages_mw": [12000, 12500, 13000, 5000],
        }
    )

    selected = select_outage_for_date(df, target_date=target, region="RTO")
    assert selected == 13000.0


def test_resolve_input_scope_dominion_preset() -> None:
    scope = _resolve_input_scope(region="RTO", region_preset="dominion")
    assert scope["load_source"] == "meteologica"
    assert scope["load_region"] == "SOUTH_DOM"
    assert scope["renewable_source"] == "meteologica"
    assert scope["renewable_solar_region"] == "SOUTH"
    assert scope["renewable_wind_region"] == "SOUTH_DOM"
    assert scope["outage_region"] == "MIDATL_DOM"
    assert scope["default_gas_hub"] == "gas_tz5"


def test_coerce_generation_forecast_frame_from_meteologica_schema() -> None:
    target = date(2026, 4, 14)
    df = pd.DataFrame(
        {
            "forecast_date": [target, target],
            "hour_ending": [1, 2],
            "forecast_generation_mw": [101.5, 202.5],
            "region": ["SOUTH_DOM", "SOUTH_DOM"],
            "source": ["solar", "solar"],
        }
    )
    out = _coerce_generation_forecast_frame(df, output_col="solar_forecast")
    assert list(out.columns) == ["date", "hour_ending", "solar_forecast"]
    assert out["date"].tolist() == [target, target]
    assert out["solar_forecast"].tolist() == [101.5, 202.5]


def test_build_hourly_inputs_merges_and_computes_net_load() -> None:
    target = date(2026, 4, 14)
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
            "forecast_execution_date": [date(2026, 4, 13)],
            "forecast_rank": [2],
            "region": ["RTO"],
            "total_outages_mw": [14000.0],
        }
    )

    out = build_hourly_inputs(
        target_date=target,
        df_load=df_load,
        df_solar=df_solar,
        df_wind=df_wind,
        df_gas=df_gas,
        df_outages=df_outages,
        gas_hub_col="gas_m3",
        outage_column="total_outages_mw",
        region="RTO",
    )

    assert len(out) == 24
    assert out["hour_ending"].tolist() == list(range(1, 25))
    assert (out["outages_mw"] == 14000.0).all()
    assert (out["net_load_mw"] == out["load_mw"] - 5000.0).all()

    he1_gas = float(out.loc[out["hour_ending"] == 1, "gas_price_usd_mmbtu"].iloc[0])
    he10_gas = float(out.loc[out["hour_ending"] == 10, "gas_price_usd_mmbtu"].iloc[0])
    assert he1_gas == 3.0
    assert he10_gas == 4.0


def test_build_hourly_inputs_maps_midnight_hour0_to_he24_for_renewables() -> None:
    target = date(2026, 4, 14)
    prev = target - timedelta(days=1)

    df_load = pd.DataFrame(
        {
            "forecast_date": [target] * 24,
            "hour_ending": list(range(1, 25)),
            "forecast_load_mw": [80000.0] * 24,
        }
    )
    # Source-style encoding: hour_ending 0..23
    df_solar = pd.DataFrame(
        {
            "date": [target] * 24,
            "hour_ending": list(range(0, 24)),
            "solar_forecast": [100.0 + h for h in range(0, 24)],
        }
    )
    df_wind = pd.DataFrame(
        {
            "date": [target] * 24,
            "hour_ending": list(range(0, 24)),
            "wind_forecast": [500.0 + h for h in range(0, 24)],
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
            "forecast_execution_date": [date(2026, 4, 13)],
            "forecast_rank": [1],
            "region": ["RTO"],
            "total_outages_mw": [10000.0],
        }
    )

    out = build_hourly_inputs(
        target_date=target,
        df_load=df_load,
        df_solar=df_solar,
        df_wind=df_wind,
        df_gas=df_gas,
        df_outages=df_outages,
    )

    # HE24 should come from source hour_ending=0
    he24 = out[out["hour_ending"] == 24].iloc[0]
    assert float(he24["solar_mw"]) == 100.0
    assert float(he24["wind_mw"]) == 500.0
