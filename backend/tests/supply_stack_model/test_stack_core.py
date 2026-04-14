from __future__ import annotations

import pandas as pd

from src.supply_stack_model.stack.dispatch import dispatch
from src.supply_stack_model.stack.merit_order import build_merit_order


def _sample_fleet() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "block_id": "nuclear",
                "fuel_type": "nuclear",
                "capacity_mw": 1000.0,
                "heat_rate_mmbtu_mwh": 0.0,
                "vom_usd_mwh": 2.0,
                "must_run": True,
                "gas_hub": "",
                "outage_weight": 0.0,
            },
            {
                "block_id": "cc",
                "fuel_type": "cc_gas",
                "capacity_mw": 2000.0,
                "heat_rate_mmbtu_mwh": 7.0,
                "vom_usd_mwh": 2.0,
                "must_run": False,
                "gas_hub": "gas_m3",
                "outage_weight": 1.0,
            },
            {
                "block_id": "ct",
                "fuel_type": "ct_gas",
                "capacity_mw": 500.0,
                "heat_rate_mmbtu_mwh": 10.0,
                "vom_usd_mwh": 4.0,
                "must_run": False,
                "gas_hub": "gas_m3",
                "outage_weight": 1.0,
            },
        ]
    )


def test_build_merit_order_orders_by_cost_and_derates() -> None:
    merit = build_merit_order(
        fleet_df=_sample_fleet(),
        gas_price_usd_mmbtu=3.0,
        outage_mw=300.0,
        coal_price_usd_mmbtu=2.5,
        oil_price_usd_mmbtu=15.0,
    )

    assert merit["block_id"].tolist()[0] == "nuclear"
    assert merit["variable_cost_usd_mwh"].tolist()[1] < merit["variable_cost_usd_mwh"].tolist()[2]

    # 300 MW derate is split pro-rata by outage_weight (both 1.0) → 150 each
    cc_cap = float(merit.loc[merit["block_id"] == "cc", "available_capacity_mw"].iloc[0])
    ct_cap = float(merit.loc[merit["block_id"] == "ct", "available_capacity_mw"].iloc[0])
    assert round(cc_cap, 1) == 1850.0
    assert round(ct_cap, 1) == 350.0


def test_dispatch_returns_marginal_fuel_and_shortage() -> None:
    merit = build_merit_order(
        fleet_df=_sample_fleet(),
        gas_price_usd_mmbtu=4.0,
        outage_mw=0.0,
        coal_price_usd_mmbtu=2.5,
        oil_price_usd_mmbtu=15.0,
    )
    balanced = dispatch(
        merit_order_df=merit,
        net_load_mw=2500.0,
        congestion_adder_usd=3.0,
        scarcity_price_cap_usd_mwh=500.0,
    )
    assert balanced["dispatch_status"] == "balanced"
    assert balanced["marginal_fuel"] == "cc_gas"
    assert balanced["price"] > balanced["marginal_variable_cost"]

    shortage = dispatch(
        merit_order_df=merit,
        net_load_mw=5000.0,
        congestion_adder_usd=0.0,
        scarcity_price_cap_usd_mwh=500.0,
    )
    assert shortage["dispatch_status"] == "short_capacity"
    assert shortage["shortage_mw"] > 0.0
