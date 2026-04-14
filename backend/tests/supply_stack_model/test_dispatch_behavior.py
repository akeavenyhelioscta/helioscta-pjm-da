"""Layer 3: Dispatch behavior tests — boundary conditions and price mechanics."""
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


def _build_merit(outage_mw: float = 0.0) -> pd.DataFrame:
    return build_merit_order(
        fleet_df=_sample_fleet(),
        gas_price_usd_mmbtu=4.0,
        outage_mw=outage_mw,
    )


# ---------------------------------------------------------------------------
# Clearing price = marginal cost + adder
# ---------------------------------------------------------------------------


def test_clearing_price_equals_marginal_cost_plus_adder() -> None:
    merit = _build_merit()
    adder = 3.0
    result = dispatch(merit, net_load_mw=2500.0, congestion_adder_usd=adder)
    expected = result["marginal_variable_cost"] + adder
    assert abs(result["price"] - expected) < 0.01


def test_clearing_price_with_zero_adder() -> None:
    merit = _build_merit()
    result = dispatch(merit, net_load_mw=2500.0, congestion_adder_usd=0.0)
    assert abs(result["price"] - result["marginal_variable_cost"]) < 0.01


# ---------------------------------------------------------------------------
# Boundary: net_load exactly at cumulative capacity breakpoint
# ---------------------------------------------------------------------------


def test_dispatch_at_exact_cumulative_breakpoint() -> None:
    merit = _build_merit()
    # Nuclear capacity = 1000 MW. At exactly 1000 MW net load,
    # nuclear should be the marginal unit.
    result = dispatch(merit, net_load_mw=1000.0)
    assert result["dispatch_status"] == "balanced"
    assert result["marginal_fuel"] == "nuclear"


def test_dispatch_one_mw_above_breakpoint() -> None:
    merit = _build_merit()
    # 1001 MW requires the next block (cc_gas)
    result = dispatch(merit, net_load_mw=1001.0)
    assert result["dispatch_status"] == "balanced"
    assert result["marginal_fuel"] == "cc_gas"


# ---------------------------------------------------------------------------
# Zero net load → oversupplied
# ---------------------------------------------------------------------------


def test_zero_net_load_returns_oversupplied() -> None:
    merit = _build_merit()
    result = dispatch(merit, net_load_mw=0.0)
    assert result["dispatch_status"] == "oversupplied"
    assert result["shortage_mw"] == 0.0


def test_negative_net_load_returns_oversupplied() -> None:
    merit = _build_merit()
    result = dispatch(merit, net_load_mw=-500.0)
    assert result["dispatch_status"] == "oversupplied"


# ---------------------------------------------------------------------------
# Scarcity: net load > total available
# ---------------------------------------------------------------------------


def test_scarcity_returns_short_capacity_status() -> None:
    merit = _build_merit()
    total = float(merit["available_capacity_mw"].sum())
    result = dispatch(merit, net_load_mw=total + 1000.0)
    assert result["dispatch_status"] == "short_capacity"
    assert result["shortage_mw"] > 0


def test_scarcity_price_capped() -> None:
    merit = _build_merit()
    total = float(merit["available_capacity_mw"].sum())
    cap = 500.0
    result = dispatch(
        merit, net_load_mw=total + 5000.0, scarcity_price_cap_usd_mwh=cap
    )
    assert result["price"] <= cap + 10.0  # adder tolerance


# ---------------------------------------------------------------------------
# Reserve margin
# ---------------------------------------------------------------------------


def test_reserve_margin_equals_available_minus_demand() -> None:
    merit = _build_merit()
    demand = 2500.0
    result = dispatch(merit, net_load_mw=demand)
    total_available = float(merit["available_capacity_mw"].sum())
    expected = total_available - demand
    assert abs(result["reserve_margin_mw"] - expected) < 0.01


def test_stack_position_pct_between_0_and_1_when_balanced() -> None:
    merit = _build_merit()
    result = dispatch(merit, net_load_mw=2500.0)
    assert result["stack_position_pct"] is not None
    assert 0.0 < result["stack_position_pct"] < 1.0
