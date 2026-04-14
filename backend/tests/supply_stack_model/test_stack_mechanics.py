"""Layer 2: Stack mechanics tests — merit order invariants and quantile monotonicity."""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.supply_stack_model.data.fleet import apply_outage_derate
from src.supply_stack_model.stack.merit_order import build_merit_order
from src.supply_stack_model.uncertainty.monte_carlo import monte_carlo_dispatch


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
                "block_id": "coal",
                "fuel_type": "coal",
                "capacity_mw": 1500.0,
                "heat_rate_mmbtu_mwh": 10.0,
                "vom_usd_mwh": 4.0,
                "must_run": False,
                "gas_hub": "",
                "outage_weight": 1.0,
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
                "heat_rate_mmbtu_mwh": 10.5,
                "vom_usd_mwh": 3.5,
                "must_run": False,
                "gas_hub": "gas_m3",
                "outage_weight": 1.0,
            },
            {
                "block_id": "oil",
                "fuel_type": "oil",
                "capacity_mw": 300.0,
                "heat_rate_mmbtu_mwh": 11.5,
                "vom_usd_mwh": 5.0,
                "must_run": False,
                "gas_hub": "",
                "outage_weight": 0.7,
            },
        ]
    )


def _build_default_merit(outage_mw: float = 500.0) -> pd.DataFrame:
    return build_merit_order(
        fleet_df=_sample_fleet(),
        gas_price_usd_mmbtu=3.5,
        outage_mw=outage_mw,
        coal_price_usd_mmbtu=2.5,
        oil_price_usd_mmbtu=15.0,
    )


# ---------------------------------------------------------------------------
# Merit order invariants
# ---------------------------------------------------------------------------


def test_merit_order_variable_cost_non_decreasing() -> None:
    merit = _build_default_merit()
    costs = merit["variable_cost_usd_mwh"].tolist()
    for i in range(1, len(costs)):
        assert costs[i] >= costs[i - 1], (
            f"Variable cost decreased at index {i}: {costs[i-1]} -> {costs[i]}"
        )


def test_cumulative_capacity_monotonic_increasing() -> None:
    merit = _build_default_merit()
    cum = merit["cumulative_capacity_mw"].tolist()
    for i in range(1, len(cum)):
        assert cum[i] > cum[i - 1], (
            f"Cumulative capacity not strictly increasing at index {i}"
        )


def test_available_capacity_non_negative() -> None:
    merit = _build_default_merit(outage_mw=2000.0)
    assert (merit["available_capacity_mw"] >= 0).all()


def test_available_capacity_does_not_exceed_nameplate() -> None:
    merit = _build_default_merit(outage_mw=0.0)
    for _, row in merit.iterrows():
        assert row["available_capacity_mw"] <= row["capacity_mw"], (
            f"Block {row['block_id']}: available {row['available_capacity_mw']} > "
            f"nameplate {row['capacity_mw']}"
        )


def test_merit_order_excludes_zero_available_capacity_blocks() -> None:
    # With very large outages, some blocks should be fully derated
    merit = build_merit_order(
        fleet_df=_sample_fleet(),
        gas_price_usd_mmbtu=3.5,
        outage_mw=4000.0,  # Large enough to zero out some blocks
        coal_price_usd_mmbtu=2.5,
        oil_price_usd_mmbtu=15.0,
    )
    assert (merit["available_capacity_mw"] > 0).all()


def test_must_run_units_not_derated() -> None:
    fleet = _sample_fleet()
    derated = apply_outage_derate(fleet, outage_mw=3000.0)
    nuclear = derated[derated["block_id"] == "nuclear"].iloc[0]
    assert float(nuclear["available_capacity_mw"]) == float(nuclear["capacity_mw"])


# ---------------------------------------------------------------------------
# Quantile monotonicity
# ---------------------------------------------------------------------------


def test_monte_carlo_quantiles_monotonic_by_probability() -> None:
    quantiles = [0.10, 0.25, 0.50, 0.75, 0.90]
    result = monte_carlo_dispatch(
        fleet_df=_sample_fleet(),
        net_load_mw=3000.0,
        gas_price_usd_mmbtu=3.5,
        outage_mw=500.0,
        quantiles=quantiles,
        n_draws=200,
        seed=42,
        coal_price_usd_mmbtu=2.5,
        oil_price_usd_mmbtu=15.0,
    )
    q_vals = result["quantiles"]
    sorted_qs = sorted(q_vals.keys())
    for i in range(1, len(sorted_qs)):
        assert q_vals[sorted_qs[i]] >= q_vals[sorted_qs[i - 1]], (
            f"Quantile monotonicity violated: "
            f"Q{sorted_qs[i-1]:.2f}={q_vals[sorted_qs[i-1]]:.4f} > "
            f"Q{sorted_qs[i]:.2f}={q_vals[sorted_qs[i]]:.4f}"
        )
