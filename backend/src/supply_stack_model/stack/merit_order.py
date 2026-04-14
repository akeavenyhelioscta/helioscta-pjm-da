"""Build merit-order supply curve for one hour."""
from __future__ import annotations

import pandas as pd

from src.supply_stack_model.data.fleet import apply_outage_derate, compute_variable_costs


def build_merit_order(
    fleet_df: pd.DataFrame,
    gas_price_usd_mmbtu: float,
    outage_mw: float,
    coal_price_usd_mmbtu: float = 2.5,
    oil_price_usd_mmbtu: float = 15.0,
) -> pd.DataFrame:
    """Return cost-ordered supply stack with cumulative capacity."""
    costed = compute_variable_costs(
        fleet_df=fleet_df,
        gas_price_usd_mmbtu=gas_price_usd_mmbtu,
        coal_price_usd_mmbtu=coal_price_usd_mmbtu,
        oil_price_usd_mmbtu=oil_price_usd_mmbtu,
    )
    derated = apply_outage_derate(costed, outage_mw=outage_mw)

    merit = derated[derated["available_capacity_mw"] > 0].copy()
    merit = merit.sort_values(
        by=["variable_cost_usd_mwh", "heat_rate_mmbtu_mwh", "fuel_type", "block_id"],
        ascending=[True, True, True, True],
    ).reset_index(drop=True)
    merit["cumulative_capacity_mw"] = merit["available_capacity_mw"].cumsum()

    cols = [
        "block_id",
        "fuel_type",
        "must_run",
        "capacity_mw",
        "available_capacity_mw",
        "heat_rate_mmbtu_mwh",
        "vom_usd_mwh",
        "fuel_price_usd_mmbtu",
        "variable_cost_usd_mwh",
        "cumulative_capacity_mw",
    ]
    return merit[cols]
