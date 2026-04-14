"""Dispatch logic: pick marginal block and clearing price."""
from __future__ import annotations

import math

import pandas as pd


def dispatch(
    merit_order_df: pd.DataFrame,
    net_load_mw: float,
    congestion_adder_usd: float = 0.0,
    scarcity_price_cap_usd_mwh: float = 500.0,
) -> dict:
    """Dispatch against a merit order and return clearing details."""
    if merit_order_df is None or len(merit_order_df) == 0:
        raise ValueError("Merit order is empty; cannot dispatch")

    demand = max(0.0, float(net_load_mw))
    total_available = float(merit_order_df["available_capacity_mw"].sum())

    if demand <= 0.0:
        marginal = merit_order_df.iloc[0]
        base_price = float(marginal["variable_cost_usd_mwh"])
        clearing_price = max(0.0, base_price + float(congestion_adder_usd))
        status = "oversupplied"
        shortage_mw = 0.0
    else:
        idx = merit_order_df.index[merit_order_df["cumulative_capacity_mw"] >= demand]
        if len(idx) > 0:
            marginal = merit_order_df.loc[idx[0]]
            base_price = float(marginal["variable_cost_usd_mwh"])
            clearing_price = max(0.0, base_price + float(congestion_adder_usd))
            status = "balanced"
            shortage_mw = 0.0
        else:
            marginal = merit_order_df.iloc[-1]
            base_price = float(marginal["variable_cost_usd_mwh"])
            shortage_mw = demand - total_available
            scarcity_component = min(
                max(0.0, (shortage_mw / 1000.0) * 50.0),
                max(0.0, float(scarcity_price_cap_usd_mwh) - base_price),
            )
            clearing_price = max(
                0.0,
                base_price + scarcity_component + float(congestion_adder_usd),
            )
            status = "short_capacity"

    reserve_margin_mw = total_available - demand
    stack_position_pct = demand / total_available if total_available > 0 else math.nan
    return {
        "price": round(clearing_price, 4),
        "base_price": round(base_price, 4),
        "marginal_block_id": str(marginal["block_id"]),
        "marginal_fuel": str(marginal["fuel_type"]),
        "marginal_heat_rate": float(marginal["heat_rate_mmbtu_mwh"]),
        "marginal_variable_cost": float(marginal["variable_cost_usd_mwh"]),
        "reserve_margin_mw": round(reserve_margin_mw, 3),
        "stack_position_pct": round(stack_position_pct, 6) if not math.isnan(stack_position_pct) else None,
        "shortage_mw": round(shortage_mw, 3),
        "total_available_capacity_mw": round(total_available, 3),
        "dispatch_status": status,
    }
