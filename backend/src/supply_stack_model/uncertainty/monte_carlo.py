"""Monte Carlo wrapper for supply stack dispatch quantiles."""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.supply_stack_model.stack.dispatch import dispatch
from src.supply_stack_model.stack.merit_order import build_merit_order


def monte_carlo_dispatch(
    fleet_df: pd.DataFrame,
    net_load_mw: float,
    gas_price_usd_mmbtu: float,
    outage_mw: float,
    quantiles: list[float],
    n_draws: int = 300,
    seed: int | None = None,
    net_load_error_std_pct: float = 0.025,
    gas_price_error_std_pct: float = 0.05,
    outage_error_std_pct: float = 0.08,
    coal_price_usd_mmbtu: float = 2.5,
    oil_price_usd_mmbtu: float = 15.0,
    congestion_adder_usd: float = 0.0,
    scarcity_price_cap_usd_mwh: float = 500.0,
) -> dict:
    """Produce price quantiles from perturbed load/gas/outage draws."""
    draws = max(1, int(n_draws))
    rng = np.random.default_rng(seed)
    prices = np.zeros(draws, dtype=float)

    base_net = max(0.0, float(net_load_mw))
    base_gas = max(0.05, float(gas_price_usd_mmbtu))
    base_outage = max(0.0, float(outage_mw))

    for i in range(draws):
        draw_net = max(0.0, base_net * (1.0 + rng.normal(0.0, net_load_error_std_pct)))
        draw_gas = max(0.05, base_gas * (1.0 + rng.normal(0.0, gas_price_error_std_pct)))
        draw_outage = max(
            0.0,
            base_outage * (1.0 + rng.normal(0.0, outage_error_std_pct)),
        )

        merit = build_merit_order(
            fleet_df=fleet_df,
            gas_price_usd_mmbtu=draw_gas,
            outage_mw=draw_outage,
            coal_price_usd_mmbtu=coal_price_usd_mmbtu,
            oil_price_usd_mmbtu=oil_price_usd_mmbtu,
        )
        result = dispatch(
            merit_order_df=merit,
            net_load_mw=draw_net,
            congestion_adder_usd=congestion_adder_usd,
            scarcity_price_cap_usd_mwh=scarcity_price_cap_usd_mwh,
        )
        prices[i] = result["price"]

    q_map = {
        float(q): float(np.quantile(prices, q))
        for q in sorted(set(float(q) for q in quantiles))
    }
    return {
        "prices": prices,
        "quantiles": q_map,
    }
