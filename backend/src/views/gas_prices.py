"""View model: ICE Next-Day Gas Prices — daily cash levels and basis spreads.

Shows recent gas price history for PJM-relevant hubs (M3, HH, Z5S, AGT)
with day-over-day changes and basis spreads to Henry Hub.

Consumed by:
  - API endpoints (JSON)
  - Markdown formatters (MD)
  - Agent (structured context for Morning Fundies)
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Hub display names (column → label)
HUB_LABELS = {
    "gas_m3_price": "M3",
    "gas_hh_price": "HH",
    "gas_z5s_price": "Z5S",
    "gas_agt_price": "AGT",
}

PRICE_COLS = list(HUB_LABELS.keys())


def _sr(val, decimals: int = 3) -> float | None:
    """Safe round — return None for NaN/None."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if np.isnan(f) else round(f, decimals)
    except (TypeError, ValueError):
        return None


def build_view_model(
    df: pd.DataFrame,
    *,
    lookback_days: int = 7,
) -> dict:
    """Build gas prices view model.

    Args:
        df: Output of ``gas_prices.pull()`` with columns:
            date, gas_m3_price, gas_hh_price, gas_z5s_price, gas_agt_price.
        lookback_days: Number of recent trading days to include.

    Returns:
        Structured dict with latest prices, daily history, and basis spreads.
    """
    if df is None or df.empty:
        return {"error": "No gas price data available."}

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.sort_values("date")

    # Trim to lookback window
    if lookback_days and len(df) > lookback_days:
        df = df.tail(lookback_days)

    # Compute day-over-day changes
    for col in PRICE_COLS:
        df[f"{col}_dod"] = df[col].diff()

    # Compute basis spreads to HH
    for col in PRICE_COLS:
        if col != "gas_hh_price":
            hub = HUB_LABELS[col]
            df[f"basis_{hub.lower()}_hh"] = df[col] - df["gas_hh_price"]

    # Build daily rows
    daily_prices = []
    for _, row in df.iterrows():
        entry = {"date": str(row["date"])}
        for col, label in HUB_LABELS.items():
            entry[label] = _sr(row.get(col))
            entry[f"{label}_dod"] = _sr(row.get(f"{col}_dod"))
        # Basis spreads
        for col, label in HUB_LABELS.items():
            if col != "gas_hh_price":
                basis_key = f"basis_{label.lower()}_hh"
                entry[f"{label}-HH"] = _sr(row.get(basis_key))
        daily_prices.append(entry)

    # Latest snapshot
    latest_row = df.iloc[-1]
    latest = {"date": str(latest_row["date"])}
    for col, label in HUB_LABELS.items():
        latest[label] = _sr(latest_row.get(col))
        latest[f"{label}_dod"] = _sr(latest_row.get(f"{col}_dod"))
    for col, label in HUB_LABELS.items():
        if col != "gas_hh_price":
            basis_key = f"basis_{label.lower()}_hh"
            latest[f"{label}-HH"] = _sr(latest_row.get(basis_key))

    return {
        "latest": latest,
        "daily_prices": daily_prices,
        "hubs": list(HUB_LABELS.values()),
        "date_range": {
            "start": str(df["date"].iloc[0]),
            "end": str(df["date"].iloc[-1]),
        },
    }


if __name__ == "__main__":
    import json

    import src.settings  # noqa: F401

    from src.data import gas_prices
    from src.like_day_forecast import configs
    from src.utils.cache_utils import pull_with_cache

    logging.basicConfig(level=logging.INFO)

    df = pull_with_cache(
        source_name="ice_gas_prices",
        pull_fn=gas_prices.pull,
        pull_kwargs={},
        cache_dir=configs.CACHE_DIR,
        cache_enabled=configs.CACHE_ENABLED,
        ttl_hours=configs.CACHE_TTL_HOURS,
        force_refresh=configs.FORCE_CACHE_REFRESH,
    )
    vm = build_view_model(df, lookback_days=7)
    print(json.dumps(vm, indent=2, default=str))
