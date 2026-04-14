"""Fleet data loaders and variable-cost helpers for supply stack dispatch."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

REQUIRED_FLEET_COLUMNS = {
    "block_id",
    "fuel_type",
    "capacity_mw",
    "heat_rate_mmbtu_mwh",
    "vom_usd_mwh",
    "must_run",
    "gas_hub",
    "outage_weight",
}

GAS_FUELS = {"gas", "cc_gas", "ct_gas"}
COAL_FUELS = {"coal"}
OIL_FUELS = {"oil"}


def load_fleet(path: str | Path | None = None) -> pd.DataFrame:
    """Load a static fleet table from CSV."""
    csv_path = Path(path) if path is not None else Path(__file__).with_name("pjm_fleet.csv")
    df = pd.read_csv(csv_path)

    missing = REQUIRED_FLEET_COLUMNS.difference(df.columns)
    if missing:
        raise ValueError(f"Fleet CSV missing required columns: {sorted(missing)}")

    out = df.copy()
    out["fuel_type"] = out["fuel_type"].astype(str).str.strip().str.lower()
    out["capacity_mw"] = pd.to_numeric(out["capacity_mw"], errors="coerce")
    out["heat_rate_mmbtu_mwh"] = pd.to_numeric(out["heat_rate_mmbtu_mwh"], errors="coerce")
    out["vom_usd_mwh"] = pd.to_numeric(out["vom_usd_mwh"], errors="coerce")
    out["outage_weight"] = pd.to_numeric(out["outage_weight"], errors="coerce").fillna(1.0)
    out["gas_hub"] = out["gas_hub"].astype(str).replace({"nan": ""})
    out["must_run"] = (
        out["must_run"]
        .astype(str)
        .str.strip()
        .str.lower()
        .isin({"1", "true", "t", "yes", "y"})
    )

    if out[["capacity_mw", "heat_rate_mmbtu_mwh", "vom_usd_mwh"]].isna().any().any():
        bad = out[
            out[["capacity_mw", "heat_rate_mmbtu_mwh", "vom_usd_mwh"]].isna().any(axis=1)
        ]
        raise ValueError(
            "Fleet CSV has non-numeric values in required numeric fields: "
            f"{bad['block_id'].tolist()}"
        )
    return out


def compute_variable_costs(
    fleet_df: pd.DataFrame,
    gas_price_usd_mmbtu: float,
    coal_price_usd_mmbtu: float,
    oil_price_usd_mmbtu: float,
) -> pd.DataFrame:
    """Compute per-block variable cost at a single hour."""
    df = fleet_df.copy()
    fuel_prices: list[float] = []
    for _, row in df.iterrows():
        fuel_type = str(row["fuel_type"]).lower()
        if fuel_type in GAS_FUELS:
            fuel_prices.append(float(gas_price_usd_mmbtu))
        elif fuel_type in COAL_FUELS:
            fuel_prices.append(float(coal_price_usd_mmbtu))
        elif fuel_type in OIL_FUELS:
            fuel_prices.append(float(oil_price_usd_mmbtu))
        else:
            fuel_prices.append(0.0)

    df["fuel_price_usd_mmbtu"] = fuel_prices
    df["variable_cost_usd_mwh"] = (
        df["heat_rate_mmbtu_mwh"] * df["fuel_price_usd_mmbtu"] + df["vom_usd_mwh"]
    )
    return df


def apply_outage_derate(
    fleet_df: pd.DataFrame,
    outage_mw: float,
) -> pd.DataFrame:
    """Apply outage derate to non-must-run units (pro-rata by outage_weight)."""
    df = fleet_df.copy()
    df["available_capacity_mw"] = df["capacity_mw"].astype(float)

    total_outage = max(0.0, float(outage_mw))
    if total_outage <= 0:
        return df

    eligible = ~df["must_run"]
    if not eligible.any():
        return df

    weights = df.loc[eligible, "outage_weight"].astype(float).clip(lower=0.0)
    if weights.sum() <= 0:
        weights = df.loc[eligible, "capacity_mw"].astype(float).clip(lower=0.0)
    if weights.sum() <= 0:
        return df

    deductions = total_outage * (weights / weights.sum())
    capacity = df.loc[eligible, "capacity_mw"].astype(float)
    df.loc[eligible, "available_capacity_mw"] = (capacity - deductions).clip(lower=0.0)
    return df
