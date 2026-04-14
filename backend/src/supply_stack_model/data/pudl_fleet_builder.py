"""Build PJM fleet CSV from PUDL EIA-860/923 generator data.

Pulls the ``out_eia__monthly_generators`` table from the public PUDL S3
bucket, filters to PJM existing generators, aggregates by technology
class, and exports the standard ``pjm_fleet.csv``.

Usage:
    python -m src.supply_stack_model.data.pudl_fleet_builder
    python -m src.supply_stack_model.data.pudl_fleet_builder --dry-run
    python -m src.supply_stack_model.data.pudl_fleet_builder --channel nightly
    python -m src.supply_stack_model.data.pudl_fleet_builder --year 2023
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from pyarrow import fs as pafs

_BACKEND = str(Path(__file__).resolve().parents[3])
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════
# Constants
# ══════════════════════════════════════════════════════════════════════

PUDL_S3_BUCKET = "pudl.catalyst.coop"
PUDL_S3_REGION = "us-west-2"
DEFAULT_CHANNEL = "stable"
GENERATORS_TABLE = "out_eia__monthly_generators"
PLANTS_TABLE = "core_eia860__scd_plants"

PJM_BA_CODES = {"PJM"}

OUTPUT_DIR = Path(__file__).resolve().parent
FLEET_CSV_PATH = OUTPUT_DIR / "pjm_fleet.csv"
GENERATORS_PARQUET_PATH = OUTPUT_DIR / "pjm_fleet_generators.parquet"

# Columns to pull from the PUDL parquet
PULL_COLUMNS = [
    "plant_id_eia",
    "generator_id",
    "report_date",
    "plant_name_eia",
    "utility_name_eia",
    "balancing_authority_code_eia",
    "fuel_type_code_pudl",
    "technology_description",
    "prime_mover_code",
    "operational_status",
    "capacity_mw",
    "summer_capacity_mw",
    "unit_heat_rate_mmbtu_per_mwh",
    "fuel_cost_per_mmbtu",
    "state",
    "minimum_load_mw",
]

# Skip zero-marginal-cost renewables (handled by forecast inputs)
SKIP_FUELS = {"solar", "wind"}

# Prime mover codes → CC vs CT classification for gas
CC_PRIME_MOVERS = {"CA", "CS"}  # Combined cycle portions
CT_PRIME_MOVERS = {"CT", "GT", "IC"}  # Combustion turbine / internal combustion
# ST (steam turbine) gas gets classified as ct_gas (older, less efficient)

# State → gas hub mapping for PJM generators
STATE_GAS_HUB = {
    "PA": "gas_m3",
    "NJ": "gas_m3",
    "MD": "gas_m3",
    "DE": "gas_m3",
    "IL": "gas_m3",
    "IN": "gas_m3",
    "MI": "gas_m3",
    "VA": "gas_dom_south",
    "NC": "gas_dom_south",
    "WV": "gas_tco",
    "OH": "gas_tco",
    "KY": "gas_tco",
    "DC": "gas_tz6",
    "TN": "gas_dom_south",
}
DEFAULT_GAS_HUB = "gas_m3"

# EIA-860 transmission/distribution owner name → PJM zone mapping.
# Derived from core_eia860__scd_plants.transmission_distribution_owner_name.
# This is the authoritative EIA record of which transmission company
# connects each plant, mapping directly to PJM pricing zones.
TD_OWNER_TO_ZONE: dict[str, str] = {
    # Exelon / Constellation
    "Commonwealth Edison Co": "COMED",
    "PECO Energy Co": "PECO",
    "Baltimore Gas & Electric Co": "BGE",
    # Dominion
    "Virginia Electric & Power Co": "DOM",
    # FirstEnergy
    "American Transmission Systems Inc": "ATSI",
    "Ohio Edison Co": "ATSI",
    "Cleveland Electric Illum Co": "ATSI",
    "The Toledo Edison Co": "ATSI",
    "FirstEnergy Co": "ATSI",
    "Metropolitan Edison Co": "METED",
    "Pennsylvania Electric Co": "PENELEC",
    "West Penn Power Company": "AP",
    "West Penn Power Co": "AP",
    "Monongahela Power Co": "AP",
    "The Potomac Edison Company": "AP",
    # AEP
    "Ohio Power Co": "AEP",
    "Indiana Michigan Power Co": "AEP",
    "Appalachian Power Co": "AEP",
    "Wheeling Power Co": "AEP",
    "AEP Appalachian Transmission Co Inc": "AEP",
    "AEP Ohio Transmission Co Inc": "AEP",
    # PSEG
    "Public Service Elec & Gas Co": "PSEG",
    # PPL
    "PPL Electric Utilities Corp": "PPL",
    # Duquesne
    "Duquesne Light Co": "DUQ",
    # Duke Ohio/Kentucky
    "Duke Energy Ohio Inc": "DEOK",
    "Duke Energy Kentucky Inc": "DEOK",
    "Duke Energy Indiana LLC": "DEOK",
    # Pepco Holdings
    "Potomac Electric Power Co": "PEPCO",
    "Delmarva Power": "DPL",
    "Delmarva Power & Light Co": "DPL",
    "Atlantic City Electric Co": "AECO",
    # Jersey Central
    "Jersey Central Power & Lt Co": "JCPL",
    # Dayton
    "Dayton Power & Light Co": "DAY",
    "AES Ohio": "DAY",
    # East Kentucky
    "East Kentucky Power Coop, Inc": "EKPC",
    "East Kentucky Power Cooperative Inc": "EKPC",
    # Rockland
    "Rockland Electric Co": "RECO",
}

PLANTS_PULL_COLUMNS = [
    "plant_id_eia",
    "report_date",
    "balancing_authority_code_eia",
    "transmission_distribution_owner_id",
    "transmission_distribution_owner_name",
]


# Technology defaults for heat rates (MMBtu/MWh) when PUDL data is missing
DEFAULT_HEAT_RATES = {
    "nuclear": 0.0,
    "coal": 10.0,
    "cc_gas": 7.0,
    "ct_gas": 10.5,
    "oil": 11.5,
    "hydro": 0.0,
    "storage": 0.0,
    "other": 12.0,
}

# VOM defaults ($/MWh) from NREL ATB
DEFAULT_VOM = {
    "nuclear": 2.50,
    "coal": 4.00,
    "cc_gas": 2.00,
    "ct_gas": 3.50,
    "oil": 5.00,
    "hydro": 1.50,
    "storage": 8.00,
    "other": 20.00,
}

# Outage weights for pro-rata derate allocation
DEFAULT_OUTAGE_WEIGHT = {
    "nuclear": 0.00,
    "coal": 1.00,
    "cc_gas": 1.00,
    "ct_gas": 1.00,
    "oil": 0.70,
    "hydro": 0.35,
    "storage": 0.50,
    "other": 0.75,
}


# ══════════════════════════════════════════════════════════════════════
# Data pull
# ══════════════════════════════════════════════════════════════════════


def pull_pudl_generators(channel: str = DEFAULT_CHANNEL) -> pd.DataFrame:
    """Pull EIA monthly generator data from the PUDL S3 bucket."""
    s3 = pafs.S3FileSystem(region=PUDL_S3_REGION, anonymous=True)
    parquet_path = f"{PUDL_S3_BUCKET}/{channel}/{GENERATORS_TABLE}.parquet"
    logger.info("Reading %s from PUDL S3 (%s channel)...", GENERATORS_TABLE, channel)

    table = pq.read_table(parquet_path, filesystem=s3, columns=PULL_COLUMNS)
    df = table.to_pandas()
    logger.info("Pulled %s rows, %s columns", f"{len(df):,}", len(df.columns))
    return df


def pull_pudl_plants(channel: str = DEFAULT_CHANNEL) -> pd.DataFrame:
    """Pull EIA-860 plant data for transmission owner → zone mapping."""
    s3 = pafs.S3FileSystem(region=PUDL_S3_REGION, anonymous=True)
    parquet_path = f"{PUDL_S3_BUCKET}/{channel}/{PLANTS_TABLE}.parquet"
    logger.info("Reading %s from PUDL S3...", PLANTS_TABLE)

    table = pq.read_table(parquet_path, filesystem=s3, columns=PLANTS_PULL_COLUMNS)
    df = table.to_pandas()
    logger.info("Pulled %s plant rows", f"{len(df):,}")
    return df


def build_plant_zone_map(
    plants_df: pd.DataFrame,
    capacity_year: int | None = None,
) -> pd.DataFrame:
    """Build plant_id_eia → pjm_zone lookup from EIA-860 transmission owner."""
    df = plants_df.copy()
    df["report_date"] = pd.to_datetime(df["report_date"])

    # Filter to PJM plants with a TD owner
    df = df[df["balancing_authority_code_eia"] == "PJM"].copy()
    df = df.dropna(subset=["transmission_distribution_owner_name"])

    # Use latest year that actually has TD owner data (may lag capacity_year)
    if len(df) == 0:
        logger.warning("No PJM plants with transmission owner data found")
        return pd.DataFrame(columns=["plant_id_eia", "pjm_zone", "transmission_distribution_owner_name"])

    latest_td_year = int(df["report_date"].dt.year.max())
    use_year = min(capacity_year, latest_td_year) if capacity_year else latest_td_year
    df = df[df["report_date"].dt.year == use_year]
    logger.info("Using TD owner data from year %d", use_year)
    df = df.sort_values("report_date").drop_duplicates(
        subset=["plant_id_eia"], keep="last"
    )

    # Map TD owner name to zone
    df["pjm_zone"] = df["transmission_distribution_owner_name"].map(TD_OWNER_TO_ZONE).fillna("")

    # Log coverage
    mapped = (df["pjm_zone"] != "").sum()
    unmapped_owners = df[df["pjm_zone"] == ""]["transmission_distribution_owner_name"].unique()
    logger.info(
        "Zone mapping: %d/%d plants mapped (%d unmapped TD owners)",
        mapped, len(df), len(unmapped_owners),
    )
    if len(unmapped_owners) > 0 and len(unmapped_owners) <= 20:
        for o in sorted(unmapped_owners):
            logger.debug("  Unmapped TD owner: %s", o)

    return df[["plant_id_eia", "pjm_zone", "transmission_distribution_owner_name"]].copy()


# ══════════════════════════════════════════════════════════════════════
# Filtering & classification
# ══════════════════════════════════════════════════════════════════════


def _classify_fuel_type(row: pd.Series) -> str:
    """Map PUDL fuel_type_code + prime_mover_code to fleet fuel_type."""
    pudl_fuel = str(row.get("fuel_type_code_pudl", "")).lower().strip()
    pm = str(row.get("prime_mover_code", "")).upper().strip()

    if pudl_fuel == "nuclear":
        return "nuclear"
    if pudl_fuel == "coal":
        return "coal"
    if pudl_fuel == "oil":
        return "oil"
    if pudl_fuel == "hydro":
        return "hydro"
    if pudl_fuel == "gas":
        if pm in CC_PRIME_MOVERS:
            return "cc_gas"
        return "ct_gas"
    if pudl_fuel in ("waste", "other", "geothermal"):
        return "other"
    return "other"


def _assign_gas_hub(row: pd.Series) -> str:
    """Assign gas hub based on state for gas-fired generators."""
    fuel = row.get("fleet_fuel_type", "")
    if fuel not in ("cc_gas", "ct_gas"):
        return ""
    state = str(row.get("state", "")).upper().strip()
    return STATE_GAS_HUB.get(state, DEFAULT_GAS_HUB)


def filter_and_classify(
    df: pd.DataFrame,
    plant_zone_map: pd.DataFrame | None = None,
    capacity_year: int | None = None,
    heat_rate_year: int | None = None,
) -> pd.DataFrame:
    """Filter to PJM existing generators and classify fuel types.

    Uses ``capacity_year`` for capacity/status and ``heat_rate_year``
    for heat rates (which lag capacity data by ~1-2 years in PUDL).
    """
    df = df.copy()
    df["report_date"] = pd.to_datetime(df["report_date"])
    df["year"] = df["report_date"].dt.year

    # Filter to PJM
    pjm = df[df["balancing_authority_code_eia"].isin(PJM_BA_CODES)].copy()
    logger.info("PJM generators: %s rows", f"{len(pjm):,}")

    # Determine capacity year (latest with data)
    if capacity_year is None:
        capacity_year = int(pjm["year"].max())
    logger.info("Using capacity year: %s", capacity_year)

    # Get capacity/status from capacity year
    cap_df = pjm[pjm["year"] == capacity_year].copy()
    cap_df = cap_df[cap_df["operational_status"] == "existing"]
    cap_df = cap_df[~cap_df["fuel_type_code_pudl"].isin(SKIP_FUELS)]
    # Keep latest month per generator
    cap_df = cap_df.sort_values("report_date").drop_duplicates(
        subset=["plant_id_eia", "generator_id"], keep="last"
    )
    logger.info(
        "Existing generators (excl solar/wind) in %s: %s",
        capacity_year, f"{len(cap_df):,}",
    )

    # Get heat rates from heat_rate_year (latest year with non-null data)
    if heat_rate_year is None:
        hr_coverage = (
            pjm.groupby("year")["unit_heat_rate_mmbtu_per_mwh"]
            .apply(lambda s: s.notna().sum())
        )
        valid_years = hr_coverage[hr_coverage > 0]
        heat_rate_year = int(valid_years.index.max()) if len(valid_years) > 0 else capacity_year
    logger.info("Using heat rate year: %s", heat_rate_year)

    hr_df = pjm[pjm["year"] == heat_rate_year].copy()
    # Filter valid heat rates (drop inf, nan, and unreasonable values)
    hr_df = hr_df[hr_df["unit_heat_rate_mmbtu_per_mwh"].notna()]
    hr_df = hr_df[np.isfinite(hr_df["unit_heat_rate_mmbtu_per_mwh"])]
    hr_df = hr_df[hr_df["unit_heat_rate_mmbtu_per_mwh"] > 0]
    hr_df = hr_df[hr_df["unit_heat_rate_mmbtu_per_mwh"] < 30]  # sanity cap
    # Average heat rate per generator across months
    hr_avg = (
        hr_df.groupby(["plant_id_eia", "generator_id"])["unit_heat_rate_mmbtu_per_mwh"]
        .mean()
        .reset_index()
        .rename(columns={"unit_heat_rate_mmbtu_per_mwh": "avg_heat_rate"})
    )
    logger.info("Generators with valid heat rates: %s", f"{len(hr_avg):,}")

    # Merge heat rates onto capacity data
    out = cap_df.merge(hr_avg, on=["plant_id_eia", "generator_id"], how="left")

    # Classify fuel type
    out["fleet_fuel_type"] = out.apply(_classify_fuel_type, axis=1)
    out["gas_hub"] = out.apply(_assign_gas_hub, axis=1)

    # Assign PJM zone from EIA-860 transmission owner
    if plant_zone_map is not None and len(plant_zone_map) > 0:
        zone_lookup = plant_zone_map[["plant_id_eia", "pjm_zone"]].drop_duplicates("plant_id_eia")
        out = out.merge(zone_lookup, on="plant_id_eia", how="left")
        out["pjm_zone"] = out["pjm_zone"].fillna("")
    else:
        out["pjm_zone"] = ""

    # Prefer summer_capacity_mw, fall back to capacity_mw
    out["effective_capacity_mw"] = out["summer_capacity_mw"].fillna(out["capacity_mw"])

    return out


# ══════════════════════════════════════════════════════════════════════
# Aggregation → fleet blocks
# ══════════════════════════════════════════════════════════════════════


def aggregate_fleet(gen_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate generators into technology-class fleet blocks."""
    records: list[dict] = []

    # Group by (fleet_fuel_type, gas_hub)
    for (fuel_type, gas_hub), grp in gen_df.groupby(
        ["fleet_fuel_type", "gas_hub"], observed=True
    ):
        cap = float(grp["effective_capacity_mw"].sum())
        if cap <= 0:
            continue

        # Capacity-weighted average heat rate (exclude NaN)
        hr_valid = grp.dropna(subset=["avg_heat_rate"])
        if len(hr_valid) > 0 and hr_valid["effective_capacity_mw"].sum() > 0:
            weights = hr_valid["effective_capacity_mw"]
            heat_rate = float(np.average(hr_valid["avg_heat_rate"], weights=weights))
        else:
            heat_rate = DEFAULT_HEAT_RATES.get(fuel_type, 12.0)

        # Build block_id
        if gas_hub:
            block_id = f"{fuel_type}_{gas_hub.replace('gas_', '')}"
        else:
            block_id = f"{fuel_type}_base"

        records.append({
            "block_id": block_id,
            "fuel_type": fuel_type,
            "capacity_mw": round(cap, 1),
            "heat_rate_mmbtu_mwh": round(heat_rate, 2),
            "vom_usd_mwh": DEFAULT_VOM.get(fuel_type, 20.0),
            "must_run": fuel_type == "nuclear",
            "gas_hub": gas_hub,
            "outage_weight": DEFAULT_OUTAGE_WEIGHT.get(fuel_type, 1.0),
        })

    fleet = pd.DataFrame(records)
    fleet = fleet.sort_values(
        by=["must_run", "heat_rate_mmbtu_mwh", "fuel_type", "block_id"],
        ascending=[False, True, True, True],
    ).reset_index(drop=True)

    return fleet


# ══════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════


def build_fleet(
    channel: str = DEFAULT_CHANNEL,
    capacity_year: int | None = None,
    heat_rate_year: int | None = None,
    dry_run: bool = False,
) -> pd.DataFrame:
    """End-to-end: pull PUDL → filter → classify → aggregate → export."""
    raw = pull_pudl_generators(channel=channel)

    # Pull EIA-860 plant data for transmission owner → zone mapping
    plants_raw = pull_pudl_plants(channel=channel)
    plant_zone_map = build_plant_zone_map(plants_raw, capacity_year=capacity_year)

    gen_df = filter_and_classify(
        raw,
        plant_zone_map=plant_zone_map,
        capacity_year=capacity_year,
        heat_rate_year=heat_rate_year,
    )
    fleet = aggregate_fleet(gen_df)

    # Summary
    total_cap = fleet["capacity_mw"].sum()
    logger.info("Fleet summary: %d blocks, %.1f GW total capacity", len(fleet), total_cap / 1000)
    for _, row in fleet.iterrows():
        logger.info(
            "  %-25s %8.0f MW  HR=%.2f  VOM=%.1f  hub=%s",
            row["block_id"],
            row["capacity_mw"],
            row["heat_rate_mmbtu_mwh"],
            row["vom_usd_mwh"],
            row["gas_hub"] or "-",
        )

    if dry_run:
        logger.info("Dry run — not writing files")
        return fleet

    # Save fleet CSV
    fleet.to_csv(FLEET_CSV_PATH, index=False)
    logger.info("Wrote fleet CSV: %s (%d blocks)", FLEET_CSV_PATH, len(fleet))

    # Save generator-level audit trail
    audit_cols = [
        "plant_id_eia", "generator_id", "plant_name_eia", "utility_name_eia",
        "state", "pjm_zone", "fuel_type_code_pudl", "prime_mover_code",
        "technology_description", "operational_status", "capacity_mw",
        "summer_capacity_mw", "effective_capacity_mw", "avg_heat_rate",
        "fleet_fuel_type", "gas_hub",
    ]
    available_cols = [c for c in audit_cols if c in gen_df.columns]
    gen_df[available_cols].to_parquet(GENERATORS_PARQUET_PATH, index=False)
    logger.info("Wrote generator audit: %s (%d generators)", GENERATORS_PARQUET_PATH, len(gen_df))

    return fleet


# ══════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════


def main():
    parser = argparse.ArgumentParser(
        description="Build PJM fleet CSV from PUDL EIA generator data",
    )
    parser.add_argument(
        "--channel", default=DEFAULT_CHANNEL, choices=["stable", "nightly"],
        help=f"PUDL S3 channel (default: {DEFAULT_CHANNEL})",
    )
    parser.add_argument(
        "--year", type=int, default=None, dest="capacity_year",
        help="Capacity/status year (default: latest available)",
    )
    parser.add_argument(
        "--heat-rate-year", type=int, default=None,
        help="Heat rate year (default: latest with data)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print aggregated blocks without writing files",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    build_fleet(
        channel=args.channel,
        capacity_year=args.capacity_year,
        heat_rate_year=args.heat_rate_year,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
