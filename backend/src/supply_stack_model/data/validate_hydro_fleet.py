"""Validate the hydro fleet block against EIA-923 generation and gridstatus fuel mix.

Splits pumped storage from conventional hydro since they behave differently:
- Conventional: run-of-river or reservoir dispatch, positive net generation
- Pumped storage: net energy consumer, used for arbitrage, negative net gen

Usage:
    python -m src.supply_stack_model.data.validate_hydro_fleet
    python -m src.supply_stack_model.data.validate_hydro_fleet --year 2023
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

PUDL_S3_BUCKET = "pudl.catalyst.coop"
PUDL_S3_REGION = "us-west-2"
GENERATORS_TABLE = "out_eia__monthly_generators"

OUTPUT_DIR = Path(__file__).resolve().parent
GENERATORS_PARQUET = OUTPUT_DIR / "pjm_fleet_generators.parquet"
HYDRO_VALIDATION_OUTPUT = OUTPUT_DIR / "hydro_validation.parquet"

PUMPED_STORAGE_TECH = "Hydroelectric Pumped Storage"

EIA_COLUMNS = [
    "plant_id_eia", "generator_id", "report_date", "plant_name_eia",
    "fuel_type_code_pudl", "net_generation_mwh", "capacity_mw",
    "balancing_authority_code_eia", "state",
]


# ══════════════════════════════════════════════════════════════════════
# EIA-923 pull and metrics
# ══════════════════════════════════════════════════════════════════════


def pull_hydro_generation(
    hydro_plant_ids: list[int],
    year: int,
    channel: str = "stable",
) -> pd.DataFrame:
    """Pull EIA-923 monthly generation for PJM hydro plants."""
    s3 = pafs.S3FileSystem(region=PUDL_S3_REGION, anonymous=True)
    path = f"{PUDL_S3_BUCKET}/{channel}/{GENERATORS_TABLE}.parquet"

    logger.info("Pulling EIA-923 generation for %d hydro plants, year=%d...",
                len(hydro_plant_ids), year)
    table = pq.read_table(
        path, filesystem=s3, columns=EIA_COLUMNS,
        filters=[
            ("plant_id_eia", "in", hydro_plant_ids),
            ("fuel_type_code_pudl", "==", "hydro"),
        ],
    )
    df = table.to_pandas()
    df["report_date"] = pd.to_datetime(df["report_date"])
    df = df[df["report_date"].dt.year == year].copy()
    logger.info("Got %d monthly rows for year %d", len(df), year)
    return df


def compute_plant_metrics(
    eia_monthly: pd.DataFrame,
    fleet_gen: pd.DataFrame,
) -> pd.DataFrame:
    """Compute per-plant annual metrics from EIA-923 data."""
    # Plant-month aggregation
    plant_monthly = (
        eia_monthly.groupby(["plant_id_eia", "plant_name_eia", "state", "report_date"])
        .agg(net_gen_mwh=("net_generation_mwh", "sum"))
        .reset_index()
    )

    # Annual aggregation
    plant_annual = (
        plant_monthly.groupby(["plant_id_eia", "plant_name_eia", "state"])
        .agg(
            total_gen_gwh=("net_gen_mwh", lambda x: x.sum() / 1000),
            peak_month_mwh=("net_gen_mwh", "max"),
            min_month_mwh=("net_gen_mwh", "min"),
            months_reporting=("report_date", "nunique"),
        )
        .reset_index()
    )

    # Fleet capacity and metadata
    fleet_hydro = fleet_gen[fleet_gen["fleet_fuel_type"] == "hydro"].copy()
    fleet_cap = (
        fleet_hydro.groupby("plant_id_eia")
        .agg(
            fleet_capacity_mw=("effective_capacity_mw", "sum"),
            technology=("technology_description", "first"),
            pjm_zone=("pjm_zone", "first"),
        )
        .reset_index()
    )
    plant_annual = plant_annual.merge(fleet_cap, on="plant_id_eia", how="left")

    # Capacity factor
    cap_ref = plant_annual["fleet_capacity_mw"]
    plant_annual["capacity_factor_pct"] = np.where(
        cap_ref > 0,
        (plant_annual["total_gen_gwh"] * 1000 / (cap_ref * 8760) * 100).round(1),
        0.0,
    )

    # Classify as pumped storage vs conventional
    plant_annual["is_pumped_storage"] = (
        plant_annual["technology"].str.contains("Pumped", case=False, na=False)
    )

    # Status
    plant_annual["status"] = np.where(
        plant_annual["total_gen_gwh"].abs() < 0.1, "inactive",
        np.where(plant_annual["is_pumped_storage"], "pumped_storage", "conventional"),
    )

    return plant_annual.sort_values("fleet_capacity_mw", ascending=False).reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════
# Fuel mix comparison
# ══════════════════════════════════════════════════════════════════════


def pull_fuel_mix_hydro(year: int) -> pd.DataFrame | None:
    """Pull aggregate hydro generation from gridstatus."""
    try:
        from src.data import fuel_mix_hourly
        df = fuel_mix_hourly.pull()
    except Exception as exc:
        logger.warning("Could not pull fuel mix: %s", exc)
        return None

    df["date"] = pd.to_datetime(df["date"])
    df = df[df["date"].dt.year == year].copy()
    if len(df) == 0:
        return None

    # fuel_mix has 'hydro' and 'storage' columns
    cols = ["date", "hour_ending"]
    if "hydro" in df.columns:
        cols.append("hydro")
    if "storage" in df.columns:
        cols.append("storage")
    return df[cols].copy()


# ══════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════


def validate(
    year: int = 2024,
    channel: str = "stable",
    dry_run: bool = False,
) -> dict:
    """Run hydro fleet validation."""
    fleet_gen = pd.read_parquet(GENERATORS_PARQUET)
    hydro_gen = fleet_gen[fleet_gen["fleet_fuel_type"] == "hydro"]
    hydro_ids = sorted(hydro_gen["plant_id_eia"].unique().tolist())
    fleet_cap = float(hydro_gen["effective_capacity_mw"].sum())

    logger.info("Hydro fleet: %d plants, %.1f GW nameplate", len(hydro_ids), fleet_cap / 1000)

    # Pull EIA-923
    eia_monthly = pull_hydro_generation(hydro_ids, year=year, channel=channel)
    plant_metrics = compute_plant_metrics(eia_monthly, fleet_gen)

    # Split metrics
    conventional = plant_metrics[~plant_metrics["is_pumped_storage"]]
    pumped = plant_metrics[plant_metrics["is_pumped_storage"]]

    # Summary
    logger.info("=" * 60)
    logger.info("Hydro Fleet Validation — %d", year)
    logger.info("=" * 60)
    logger.info("Total capacity: %s MW", f"{fleet_cap:,.0f}")
    logger.info("Conventional: %d plants, %s MW",
                len(conventional), f"{conventional['fleet_capacity_mw'].sum():,.0f}")
    logger.info("Pumped storage: %d plants, %s MW",
                len(pumped), f"{pumped['fleet_capacity_mw'].sum():,.0f}")

    logger.info("")
    logger.info("Conventional hydro:")
    for _, r in conventional.iterrows():
        logger.info("  %-35s %5s  %s MW  gen=%7.1f GWh  CF=%5.1f%%",
                     r["plant_name_eia"], r["state"],
                     f"{r['fleet_capacity_mw']:5,.0f}",
                     r["total_gen_gwh"], r["capacity_factor_pct"])

    logger.info("")
    logger.info("Pumped storage:")
    for _, r in pumped.iterrows():
        logger.info("  %-35s %5s  %s MW  net=%7.1f GWh",
                     r["plant_name_eia"], r["state"],
                     f"{r['fleet_capacity_mw']:5,.0f}",
                     r["total_gen_gwh"])

    if not dry_run:
        plant_metrics.to_parquet(HYDRO_VALIDATION_OUTPUT, index=False)
        logger.info("Saved: %s", HYDRO_VALIDATION_OUTPUT)

    return {
        "year": year,
        "fleet_capacity_mw": fleet_cap,
        "plant_metrics": plant_metrics,
    }


# ══════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════


def main():
    parser = argparse.ArgumentParser(description="Validate PJM hydro fleet against EIA-923")
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--channel", default="stable", choices=["stable", "nightly"])
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    validate(year=args.year, channel=args.channel, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
