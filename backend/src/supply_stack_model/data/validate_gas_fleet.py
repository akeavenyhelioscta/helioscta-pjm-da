"""Validate gas fleet blocks against EPA CEMS hourly generation data.

CEMS provides hourly gross_load_mw and heat_content_mmbtu per unit for
every fossil-fuel plant with a continuous emissions monitor. This lets us
validate CC vs CT capacity factors, implied heat rates, and cross-reference
against the gridstatus fuel mix aggregate gas column.

Usage:
    python -m src.supply_stack_model.data.validate_gas_fleet
    python -m src.supply_stack_model.data.validate_gas_fleet --year 2023
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
CEMS_TABLE = "core_epacems__hourly_emissions"

OUTPUT_DIR = Path(__file__).resolve().parent
GENERATORS_PARQUET = OUTPUT_DIR / "pjm_fleet_generators.parquet"
GAS_VALIDATION_OUTPUT = OUTPUT_DIR / "gas_validation.parquet"
GAS_MONTHLY_OUTPUT = OUTPUT_DIR / "gas_monthly.parquet"

CEMS_COLUMNS = [
    "plant_id_eia",
    "operating_datetime_utc",
    "gross_load_mw",
    "heat_content_mmbtu",
    "operating_time_hours",
    "state",
    "year",
]


# ══════════════════════════════════════════════════════════════════════
# CEMS pull
# ══════════════════════════════════════════════════════════════════════


def pull_cems_gas(
    gas_plant_ids: list[int],
    year: int,
    channel: str = "stable",
) -> pd.DataFrame:
    """Pull CEMS hourly data for PJM gas plants."""
    s3 = pafs.S3FileSystem(region=PUDL_S3_REGION, anonymous=True)
    path = f"{PUDL_S3_BUCKET}/{channel}/{CEMS_TABLE}.parquet"

    logger.info("Pulling CEMS for %d gas plants, year=%d...", len(gas_plant_ids), year)
    table = pq.read_table(
        path, filesystem=s3, columns=CEMS_COLUMNS,
        filters=[("plant_id_eia", "in", gas_plant_ids), ("year", "==", year)],
    )
    df = table.to_pandas()
    df["operating_datetime_utc"] = pd.to_datetime(df["operating_datetime_utc"])
    logger.info("Got %s CEMS rows, %d plants", f"{len(df):,}", df["plant_id_eia"].nunique())
    return df


# ══════════════════════════════════════════════════════════════════════
# Plant-level metrics
# ══════════════════════════════════════════════════════════════════════


def compute_plant_metrics(
    cems_df: pd.DataFrame,
    fleet_gen: pd.DataFrame,
) -> pd.DataFrame:
    """Per-plant annual metrics from CEMS data."""
    # Plant-level aggregation
    plant = (
        cems_df.groupby("plant_id_eia")
        .agg(
            total_hours=("gross_load_mw", "count"),
            gen_hours=("gross_load_mw", lambda x: (x.notna() & (x > 0)).sum()),
            total_gen_mwh=("gross_load_mw", lambda x: x.clip(lower=0).sum()),
            peak_mw=("gross_load_mw", "max"),
            avg_mw=("gross_load_mw", lambda x: x.clip(lower=0).mean()),
            total_mmbtu=("heat_content_mmbtu", "sum"),
        )
        .reset_index()
    )

    # Fleet info
    gas_gen = fleet_gen[fleet_gen["fleet_fuel_type"].isin(["cc_gas", "ct_gas"])].copy()
    fleet_info = (
        gas_gen.groupby("plant_id_eia")
        .agg(
            plant_name=("plant_name_eia", "first"),
            fleet_fuel_type=("fleet_fuel_type", "first"),
            fleet_capacity_mw=("effective_capacity_mw", "sum"),
            pjm_zone=("pjm_zone", "first"),
            state=("state", "first"),
            fleet_heat_rate=("avg_heat_rate", "mean"),
        )
        .reset_index()
    )
    plant = plant.merge(fleet_info, on="plant_id_eia", how="left")

    # Derived metrics
    plant["capacity_factor_pct"] = np.where(
        plant["fleet_capacity_mw"] > 0,
        (plant["avg_mw"] / plant["fleet_capacity_mw"] * 100).round(1),
        0.0,
    )
    plant["cems_heat_rate"] = np.where(
        plant["total_gen_mwh"] > 0,
        (plant["total_mmbtu"] / plant["total_gen_mwh"]).round(2),
        np.nan,
    )
    plant["total_gen_gwh"] = (plant["total_gen_mwh"] / 1000).round(1)
    plant["availability_pct"] = np.where(
        plant["total_hours"] > 0,
        (plant["gen_hours"] / plant["total_hours"] * 100).round(1),
        0.0,
    )

    return plant.sort_values("fleet_capacity_mw", ascending=False).reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════
# Monthly aggregation (for chart)
# ══════════════════════════════════════════════════════════════════════


def compute_monthly(
    cems_df: pd.DataFrame,
    fleet_gen: pd.DataFrame,
) -> pd.DataFrame:
    """Monthly generation by fleet type (cc_gas, ct_gas) from CEMS."""
    # Map plant → fleet_fuel_type
    gas_gen = fleet_gen[fleet_gen["fleet_fuel_type"].isin(["cc_gas", "ct_gas"])]
    plant_type = gas_gen.groupby("plant_id_eia")["fleet_fuel_type"].first().to_dict()

    df = cems_df.copy()
    df["fleet_fuel_type"] = df["plant_id_eia"].map(plant_type)
    df = df[df["fleet_fuel_type"].notna()].copy()
    df["month"] = df["operating_datetime_utc"].dt.to_period("M")

    monthly = (
        df.groupby(["month", "fleet_fuel_type"])
        .agg(
            avg_mw=("gross_load_mw", lambda x: x.clip(lower=0).mean()),
            total_mmbtu=("heat_content_mmbtu", "sum"),
            total_gen_mwh=("gross_load_mw", lambda x: x.clip(lower=0).sum()),
        )
        .reset_index()
    )
    monthly["month_start"] = monthly["month"].dt.to_timestamp()
    monthly["implied_hr"] = np.where(
        monthly["total_gen_mwh"] > 0,
        (monthly["total_mmbtu"] / monthly["total_gen_mwh"]).round(2),
        np.nan,
    )
    return monthly


# ══════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════


def validate(
    year: int = 2024,
    channel: str = "stable",
    dry_run: bool = False,
) -> dict:
    """Run gas fleet CEMS validation."""
    fleet_gen = pd.read_parquet(GENERATORS_PARQUET)
    gas_gen = fleet_gen[fleet_gen["fleet_fuel_type"].isin(["cc_gas", "ct_gas"])]
    gas_ids = sorted(gas_gen["plant_id_eia"].unique().tolist())

    cc_cap = float(gas_gen[gas_gen["fleet_fuel_type"] == "cc_gas"]["effective_capacity_mw"].sum())
    ct_cap = float(gas_gen[gas_gen["fleet_fuel_type"] == "ct_gas"]["effective_capacity_mw"].sum())
    logger.info("Gas fleet: %d plants, CC=%.1f GW, CT=%.1f GW",
                len(gas_ids), cc_cap / 1000, ct_cap / 1000)

    # Pull CEMS
    cems_df = pull_cems_gas(gas_ids, year=year, channel=channel)

    # Plant metrics
    plant_metrics = compute_plant_metrics(cems_df, fleet_gen)

    # Monthly
    monthly = compute_monthly(cems_df, fleet_gen)

    # Summary
    logger.info("=" * 60)
    logger.info("Gas Fleet CEMS Validation — %d", year)
    logger.info("=" * 60)

    for fuel in ["cc_gas", "ct_gas"]:
        sub = plant_metrics[plant_metrics["fleet_fuel_type"] == fuel]
        if len(sub) == 0:
            continue
        fleet_mw = sub["fleet_capacity_mw"].sum()
        avg_cf = sub["avg_mw"].sum() / fleet_mw * 100 if fleet_mw > 0 else 0
        hr_valid = sub[sub["cems_heat_rate"].notna() & (sub["cems_heat_rate"] > 0) & (sub["cems_heat_rate"] < 30)]
        avg_hr = float(np.average(hr_valid["cems_heat_rate"], weights=hr_valid["total_gen_mwh"])) if len(hr_valid) > 0 else 0

        logger.info("")
        logger.info("%s: %d plants with CEMS, %s MW fleet", fuel, len(sub), f"{fleet_mw:,.0f}")
        logger.info("  Avg CF: %.1f%%", avg_cf)
        logger.info("  Weighted avg heat rate: %.2f MMBtu/MWh", avg_hr)
        logger.info("  Top plants:")
        for _, r in sub.head(10).iterrows():
            logger.info("    %-35s %6s  %s MW  CF=%5.1f%%  HR=%5.2f",
                         r.get("plant_name", "?"), r.get("pjm_zone", ""),
                         f"{r['fleet_capacity_mw']:5,.0f}",
                         r["capacity_factor_pct"],
                         r["cems_heat_rate"] if pd.notna(r["cems_heat_rate"]) else 0)

    if not dry_run:
        plant_metrics.to_parquet(GAS_VALIDATION_OUTPUT, index=False)
        monthly.to_parquet(GAS_MONTHLY_OUTPUT, index=False)
        logger.info("Saved: %s", GAS_VALIDATION_OUTPUT)
        logger.info("Saved: %s", GAS_MONTHLY_OUTPUT)

    return {
        "year": year,
        "plant_metrics": plant_metrics,
        "monthly": monthly,
    }


# ══════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════


def main():
    parser = argparse.ArgumentParser(description="Validate PJM gas fleet with EPA CEMS data")
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--channel", default="stable", choices=["stable", "nightly"])
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    validate(year=args.year, channel=args.channel, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
