"""Validate the nuclear fleet block against EIA-923 generation data and gridstatus fuel mix.

Cross-references three sources:
  1. EIA-923 monthly generation from PUDL (plant-level net_generation_mwh)
  2. Gridstatus PJM fuel mix (aggregate hourly nuclear MW)
  3. Fleet generators parquet (nameplate capacity per plant)

Usage:
    python -m src.supply_stack_model.data.validate_nuclear_fleet
    python -m src.supply_stack_model.data.validate_nuclear_fleet --year 2023
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
VALIDATION_OUTPUT = OUTPUT_DIR / "nuclear_validation.parquet"

EIA_COLUMNS = [
    "plant_id_eia",
    "generator_id",
    "report_date",
    "plant_name_eia",
    "fuel_type_code_pudl",
    "net_generation_mwh",
    "capacity_mw",
    "balancing_authority_code_eia",
    "state",
]


# ══════════════════════════════════════════════════════════════════════
# EIA-923 plant-level validation
# ══════════════════════════════════════════════════════════════════════


def pull_nuclear_generation(
    nuclear_plant_ids: list[int],
    year: int,
    channel: str = "stable",
) -> pd.DataFrame:
    """Pull EIA-923 monthly generation for PJM nuclear plants."""
    s3 = pafs.S3FileSystem(region=PUDL_S3_REGION, anonymous=True)
    path = f"{PUDL_S3_BUCKET}/{channel}/{GENERATORS_TABLE}.parquet"

    logger.info("Pulling EIA-923 generation for %d nuclear plants, year=%d...", len(nuclear_plant_ids), year)
    table = pq.read_table(
        path, filesystem=s3, columns=EIA_COLUMNS,
        filters=[
            ("plant_id_eia", "in", nuclear_plant_ids),
            ("fuel_type_code_pudl", "==", "nuclear"),
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
    """Compute per-plant annual validation metrics from EIA-923 data."""
    # Aggregate to plant-month level
    plant_monthly = (
        eia_monthly.groupby(["plant_id_eia", "plant_name_eia", "state", "report_date"])
        .agg(
            net_gen_mwh=("net_generation_mwh", "sum"),
            eia_capacity_mw=("capacity_mw", "sum"),
        )
        .reset_index()
    )

    # Annual metrics per plant
    plant_annual = (
        plant_monthly.groupby(["plant_id_eia", "plant_name_eia", "state"])
        .agg(
            total_gen_gwh=("net_gen_mwh", lambda x: x.sum() / 1000),
            eia_capacity_mw=("eia_capacity_mw", "mean"),
            months_reporting=("report_date", "nunique"),
        )
        .reset_index()
    )

    # Fleet nameplate capacity from our generator parquet
    fleet_nuc = fleet_gen[fleet_gen["fleet_fuel_type"] == "nuclear"].copy()
    fleet_cap = (
        fleet_nuc.groupby("plant_id_eia")["effective_capacity_mw"]
        .sum()
        .reset_index()
        .rename(columns={"effective_capacity_mw": "fleet_capacity_mw"})
    )
    plant_annual = plant_annual.merge(fleet_cap, on="plant_id_eia", how="left")

    # Capacity factor (based on fleet capacity)
    cap_ref = plant_annual["fleet_capacity_mw"].fillna(plant_annual["eia_capacity_mw"])
    hours_in_year = 8760
    plant_annual["capacity_factor_pct"] = (
        plant_annual["total_gen_gwh"] * 1000 / (cap_ref * hours_in_year) * 100
    ).round(1)

    # Peak monthly generation → implied peak MW
    peak_monthly = (
        plant_monthly.groupby("plant_id_eia")["net_gen_mwh"]
        .max()
        .reset_index()
        .rename(columns={"net_gen_mwh": "peak_month_mwh"})
    )
    plant_annual = plant_annual.merge(peak_monthly, on="plant_id_eia", how="left")
    # Approximate peak MW from peak month (assuming ~730 hours/month)
    plant_annual["peak_implied_mw"] = (plant_annual["peak_month_mwh"] / 730).round(0)

    # Status classification
    plant_annual["status"] = np.where(
        plant_annual["capacity_factor_pct"] < 1.0,
        "offline",
        np.where(
            plant_annual["capacity_factor_pct"] < 50.0,
            "partial",
            "operating",
        ),
    )

    plant_annual = plant_annual.sort_values("fleet_capacity_mw", ascending=False).reset_index(drop=True)
    return plant_annual


# ══════════════════════════════════════════════════════════════════════
# Fuel mix comparison
# ══════════════════════════════════════════════════════════════════════


def pull_fuel_mix_nuclear(year: int) -> pd.DataFrame | None:
    """Pull aggregate nuclear generation from gridstatus for a given year."""
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

    return df[["date", "hour_ending", "nuclear"]].copy()


def compute_fleet_vs_fuelmix(
    eia_monthly: pd.DataFrame,
    fuel_mix: pd.DataFrame | None,
    fleet_capacity_mw: float,
) -> dict:
    """Compute fleet-level comparison metrics."""
    metrics: dict = {}

    # EIA-923 fleet aggregate
    total_gen_gwh = float(eia_monthly["net_generation_mwh"].sum()) / 1000
    metrics["eia_total_gen_gwh"] = round(total_gen_gwh, 1)
    metrics["fleet_capacity_mw"] = round(fleet_capacity_mw, 1)
    metrics["eia_capacity_factor_pct"] = round(
        total_gen_gwh * 1000 / (fleet_capacity_mw * 8760) * 100, 1
    )

    # Monthly generation time series
    monthly_gen = (
        eia_monthly.groupby(pd.Grouper(key="report_date", freq="MS"))["net_generation_mwh"]
        .sum()
        .reset_index()
    )
    monthly_gen["avg_mw"] = monthly_gen["net_generation_mwh"] / 730  # approx hours/month
    metrics["eia_peak_monthly_avg_mw"] = round(float(monthly_gen["avg_mw"].max()), 0)
    metrics["eia_min_monthly_avg_mw"] = round(float(monthly_gen["avg_mw"].min()), 0)

    if fuel_mix is not None and len(fuel_mix) > 0:
        fm_mean = float(fuel_mix["nuclear"].mean())
        fm_max = float(fuel_mix["nuclear"].max())
        fm_min = float(fuel_mix["nuclear"].min())
        metrics["fuelmix_mean_mw"] = round(fm_mean, 0)
        metrics["fuelmix_peak_mw"] = round(fm_max, 0)
        metrics["fuelmix_min_mw"] = round(fm_min, 0)
        metrics["fuelmix_capacity_factor_pct"] = round(fm_mean / fleet_capacity_mw * 100, 1)

        # Monthly comparison: EIA vs fuel_mix
        fuel_mix["month"] = fuel_mix["date"].dt.to_period("M")
        fm_monthly_avg = fuel_mix.groupby("month")["nuclear"].mean().reset_index()
        fm_monthly_avg["month_start"] = fm_monthly_avg["month"].dt.to_timestamp()
        merged = monthly_gen.merge(
            fm_monthly_avg, left_on="report_date", right_on="month_start", how="inner"
        )
        if len(merged) > 0:
            corr = float(merged["avg_mw"].corr(merged["nuclear"]))
            bias = float((merged["avg_mw"] - merged["nuclear"]).mean())
            metrics["monthly_correlation"] = round(corr, 3)
            metrics["monthly_bias_mw"] = round(bias, 0)

    return metrics


# ══════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════


def validate(
    year: int = 2024,
    channel: str = "stable",
    dry_run: bool = False,
) -> dict:
    """Run full nuclear fleet validation and return results."""
    # Load fleet generator data
    fleet_gen = pd.read_parquet(GENERATORS_PARQUET)
    nuc_gen = fleet_gen[fleet_gen["fleet_fuel_type"] == "nuclear"]
    nuc_ids = sorted(nuc_gen["plant_id_eia"].unique().tolist())
    fleet_capacity_mw = float(nuc_gen["effective_capacity_mw"].sum())

    logger.info(
        "Nuclear fleet: %d plants, %.1f GW nameplate",
        len(nuc_ids), fleet_capacity_mw / 1000,
    )

    # Pull EIA-923 generation
    eia_monthly = pull_nuclear_generation(nuc_ids, year=year, channel=channel)

    # Compute plant-level metrics
    plant_metrics = compute_plant_metrics(eia_monthly, fleet_gen)

    # Pull fuel mix for comparison
    fuel_mix = pull_fuel_mix_nuclear(year)

    # Fleet-level comparison
    fleet_metrics = compute_fleet_vs_fuelmix(eia_monthly, fuel_mix, fleet_capacity_mw)

    # Print summary
    logger.info("=" * 60)
    logger.info("Nuclear Fleet Validation — %d", year)
    logger.info("=" * 60)
    logger.info("Fleet capacity: %.1f GW", fleet_capacity_mw / 1000)
    logger.info("EIA-923 total generation: %.1f TWh", fleet_metrics["eia_total_gen_gwh"] / 1000)
    logger.info("EIA-923 capacity factor: %.1f%%", fleet_metrics["eia_capacity_factor_pct"])
    if "fuelmix_mean_mw" in fleet_metrics:
        logger.info("Fuel mix avg nuclear: %s MW", f"{fleet_metrics['fuelmix_mean_mw']:,.0f}")
        logger.info("Fuel mix capacity factor: %.1f%%", fleet_metrics["fuelmix_capacity_factor_pct"])
    if "monthly_correlation" in fleet_metrics:
        logger.info("Monthly EIA vs fuel_mix correlation: %.3f", fleet_metrics["monthly_correlation"])
        logger.info("Monthly EIA vs fuel_mix bias: %s MW", f"{fleet_metrics['monthly_bias_mw']:+,.0f}")

    logger.info("")
    logger.info("Per-plant summary:")
    for _, row in plant_metrics.iterrows():
        logger.info(
            "  %-35s %5s  %6.0f MW  CF=%5.1f%%  gen=%7.1f GWh  [%s]",
            row["plant_name_eia"],
            row["state"],
            row.get("fleet_capacity_mw", 0) or 0,
            row["capacity_factor_pct"],
            row["total_gen_gwh"],
            row["status"],
        )

    # Offline plants warning
    offline = plant_metrics[plant_metrics["status"] == "offline"]
    if len(offline) > 0:
        offline_mw = float(offline["fleet_capacity_mw"].sum())
        logger.warning(
            "%d plant(s) appear OFFLINE (%.0f MW): %s",
            len(offline),
            offline_mw,
            ", ".join(offline["plant_name_eia"].tolist()),
        )

    if not dry_run:
        plant_metrics.to_parquet(VALIDATION_OUTPUT, index=False)
        logger.info("Saved: %s", VALIDATION_OUTPUT)

    return {
        "year": year,
        "fleet_capacity_mw": fleet_capacity_mw,
        "plant_metrics": plant_metrics,
        "fleet_metrics": fleet_metrics,
        "fuel_mix_monthly": fuel_mix,
    }


# ══════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════


def main():
    parser = argparse.ArgumentParser(description="Validate PJM nuclear fleet against EIA-923 and fuel mix")
    parser.add_argument("--year", type=int, default=2024, help="Validation year (default: 2024)")
    parser.add_argument("--channel", default="stable", choices=["stable", "nightly"])
    parser.add_argument("--dry-run", action="store_true", help="Print results without saving")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    validate(year=args.year, channel=args.channel, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
