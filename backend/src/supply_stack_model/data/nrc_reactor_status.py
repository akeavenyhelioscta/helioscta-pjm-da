"""Pull NRC daily reactor power status and validate against PJM nuclear fleet.

The NRC publishes a pipe-delimited text file with the last 365 days of
daily power levels (0-100%) for every US nuclear reactor.

Usage:
    python -m src.supply_stack_model.data.nrc_reactor_status
    python -m src.supply_stack_model.data.nrc_reactor_status --dry-run
"""
from __future__ import annotations

import argparse
import io
import logging
import sys
import urllib.request
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

_BACKEND = str(Path(__file__).resolve().parents[3])
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

logger = logging.getLogger(__name__)

NRC_URL = (
    "https://www.nrc.gov/reading-rm/doc-collections/event-status/"
    "reactor-status/powerreactorstatusforlast365days.txt"
)
NRC_TIMEOUT_SECONDS = 30

OUTPUT_DIR = Path(__file__).resolve().parent
GENERATORS_PARQUET = OUTPUT_DIR / "pjm_fleet_generators.parquet"
NRC_VALIDATION_OUTPUT = OUTPUT_DIR / "nrc_nuclear_validation.parquet"
NRC_DAILY_OUTPUT = OUTPUT_DIR / "nrc_daily_status.parquet"
NRC_PLANT_DAILY_OUTPUT = OUTPUT_DIR / "nrc_plant_daily.parquet"

# ══════════════════════════════════════════════════════════════════════
# NRC unit name → EIA plant_id / generator_id crosswalk
#
# Built by matching NRC "Unit" names against pjm_fleet_generators.parquet.
# Each entry: NRC name → (plant_id_eia, generator_id, unit_capacity_mw)
# Capacities come from EIA summer_capacity_mw in the fleet generators.
# ══════════════════════════════════════════════════════════════════════

NRC_UNIT_CROSSWALK: dict[str, dict] = {
    # Peach Bottom (PA) — plant 3166
    "Peach Bottom 2": {"plant_id_eia": 3166, "generator_id": "2", "capacity_mw": 1265},
    "Peach Bottom 3": {"plant_id_eia": 3166, "generator_id": "3", "capacity_mw": 1285},
    # Susquehanna (PA) — plant 6103
    "Susquehanna 1": {"plant_id_eia": 6103, "generator_id": "1", "capacity_mw": 1247},
    "Susquehanna 2": {"plant_id_eia": 6103, "generator_id": "2", "capacity_mw": 1247},
    # Braidwood (IL) — plant 6022
    "Braidwood 1": {"plant_id_eia": 6022, "generator_id": "1", "capacity_mw": 1183},
    "Braidwood 2": {"plant_id_eia": 6022, "generator_id": "2", "capacity_mw": 1149},
    # Byron (IL) — plant 6023
    "Byron 1": {"plant_id_eia": 6023, "generator_id": "1", "capacity_mw": 1164},
    "Byron 2": {"plant_id_eia": 6023, "generator_id": "2", "capacity_mw": 1136},
    # Salem (NJ) — plant 2410
    "Salem 1": {"plant_id_eia": 2410, "generator_id": "1", "capacity_mw": 1146},
    "Salem 2": {"plant_id_eia": 2410, "generator_id": "2", "capacity_mw": 1139},
    # LaSalle (IL) — plant 6026
    "LaSalle 1": {"plant_id_eia": 6026, "generator_id": "1", "capacity_mw": 1130},
    "LaSalle 2": {"plant_id_eia": 6026, "generator_id": "2", "capacity_mw": 1134},
    # Limerick (PA) — plant 6105
    "Limerick 1": {"plant_id_eia": 6105, "generator_id": "1", "capacity_mw": 1120},
    "Limerick 2": {"plant_id_eia": 6105, "generator_id": "2", "capacity_mw": 1122},
    # D.C. Cook (MI) — plant 6000
    "D.C. Cook 1": {"plant_id_eia": 6000, "generator_id": "1", "capacity_mw": 1009},
    "D.C. Cook 2": {"plant_id_eia": 6000, "generator_id": "2", "capacity_mw": 1168},
    # North Anna (VA) — plant 6168
    "North Anna 1": {"plant_id_eia": 6168, "generator_id": "1", "capacity_mw": 948},
    "North Anna 2": {"plant_id_eia": 6168, "generator_id": "2", "capacity_mw": 944},
    # Quad Cities (IL) — plant 880
    "Quad Cities 1": {"plant_id_eia": 880, "generator_id": "1", "capacity_mw": 908},
    "Quad Cities 2": {"plant_id_eia": 880, "generator_id": "2", "capacity_mw": 911},
    # Beaver Valley (PA) — plant 6040
    "Beaver Valley 1": {"plant_id_eia": 6040, "generator_id": "1", "capacity_mw": 907},
    "Beaver Valley 2": {"plant_id_eia": 6040, "generator_id": "2", "capacity_mw": 901},
    # Dresden (IL) — plant 869
    "Dresden 2": {"plant_id_eia": 869, "generator_id": "2", "capacity_mw": 902},
    "Dresden 3": {"plant_id_eia": 869, "generator_id": "3", "capacity_mw": 895},
    # Calvert Cliffs (MD) — plant 6011
    "Calvert Cliffs 1": {"plant_id_eia": 6011, "generator_id": "1", "capacity_mw": 884},
    "Calvert Cliffs 2": {"plant_id_eia": 6011, "generator_id": "2", "capacity_mw": 861},
    # Surry (VA) — plant 3806
    "Surry 1": {"plant_id_eia": 3806, "generator_id": "1", "capacity_mw": 838},
    "Surry 2": {"plant_id_eia": 3806, "generator_id": "2", "capacity_mw": 838},
    # Perry (OH) — plant 6020
    "Perry 1": {"plant_id_eia": 6020, "generator_id": "1", "capacity_mw": 1240},
    # Hope Creek (NJ) — plant 6118 (separate from Salem despite same site)
    "Hope Creek 1": {"plant_id_eia": 6118, "generator_id": "1", "capacity_mw": 1174},
    # Davis-Besse (OH) — plant 6149
    "Davis-Besse": {"plant_id_eia": 6149, "generator_id": "1", "capacity_mw": 894},
    # Three Mile Island (PA) — plant 8011
    "Three Mile Island 1": {"plant_id_eia": 8011, "generator_id": "1", "capacity_mw": 803},
}

PJM_NRC_UNITS = set(NRC_UNIT_CROSSWALK.keys())


# ══════════════════════════════════════════════════════════════════════
# Pull
# ══════════════════════════════════════════════════════════════════════


def pull_nrc_status() -> pd.DataFrame:
    """Pull last 365 days of NRC reactor power status."""
    logger.info("Downloading NRC reactor status from %s", NRC_URL)
    response = urllib.request.urlopen(NRC_URL, timeout=NRC_TIMEOUT_SECONDS)
    text = response.read().decode("utf-8")

    df = pd.read_csv(io.StringIO(text), sep="|")
    df["ReportDt"] = pd.to_datetime(df["ReportDt"], format="mixed")
    df["Power"] = pd.to_numeric(df["Power"], errors="coerce").fillna(0).astype(int)

    logger.info("NRC data: %d rows, %d units, %s to %s",
                len(df), df["Unit"].nunique(),
                df["ReportDt"].min().date(), df["ReportDt"].max().date())
    return df


def filter_pjm_units(nrc_df: pd.DataFrame) -> pd.DataFrame:
    """Filter to PJM nuclear units and enrich with fleet data."""
    pjm = nrc_df[nrc_df["Unit"].isin(PJM_NRC_UNITS)].copy()

    pjm["plant_id_eia"] = pjm["Unit"].map(lambda u: NRC_UNIT_CROSSWALK[u]["plant_id_eia"])
    pjm["generator_id"] = pjm["Unit"].map(lambda u: NRC_UNIT_CROSSWALK[u]["generator_id"])
    pjm["unit_capacity_mw"] = pjm["Unit"].map(lambda u: NRC_UNIT_CROSSWALK[u]["capacity_mw"])
    pjm["effective_mw"] = (pjm["unit_capacity_mw"] * pjm["Power"] / 100.0).round(1)
    pjm["date"] = pjm["ReportDt"].dt.date

    logger.info("PJM nuclear: %d rows, %d units", len(pjm), pjm["Unit"].nunique())
    return pjm


# ══════════════════════════════════════════════════════════════════════
# Daily fleet aggregation
# ══════════════════════════════════════════════════════════════════════


def compute_daily_fleet(pjm_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate to daily fleet-level nuclear capacity."""
    nameplate = sum(u["capacity_mw"] for u in NRC_UNIT_CROSSWALK.values())

    daily = (
        pjm_df.groupby("date")
        .agg(
            effective_mw=("effective_mw", "sum"),
            units_reporting=("Unit", "nunique"),
            units_at_100=("Power", lambda x: (x == 100).sum()),
            units_at_0=("Power", lambda x: (x == 0).sum()),
            avg_power_pct=("Power", "mean"),
        )
        .reset_index()
    )
    daily["nameplate_mw"] = nameplate
    daily["utilization_pct"] = (daily["effective_mw"] / nameplate * 100).round(1)
    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily.sort_values("date").reset_index(drop=True)

    return daily


# ══════════════════════════════════════════════════════════════════════
# Unit-level summary
# ══════════════════════════════════════════════════════════════════════


def compute_unit_summary(pjm_df: pd.DataFrame) -> pd.DataFrame:
    """Per-unit summary metrics across the reporting period."""
    latest_date = pjm_df["date"].max()

    records: list[dict] = []
    for unit_name, info in sorted(NRC_UNIT_CROSSWALK.items(), key=lambda x: -x[1]["capacity_mw"]):
        unit_df = pjm_df[pjm_df["Unit"] == unit_name]
        if len(unit_df) == 0:
            continue

        latest_row = unit_df[unit_df["date"] == latest_date]
        current_power = int(latest_row["Power"].iloc[0]) if len(latest_row) > 0 else None

        avg_power = float(unit_df["Power"].mean())
        days_at_100 = int((unit_df["Power"] == 100).sum())
        days_at_0 = int((unit_df["Power"] == 0).sum())
        total_days = len(unit_df)

        # 30-day metrics
        last_30 = unit_df.sort_values("date", ascending=False).head(30)
        avg_power_30d = float(last_30["Power"].mean())

        records.append({
            "unit_name": unit_name,
            "plant_id_eia": info["plant_id_eia"],
            "generator_id": info["generator_id"],
            "capacity_mw": info["capacity_mw"],
            "current_power_pct": current_power,
            "current_effective_mw": round(info["capacity_mw"] * (current_power or 0) / 100, 1),
            "avg_power_365d_pct": round(avg_power, 1),
            "avg_power_30d_pct": round(avg_power_30d, 1),
            "days_at_100_pct": days_at_100,
            "days_at_0_pct": days_at_0,
            "total_days": total_days,
            "availability_pct": round((total_days - days_at_0) / max(total_days, 1) * 100, 1),
        })

    df = pd.DataFrame(records).sort_values("capacity_mw", ascending=False).reset_index(drop=True)

    # Status classification
    df["status"] = np.where(
        df["current_power_pct"] == 0, "offline",
        np.where(df["current_power_pct"] < 50, "partial", "operating"),
    )

    return df


# ══════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════


def validate(dry_run: bool = False) -> dict:
    """Pull NRC data, compute metrics, save outputs."""
    nrc_df = pull_nrc_status()
    pjm_df = filter_pjm_units(nrc_df)
    daily = compute_daily_fleet(pjm_df)
    unit_summary = compute_unit_summary(pjm_df)

    latest_date = pjm_df["date"].max()
    nameplate = sum(u["capacity_mw"] for u in NRC_UNIT_CROSSWALK.values())
    today_row = daily[daily["date"] == pd.Timestamp(latest_date)]
    today_eff = float(today_row["effective_mw"].iloc[0]) if len(today_row) > 0 else 0

    # Print summary
    logger.info("=" * 60)
    logger.info("NRC Nuclear Fleet Status — %s", latest_date)
    logger.info("=" * 60)
    logger.info("Nameplate: %s MW", f"{nameplate:,.0f}")
    logger.info("Effective today: %s MW (%.1f%%)", f"{today_eff:,.0f}", today_eff / nameplate * 100)
    logger.info("30-day avg effective: %s MW", f"{daily.tail(30)['effective_mw'].mean():,.0f}")
    logger.info("")

    operating = unit_summary[unit_summary["status"] == "operating"]
    offline = unit_summary[unit_summary["status"] == "offline"]

    logger.info("Unit status (%d PJM units):", len(unit_summary))
    for _, row in unit_summary.iterrows():
        logger.info(
            "  %-25s  %s MW  power=%3d%%  30d_avg=%5.1f%%  days@0=%3d  [%s]",
            row["unit_name"],
            f"{row['capacity_mw']:5,.0f}",
            row["current_power_pct"] or 0,
            row["avg_power_30d_pct"],
            row["days_at_0_pct"],
            row["status"],
        )

    if len(offline) > 0:
        off_mw = float(offline["capacity_mw"].sum())
        logger.warning(
            "%d unit(s) OFFLINE today (%s MW): %s",
            len(offline), f"{off_mw:,.0f}",
            ", ".join(offline["unit_name"].tolist()),
        )

    # Plant-level daily aggregation (for DoD and sparklines)
    plant_daily = (
        pjm_df.groupby(["date", "plant_id_eia"])
        .agg(
            effective_mw=("effective_mw", "sum"),
            capacity_mw=("unit_capacity_mw", "sum"),
            avg_power_pct=("Power", "mean"),
        )
        .reset_index()
    )
    plant_daily["outage_mw"] = plant_daily["capacity_mw"] - plant_daily["effective_mw"]
    plant_daily = plant_daily.sort_values(["plant_id_eia", "date"]).reset_index(drop=True)

    if not dry_run:
        unit_summary.to_parquet(NRC_VALIDATION_OUTPUT, index=False)
        daily.to_parquet(NRC_DAILY_OUTPUT, index=False)
        plant_daily.to_parquet(NRC_PLANT_DAILY_OUTPUT, index=False)
        logger.info("Saved: %s", NRC_PLANT_DAILY_OUTPUT)
        logger.info("Saved: %s", NRC_VALIDATION_OUTPUT)
        logger.info("Saved: %s", NRC_DAILY_OUTPUT)

    return {
        "latest_date": latest_date,
        "nameplate_mw": nameplate,
        "effective_mw_today": today_eff,
        "unit_summary": unit_summary,
        "daily": daily,
        "pjm_hourly": pjm_df,
    }


# ══════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════


def main():
    parser = argparse.ArgumentParser(description="Pull NRC reactor status and validate PJM nuclear fleet")
    parser.add_argument("--dry-run", action="store_true", help="Print results without saving")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    validate(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
