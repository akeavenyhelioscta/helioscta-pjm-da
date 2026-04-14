"""Parse PJM PSS/E RAW file to extract buses, generators, and zones.

Provides bus→zone mapping and generator-level Pmax/Pmin/status data
for crosswalking to the EIA fleet database.

Usage:
    python -m src.supply_stack_model.data.parse_psse_raw
    python -m src.supply_stack_model.data.parse_psse_raw --dry-run
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_BACKEND = str(Path(__file__).resolve().parents[3])
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

logger = logging.getLogger(__name__)

RAW_FILE_PATH = Path(__file__).resolve().parents[4] / ".archive" / "pjm_network_file" / "pjm_network_model.raw"
OUTPUT_DIR = Path(__file__).resolve().parent
GENERATORS_PARQUET = OUTPUT_DIR / "pjm_fleet_generators.parquet"
RAW_GENERATORS_OUTPUT = OUTPUT_DIR / "raw_generators.parquet"
RAW_BUSES_OUTPUT = OUTPUT_DIR / "raw_buses.parquet"
CROSSWALK_OUTPUT = OUTPUT_DIR / "raw_eia_crosswalk.parquet"


# ══════════════════════════════════════════════════════════════════════
# RAW file parsing
# ══════════════════════════════════════════════════════════════════════


def _find_section(lines: list[str], begin_marker: str) -> tuple[int, int]:
    """Find start/end line indices for a section."""
    start = None
    for i, line in enumerate(lines):
        if begin_marker in line:
            start = i + 1
        elif start is not None and line.strip().startswith("0 /"):
            return start, i
    if start is not None:
        return start, len(lines)
    raise ValueError(f"Section not found: {begin_marker}")


def parse_buses(lines: list[str]) -> pd.DataFrame:
    """Parse bus records from the RAW file.

    PSS/E v30 bus format (first 3 lines are header):
    bus_id, 'name', base_kv, type, area_num, zone_num, owner, vm, va, ...
    """
    # Buses are lines 4 to the first "0 /End of Bus data" marker
    bus_start = 3  # After 3 header lines
    bus_end = None
    for i, line in enumerate(lines):
        if "End of Bus data" in line:
            bus_end = i
            break
    if bus_end is None:
        raise ValueError("Could not find end of bus data")

    records = []
    for i in range(bus_start, bus_end):
        line = lines[i].strip()
        if not line or line.startswith("0 /"):
            continue
        parts = line.split(",")
        if len(parts) < 8:
            continue
        try:
            bus_id = int(parts[0].strip())
            name = parts[1].strip().strip("'").strip()
            base_kv = float(parts[2].strip())
            bus_type = int(parts[3].strip())
            # Field 5 is Gl (shunt conductance), field 6 is Bl (shunt susceptance)
            # Field 7 is area, field 8 is zone
            area = int(parts[6].strip()) if len(parts) > 6 else 0
            zone = int(parts[7].strip()) if len(parts) > 7 else 0
            records.append({
                "bus_id": bus_id,
                "bus_name": name,
                "base_kv": base_kv,
                "bus_type": bus_type,
                "area": area,
                "zone_id": zone,
            })
        except (ValueError, IndexError):
            continue

    df = pd.DataFrame(records)
    logger.info("Parsed %d buses", len(df))
    return df


def parse_zones(lines: list[str]) -> pd.DataFrame:
    """Parse zone records."""
    start, end = _find_section(lines, "Begin Zone data")
    records = []
    for i in range(start, end):
        line = lines[i].strip()
        if not line:
            continue
        parts = line.split(",", 1)
        if len(parts) < 2:
            continue
        try:
            zone_id = int(parts[0].strip())
            zone_name = parts[1].strip().strip("'").strip()
            records.append({"zone_id": zone_id, "zone_name": zone_name})
        except ValueError:
            continue
    df = pd.DataFrame(records)
    logger.info("Parsed %d zones", len(df))
    return df


def parse_generators(lines: list[str]) -> pd.DataFrame:
    """Parse generator records.

    PSS/E v30 generator format:
    bus_id, 'gen_id', PG, QG, QT, QB, VS, IREG, MBASE, ...
    fields[14]=status, fields[15]=PT(dummy), fields[16]=PT(Pmax), fields[17]=PB(Pmin)
    fields[18]=owner1, fields[19]=frac1
    """
    start, end = _find_section(lines, "Begin Generator data")
    records = []
    for i in range(start, end):
        line = lines[i].strip()
        if not line:
            continue
        parts = line.split(",")
        if len(parts) < 18:
            continue
        try:
            bus_id = int(parts[0].strip())
            gen_id = parts[1].strip().strip("'").strip()
            pg = float(parts[2].strip())  # Current MW dispatch
            mbase = float(parts[8].strip())  # Machine MVA base
            status = int(parts[14].strip())  # 1=in-service, 0=out
            pt = float(parts[16].strip())  # Pmax
            pb = float(parts[17].strip())  # Pmin
            owner = int(parts[18].strip()) if len(parts) > 18 else 0
            records.append({
                "bus_id": bus_id,
                "gen_id": gen_id,
                "pg_mw": pg,
                "mbase_mva": mbase,
                "status": status,
                "pmax_mw": pt,
                "pmin_mw": pb,
                "owner_id": owner,
            })
        except (ValueError, IndexError):
            continue

    df = pd.DataFrame(records)
    logger.info("Parsed %d generators", len(df))
    return df


def parse_owners(lines: list[str]) -> pd.DataFrame:
    """Parse owner records."""
    start, end = _find_section(lines, "Begin Owner data")
    records = []
    for i in range(start, end):
        line = lines[i].strip()
        if not line:
            continue
        parts = line.split(",", 1)
        if len(parts) < 2:
            continue
        try:
            owner_id = int(parts[0].strip())
            owner_name = parts[1].strip().strip("'").strip()
            records.append({"owner_id": owner_id, "owner_name": owner_name})
        except ValueError:
            continue
    df = pd.DataFrame(records)
    logger.info("Parsed %d owners", len(df))
    return df


def parse_raw_file(raw_path: Path) -> dict[str, pd.DataFrame]:
    """Parse the full RAW file and return buses, zones, generators, owners."""
    logger.info("Reading RAW file: %s", raw_path)
    with open(raw_path, "r") as f:
        lines = f.readlines()
    logger.info("File has %d lines", len(lines))

    buses = parse_buses(lines)
    zones = parse_zones(lines)
    generators = parse_generators(lines)
    owners = parse_owners(lines)

    # Enrich buses with zone names
    buses = buses.merge(zones, on="zone_id", how="left")

    # Enrich generators with bus info
    gen_enriched = generators.merge(
        buses[["bus_id", "bus_name", "base_kv", "area", "zone_id", "zone_name"]],
        on="bus_id", how="left",
    )
    gen_enriched = gen_enriched.merge(owners, on="owner_id", how="left")

    return {
        "buses": buses,
        "zones": zones,
        "generators": gen_enriched,
        "owners": owners,
    }


# ══════════════════════════════════════════════════════════════════════
# EIA crosswalk
# ══════════════════════════════════════════════════════════════════════


def _normalize_name(name: str) -> str:
    """Normalize plant/bus name for fuzzy matching."""
    name = name.lower().strip()
    # Remove common suffixes
    for suffix in [" generating station", " generation station", " power station",
                   " energy center", " power plant", " nuclear", " hydro",
                   " generating", " station", " plant", " gen", " llc",
                   " power", " energy"]:
        name = name.replace(suffix, "")
    # Remove special chars
    name = re.sub(r"[^a-z0-9 ]", "", name)
    return name.strip()


def build_crosswalk(
    raw_generators: pd.DataFrame,
    fleet_generators: pd.DataFrame,
) -> pd.DataFrame:
    """Match RAW file generators to EIA fleet by bus name similarity.

    Strategy: aggregate RAW generators to bus-level (sum Pmax), then match
    against EIA plant-level (sum capacity) by normalized name + capacity proximity.
    """
    # RAW: aggregate to bus level (a plant may have multiple generator records)
    raw_plants = (
        raw_generators.groupby(["bus_name", "zone_name"])
        .agg(
            pmax_total_mw=("pmax_mw", "sum"),
            pmin_total_mw=("pmin_mw", "sum"),
            pg_total_mw=("pg_mw", "sum"),
            n_units=("gen_id", "nunique"),
            any_in_service=("status", "max"),
            bus_ids=("bus_id", lambda x: sorted(x.unique().tolist())),
        )
        .reset_index()
    )
    raw_plants["raw_name_norm"] = raw_plants["bus_name"].apply(_normalize_name)

    # EIA: aggregate to plant level
    eia_plants = (
        fleet_generators.groupby(["plant_id_eia", "plant_name_eia", "state", "pjm_zone", "fleet_fuel_type"])
        .agg(
            eia_capacity_mw=("effective_capacity_mw", "sum"),
            n_gens=("generator_id", "nunique"),
        )
        .reset_index()
    )
    eia_plants["eia_name_norm"] = eia_plants["plant_name_eia"].apply(_normalize_name)

    # Match by normalized name
    matches = []
    used_eia = set()

    for _, raw_row in raw_plants.iterrows():
        raw_norm = raw_row["raw_name_norm"]
        if not raw_norm or len(raw_norm) < 3:
            continue

        best_match = None
        best_score = 0

        for _, eia_row in eia_plants.iterrows():
            eia_id = int(eia_row["plant_id_eia"])
            if eia_id in used_eia:
                continue
            eia_norm = eia_row["eia_name_norm"]

            # Name similarity: check containment both ways
            if raw_norm in eia_norm or eia_norm in raw_norm:
                # Capacity proximity bonus
                raw_mw = raw_row["pmax_total_mw"]
                eia_mw = eia_row["eia_capacity_mw"]
                cap_ratio = min(raw_mw, eia_mw) / max(raw_mw, eia_mw, 1) if max(raw_mw, eia_mw) > 0 else 0
                score = len(min(raw_norm, eia_norm, key=len)) + cap_ratio * 5

                if score > best_score:
                    best_score = score
                    best_match = eia_row

        if best_match is not None and best_score >= 3:
            eia_id = int(best_match["plant_id_eia"])
            used_eia.add(eia_id)
            matches.append({
                "bus_name": raw_row["bus_name"],
                "zone_name": raw_row["zone_name"],
                "pmax_mw": raw_row["pmax_total_mw"],
                "pmin_mw": raw_row["pmin_total_mw"],
                "pg_mw": raw_row["pg_total_mw"],
                "raw_units": raw_row["n_units"],
                "in_service": bool(raw_row["any_in_service"]),
                "plant_id_eia": eia_id,
                "plant_name_eia": best_match["plant_name_eia"],
                "state": best_match["state"],
                "pjm_zone_eia": best_match["pjm_zone"],
                "fleet_fuel_type": best_match["fleet_fuel_type"],
                "eia_capacity_mw": best_match["eia_capacity_mw"],
                "match_score": round(best_score, 1),
            })

    crosswalk = pd.DataFrame(matches).sort_values("eia_capacity_mw", ascending=False).reset_index(drop=True)
    logger.info("Matched %d RAW bus groups to EIA plants", len(crosswalk))
    return crosswalk


# ══════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════


def run(raw_path: Path | None = None, dry_run: bool = False) -> dict:
    """Parse RAW file, build crosswalk, save outputs."""
    raw_path = raw_path or RAW_FILE_PATH
    if not raw_path.exists():
        raise FileNotFoundError(f"RAW file not found: {raw_path}")

    parsed = parse_raw_file(raw_path)
    gen = parsed["generators"]

    # Summary
    in_service = gen[gen["status"] == 1]
    logger.info("=" * 60)
    logger.info("PSS/E RAW File Summary")
    logger.info("=" * 60)
    logger.info("Total generators: %d (%d in service)", len(gen), len(in_service))
    logger.info("Total Pmax (in service): %s MW", f"{in_service['pmax_mw'].sum():,.0f}")
    logger.info("Zones: %d", gen["zone_name"].nunique())
    logger.info("Owners: %d", gen["owner_name"].nunique())

    # Zone summary
    logger.info("")
    logger.info("Top zones by Pmax:")
    zone_cap = in_service.groupby("zone_name")["pmax_mw"].sum().sort_values(ascending=False)
    for zone, mw in zone_cap.head(15).items():
        logger.info("  %-15s %s MW", zone, f"{mw:8,.0f}")

    # Build crosswalk
    fleet_gen = pd.read_parquet(GENERATORS_PARQUET)
    crosswalk = build_crosswalk(gen, fleet_gen)

    logger.info("")
    logger.info("Crosswalk matches (top 20 by capacity):")
    for _, r in crosswalk.head(20).iterrows():
        cap_diff = r["pmax_mw"] - r["eia_capacity_mw"]
        logger.info(
            "  %-12s → %-35s %s  RAW=%s MW  EIA=%s MW  diff=%s",
            r["bus_name"], r["plant_name_eia"], r["fleet_fuel_type"],
            f"{r['pmax_mw']:6,.0f}", f"{r['eia_capacity_mw']:6,.0f}",
            f"{cap_diff:+,.0f}",
        )

    # Unmatched large RAW generators
    matched_buses = set(crosswalk["bus_name"].tolist())
    raw_plants = gen.groupby("bus_name")["pmax_mw"].sum().reset_index()
    unmatched = raw_plants[~raw_plants["bus_name"].isin(matched_buses)]
    unmatched = unmatched[unmatched["pmax_mw"] > 100].sort_values("pmax_mw", ascending=False)
    if len(unmatched) > 0:
        logger.info("")
        logger.info("Unmatched RAW buses > 100 MW (%d):", len(unmatched))
        for _, r in unmatched.head(15).iterrows():
            logger.info("  %-15s %s MW", r["bus_name"], f"{r['pmax_mw']:6,.0f}")

    if not dry_run:
        gen.to_parquet(RAW_GENERATORS_OUTPUT, index=False)
        crosswalk.to_parquet(CROSSWALK_OUTPUT, index=False)
        logger.info("Saved: %s (%d generators)", RAW_GENERATORS_OUTPUT, len(gen))
        logger.info("Saved: %s (%d matches)", CROSSWALK_OUTPUT, len(crosswalk))

    return {"generators": gen, "crosswalk": crosswalk, "buses": parsed["buses"], "zones": parsed["zones"]}


# ══════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════


def main():
    parser = argparse.ArgumentParser(description="Parse PJM PSS/E RAW file and build EIA crosswalk")
    parser.add_argument("--raw-file", type=Path, default=RAW_FILE_PATH)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    run(raw_path=args.raw_file, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
