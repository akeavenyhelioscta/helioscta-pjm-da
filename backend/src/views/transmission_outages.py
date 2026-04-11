"""View model: Active transmission outages — congestion risk context.

Produces two sections:
  1. Regional summary — one row per region with outage counts by voltage tier,
     risk-flagged count, and chronic outage indicator.
  2. Notable outages — individual rows for outages that are high-risk, high-voltage,
     recently started, or returning soon. Each tagged with why it's notable.

Consumed by:
  - API endpoints (JSON / markdown)
  - Agent (cross-reference with LMP congestion spikes)
"""
from __future__ import annotations

import re
from datetime import date

import numpy as np
import pandas as pd

# Zone → region mapping (matches PJM pricing/congestion regions)
_ZONE_MAP = {
    "AEP": "AEP / West",
    "COMED": "ComEd / West", "AMIL": "ComEd / West", "CWLP": "ComEd / West",
    "CONS": "MISO-seam / West", "WEC": "MISO-seam / West",
    "NIPS": "MISO-seam / West", "CIN": "MISO-seam / West",
    "DOM": "Dominion",
    "DAYTON": "Ohio Valley", "DEOK": "Ohio Valley",
    "EKPC": "Ohio Valley", "EKPCEL": "Ohio Valley",
    "FE": "FirstEnergy / Ohio", "CPP": "FirstEnergy / Ohio",
    "NXTERA": "FirstEnergy / Ohio",
    "DUQU": "West Penn / MidAtl", "PN": "West Penn / MidAtl",
    "PL-N": "West Penn / MidAtl", "PL-S": "West Penn / MidAtl",
    "PE": "West Penn / MidAtl",
    "PS-N": "East / NJ-DE-MD", "PS-S": "East / NJ-DE-MD",
    "JC-N": "East / NJ-DE-MD", "JC-S": "East / NJ-DE-MD",
    "RECO": "East / NJ-DE-MD", "AE": "East / NJ-DE-MD",
    "DPL": "East / NJ-DE-MD", "PEP": "East / NJ-DE-MD",
    "ME": "MidAtl / BGE-PEPCO", "SMECO": "MidAtl / BGE-PEPCO",
    "APSS": "MidAtl / BGE-PEPCO", "BC": "MidAtl / BGE-PEPCO",
    "NAEA": "MidAtl / BGE-PEPCO", "UGI": "MidAtl / BGE-PEPCO",
    "AMP": "MidAtl / BGE-PEPCO",
    "LGEE": "LGEE / South", "OVEC": "LGEE / South",
    "HTP": "NY ties", "NEPTUN": "NY ties", "LINVFT": "NY ties",
    "SENY": "NY ties", "UPNY": "NY ties",
}

# Prefix-based zone mapping (AEP-*, DOM-*, FE*)
_ZONE_PREFIX_MAP = {
    "AEP": "AEP / West",
    "DOM": "Dominion",
    "FE": "FirstEnergy / Ohio",
}

REGION_ORDER = [
    "AEP / West", "ComEd / West", "MISO-seam / West",
    "FirstEnergy / Ohio", "Ohio Valley",
    "Dominion", "West Penn / MidAtl",
    "MidAtl / BGE-PEPCO", "East / NJ-DE-MD",
    "LGEE / South", "NY ties",
]


def _map_zone_to_region(zone: str) -> str:
    """Map a PJM zone code to a congestion region."""
    if zone in _ZONE_MAP:
        return _ZONE_MAP[zone]
    for prefix, region in _ZONE_PREFIX_MAP.items():
        if zone.startswith(prefix):
            return region
    return zone  # unmapped zones keep their raw name


# Equipment type → congestion-impact category
_EQUIP_CATEGORY = {
    "LINE": "path",      # removes a flow corridor between two substations
    "XFMR": "capacity",  # removes local transformation capacity at a substation
    "PS": "capacity",     # removes phase-shifting capability at a substation
}


def _parse_facility(facility: str, equip_type: str) -> dict:
    """Parse facility name to extract route (lines) or station (equipment).

    Lines have two endpoints (from/to) identifying the flow path affected.
    Transformers and phase shifters reference a single substation.

    Returns dict with keys: from_station, to_station, station.
    """
    result = {"from_station": None, "to_station": None, "station": None}
    if not facility:
        return result

    # Extract the description after "{kV} KV"
    match = re.search(r"\d+\s+KV\s+(.+)", facility)
    if not match:
        return result
    desc = match.group(1).strip()

    if equip_type == "LINE":
        # Lines: description contains "FROM-TO" route (hyphen-separated)
        parts = re.split(r"\s*-\s*", desc, maxsplit=1)
        if len(parts) == 2:
            result["from_station"] = re.sub(r"\s+", " ", parts[0]).strip()
            result["to_station"] = re.sub(r"\s+", " ", parts[1]).strip()
    else:
        # XFMR, PS: first token in description is the substation name
        result["station"] = desc.split()[0] if desc else None

    return result


def build_view_model(df: pd.DataFrame, reference_date: date | None = None) -> dict:
    """Build the transmission outages view model.

    Parameters
    ----------
    df : pd.DataFrame
        Output of ``transmission_outages.pull()`` — active/approved ≥230 kV outages.
    reference_date : date, optional
        Defaults to today.
    """
    if df is None or df.empty:
        return {"error": "No transmission outage data available."}

    if reference_date is None:
        reference_date = date.today()

    df = df.copy()
    df["region"] = df["zone"].fillna("").apply(_map_zone_to_region)
    df["voltage_kv"] = pd.to_numeric(df["voltage_kv"], errors="coerce").fillna(0).astype(int)
    df["days_out"] = pd.to_numeric(df["days_out"], errors="coerce")
    df["days_to_return"] = pd.to_numeric(df["days_to_return"], errors="coerce")
    df["start_datetime"] = pd.to_datetime(df["start_datetime"], errors="coerce")
    df["end_datetime"] = pd.to_datetime(df["end_datetime"], errors="coerce")
    df["last_revised"] = pd.to_datetime(df["last_revised"], errors="coerce")
    df["risk_flag"] = df["risk"].fillna("").str.strip().str.lower() == "yes"
    df["equip_category"] = df["equipment_type"].map(_EQUIP_CATEGORY).fillna("other")

    # Split active vs cancelled
    df_active = df[df["outage_state"].isin(["Active", "Approved"])].copy()
    df_cancelled = df[df["outage_state"] == "Cancelle"].copy()

    # ── Section 1: Regional Summary (active only) ────────────────
    regional = _build_regional_summary(df_active)

    # ── Section 2: Notable Outages (active only) ─────────────────
    notable = _build_notable_outages(df_active, reference_date)

    # ── Section 3: Recently Cancelled ────────────────────────────
    recently_cancelled = _build_recently_cancelled(df_cancelled)

    return {
        "reference_date": str(reference_date),
        "total_active": len(df_active),
        "regional_summary": regional,
        "notable_outages": notable,
        "recently_cancelled": recently_cancelled,
    }


def _build_regional_summary(df: pd.DataFrame) -> list[dict]:
    """One row per region: outage counts by voltage tier, risk count, chronic indicator."""
    rows = []
    for region in REGION_ORDER:
        rdf = df[df["region"] == region]
        if rdf.empty:
            continue

        rows.append({
            "region": region,
            "total": len(rdf),
            "path_count": int((rdf["equip_category"] == "path").sum()),
            "capacity_count": int((rdf["equip_category"] == "capacity").sum()),
            "count_765kv": int((rdf["voltage_kv"] == 765).sum()),
            "count_500kv": int((rdf["voltage_kv"] == 500).sum()),
            "count_345kv": int((rdf["voltage_kv"] == 345).sum()),
            "count_230kv": int((rdf["voltage_kv"] == 230).sum()),
            "risk_flagged": int(rdf["risk_flag"].sum()),
            "longest_out_days": _si(rdf["days_out"].max()),
            "soonest_return_days": _si(rdf.loc[rdf["days_to_return"].notna(), "days_to_return"].min()),
        })

    # Add any unmapped regions at the end
    mapped = {r["region"] for r in rows}
    for region in sorted(df["region"].unique()):
        if region not in mapped:
            rdf = df[df["region"] == region]
            rows.append({
                "region": region,
                "total": len(rdf),
                "path_count": int((rdf["equip_category"] == "path").sum()),
                "capacity_count": int((rdf["equip_category"] == "capacity").sum()),
                "count_765kv": int((rdf["voltage_kv"] == 765).sum()),
                "count_500kv": int((rdf["voltage_kv"] == 500).sum()),
                "count_345kv": int((rdf["voltage_kv"] == 345).sum()),
                "count_230kv": int((rdf["voltage_kv"] == 230).sum()),
                "risk_flagged": int(rdf["risk_flag"].sum()),
                "longest_out_days": _si(rdf["days_out"].max()),
                "soonest_return_days": _si(rdf.loc[rdf["days_to_return"].notna(), "days_to_return"].min()),
            })

    return rows


def _build_notable_outages(df: pd.DataFrame, reference_date: date) -> list[dict]:
    """Individual outages tagged with why they're notable."""
    notable_rows = []
    seen_tickets = set()

    for _, row in df.iterrows():
        tags = []
        if row["risk_flag"]:
            tags.append("high-risk")
        if row["voltage_kv"] >= 500:
            tags.append("500kv+")
        if (row["start_datetime"] is not pd.NaT
                and (reference_date - row["start_datetime"].date()).days <= 3):
            tags.append("new")
        if (row["days_to_return"] is not None
                and not np.isnan(row["days_to_return"])
                and row["days_to_return"] <= 7):
            tags.append("returning")

        if not tags:
            continue

        # Deduplicate by ticket_id
        tid = row.get("ticket_id")
        if tid and tid in seen_tickets:
            continue
        if tid:
            seen_tickets.add(tid)

        cause_raw = row.get("cause", "") or ""
        cause_primary = cause_raw.split(";")[0].strip() if cause_raw else ""

        equip_type = row.get("equipment_type", "")
        parsed = _parse_facility(row.get("facility_name", ""), equip_type)

        notable_rows.append({
            "tags": tags,
            "region": row["region"],
            "zone": row["zone"],
            "facility": row.get("facility_name", ""),
            "equip": equip_type,
            "equip_category": _EQUIP_CATEGORY.get(equip_type, "other"),
            "kv": row["voltage_kv"],
            "from_station": parsed["from_station"],
            "to_station": parsed["to_station"],
            "station": parsed["station"],
            "started": str(row["start_datetime"].date()) if row["start_datetime"] is not pd.NaT else None,
            "est_return": str(row["end_datetime"].date()) if row["end_datetime"] is not pd.NaT else None,
            "days_out": _si(row["days_out"]),
            "days_to_return": _si(row["days_to_return"]),
            "cause": cause_primary,
        })

    # Sort: high-risk first, then by voltage desc, then by days_to_return asc
    def sort_key(r):
        risk_priority = 0 if "high-risk" in r["tags"] else 1
        return (risk_priority, -r["kv"], r.get("days_to_return") or 9999)

    notable_rows.sort(key=sort_key)
    return notable_rows


def _build_recently_cancelled(df: pd.DataFrame) -> list[dict]:
    """Outages cancelled in the last 7 days — congestion relief signal."""
    if df.empty:
        return []

    rows = []
    seen_tickets = set()

    for _, row in df.sort_values("voltage_kv", ascending=False).iterrows():
        tid = row.get("ticket_id")
        if tid and tid in seen_tickets:
            continue
        if tid:
            seen_tickets.add(tid)

        cause_raw = row.get("cause", "") or ""
        cause_primary = cause_raw.split(";")[0].strip() if cause_raw else ""

        equip_type = row.get("equipment_type", "")
        parsed = _parse_facility(row.get("facility_name", ""), equip_type)

        rows.append({
            "region": row["region"],
            "zone": row["zone"],
            "facility": row.get("facility_name", ""),
            "equip": equip_type,
            "equip_category": _EQUIP_CATEGORY.get(equip_type, "other"),
            "kv": row["voltage_kv"],
            "from_station": parsed["from_station"],
            "to_station": parsed["to_station"],
            "station": parsed["station"],
            "was_scheduled_start": str(row["start_datetime"].date()) if row["start_datetime"] is not pd.NaT else None,
            "was_scheduled_end": str(row["end_datetime"].date()) if row["end_datetime"] is not pd.NaT else None,
            "cancelled_date": str(row["last_revised"].date()) if row["last_revised"] is not pd.NaT else None,
            "cause": cause_primary,
        })

    return rows


def _si(val) -> int | None:
    """Safe int — return None for NaN/None."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if np.isnan(f) else int(f)
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    import json
    import logging

    import src.settings  # noqa: F401 — load env vars

    from src.like_day_forecast import configs
    from src.data import transmission_outages as tx_data
    from src.utils.cache_utils import pull_with_cache

    logging.basicConfig(level=logging.INFO)

    CACHE = dict(
        cache_dir=configs.CACHE_DIR,
        cache_enabled=configs.CACHE_ENABLED,
        ttl_hours=configs.CACHE_TTL_HOURS,
        force_refresh=configs.FORCE_CACHE_REFRESH,
    )

    df = pull_with_cache(
        source_name="pjm_transmission_outages",
        pull_fn=tx_data.pull,
        pull_kwargs={},
        **CACHE,
    )

    vm = build_view_model(df)
    print(json.dumps(vm, indent=2, default=str))
