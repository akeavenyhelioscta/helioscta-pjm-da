from __future__ import annotations

import itertools
import re
import zipfile
from pathlib import Path

import pandas as pd
import requests


BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent.parent
WORKBOOK_PATH = ROOT_DIR / ".excel" / "PJM_Stack_Model_v1_2026_mar_10.xlsx"
OFFICIAL_DIR = ROOT_DIR / ".excel" / "official_sources"
OUT_DIR = BASE_DIR / "csvs" / "RTO"

EIA_860M_URL = "https://www.eia.gov/electricity/data/eia860m/xls/january_generator2026.xlsx"
EIA_860M_PATH = OFFICIAL_DIR / "eia860m_jan2026.xlsx"
EIA_860_URL = "https://www.eia.gov/electricity/data/eia860/xls/eia8602024.zip"
EIA_860_ZIP_PATH = OFFICIAL_DIR / "eia860_2024.zip"
EIA_860_EXTRACT_DIR = OFFICIAL_DIR / "eia860_2024"
EIA_860_GEN_PATH = EIA_860_EXTRACT_DIR / "3_1_Generator_Y2024.xlsx"
EIA_860_OWNER_PATH = EIA_860_EXTRACT_DIR / "4___Owner_Y2024.xlsx"

OUTPUT_CSV = OUT_DIR / "official_capacity_verification_2026_03_11.csv"
OUTPUT_MD = OUT_DIR / "official_capacity_verification_2026_03_11.md"

CAPACITY_TOLERANCE_MW = 0.11
MAX_SUBSET_CANDIDATES = 10
MAX_SUBSET_SIZE = 6

SOURCE_URLS = {
    "860m": EIA_860M_URL,
    "860_annual": EIA_860_URL,
}

SHEET_TITLES_860M = {
    "Operating": "Inventory of Operating Generators as of January 2026",
    "Retired": "Inventory of Retired Generators as of January 2026",
    "Planned": "Inventory of Planned Generators as of January 2026",
    "Canceled or Postponed": "Inventory of Canceled or Indefinitely Postponed Projects as of January 2026",
}

SHEET_TITLES_860 = {
    "Operable": "2024 Form EIA-860 Data - Schedule 3, 'Generator Data' (Operable Units Only)",
    "Proposed": "2024 Form EIA-860 Data - Schedule 3, 'Generator Data' (Proposed Units Only)",
    "Retired and Canceled": "2024 Form EIA-860 Data - Schedule 3, 'Generator Data' (Retired & Canceled Units Only)",
    "Ownership": "2024 Form EIA-860 Data - Schedule 4, 'Generator Ownership' (Jointly or Third-Party Owned Only)",
}


def ensure_file(url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        return
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    destination.write_bytes(response.content)


def ensure_official_sources() -> None:
    ensure_file(EIA_860M_URL, EIA_860M_PATH)
    ensure_file(EIA_860_URL, EIA_860_ZIP_PATH)
    if not EIA_860_GEN_PATH.exists() or not EIA_860_OWNER_PATH.exists():
        EIA_860_EXTRACT_DIR.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(EIA_860_ZIP_PATH) as archive:
            archive.extractall(EIA_860_EXTRACT_DIR)


def normalize_name(value: object) -> str:
    return re.sub(r"[^a-z0-9\s]", "", str(value).lower()).strip()


def strip_trailing_location(plant_name: str) -> str:
    return re.sub(r"\s*\([^)]*,\s*[A-Z]{2}\)$", "", str(plant_name)).strip()


def extract_state_hint(plant_name: str) -> str | None:
    match = re.search(r"\(([^,]+),\s*([A-Z]{2})\)$", str(plant_name))
    return match.group(2) if match else None


def fuel_bucket_from_eia(
    technology: object, energy_source_code: object, prime_mover_code: object
) -> str:
    technology_text = str(technology).lower()
    energy_source = str(energy_source_code).upper().strip()
    prime_mover = str(prime_mover_code).upper().strip()

    if "nuclear" in technology_text or energy_source == "NUC":
        return "Nuclear"
    if "coal" in technology_text or energy_source in {"BIT", "SUB", "LIG", "WC", "RC", "SC"}:
        return "Coal"
    if "solar" in technology_text or energy_source == "SUN":
        return "Solar"
    if "wind" in technology_text or energy_source == "WND":
        return "Wind"
    if (
        "hydroelectric" in technology_text
        or "hydrokinetic" in technology_text
        or energy_source == "WAT"
        or prime_mover in {"HY", "PS"}
    ):
        return "Hydro"
    if (
        energy_source
        in {"WDS", "LFG", "BLQ", "OBG", "OBL", "SLW", "MSW", "AB", "OBS", "WDL", "WOO", "WWW"}
        or "biomass" in technology_text
        or "landfill" in technology_text
        or "wood" in technology_text
        or "municipal solid waste" in technology_text
    ):
        return "Biomass"
    if energy_source in {"DFO", "RFO", "JF", "KER", "PC", "WO"} or "petroleum" in technology_text:
        return "Oil"
    if "battery" in technology_text or energy_source in {"MWH", "WH"}:
        return "Storage"
    if (
        energy_source in {"NG", "BFG", "OG", "PG"}
        or "natural gas" in technology_text
        or prime_mover in {"GT", "CT", "CA", "CS", "CC", "ST", "IC"}
    ):
        if "combined cycle" in technology_text or prime_mover in {"CA", "CS", "CC"}:
            return "Gas CC"
        return "Gas CT/ST"
    return "Other"


def to_number(value: object) -> float | None:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return None
    return float(numeric)


def approx_equal(left: object, right: object, tolerance: float = CAPACITY_TOLERANCE_MW) -> bool:
    left_value = to_number(left)
    right_value = to_number(right)
    if left_value is None or right_value is None:
        return False
    return abs(left_value - right_value) <= tolerance


def format_number(value: object) -> str:
    numeric = to_number(value)
    if numeric is None:
        return "n/a"
    if abs(numeric - round(numeric)) < 1e-9:
        return str(int(round(numeric)))
    return f"{numeric:.3f}".rstrip("0").rstrip(".")


def load_workbook_records() -> pd.DataFrame:
    raw = pd.read_excel(WORKBOOK_PATH, sheet_name="PJM Raw Data", header=1)
    raw = raw.dropna(subset=[raw.columns[0]]).rename(
        columns={
            raw.columns[0]: "plant_name",
            raw.columns[1]: "fuel_category",
            raw.columns[2]: "unit_type",
            raw.columns[3]: "power_hub",
            raw.columns[4]: "zone",
            raw.columns[5]: "fuel_hub",
            raw.columns[6]: "summer_cap_mw",
            raw.columns[7]: "winter_cap_mw",
        }
    )

    key_columns = ["plant_name", "fuel_category", "unit_type", "summer_cap_mw", "winter_cap_mw"]
    grouped = raw.groupby(key_columns).size().reset_index(name="wb_occurrences")
    grouped["plant_name_base"] = grouped["plant_name"].map(strip_trailing_location)
    grouped["name_clean_base"] = grouped["plant_name_base"].map(normalize_name)
    grouped["name_clean_full"] = grouped["plant_name"].map(normalize_name)
    grouped["state_hint"] = grouped["plant_name"].map(extract_state_hint)
    return grouped


def load_eia_860m() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for sheet_name in SHEET_TITLES_860M:
        frame = pd.read_excel(EIA_860M_PATH, sheet_name=sheet_name, header=2)
        frame["source_sheet"] = sheet_name
        frame["source_title"] = SHEET_TITLES_860M[sheet_name]
        frame["source_date"] = "2026-01"
        frame["source_url"] = SOURCE_URLS["860m"]
        frame["sheet_row"] = range(4, 4 + len(frame))
        frames.append(frame)

    eia = pd.concat(frames, ignore_index=True).rename(
        columns={
            "Plant ID": "plant_id_eia",
            "Plant Name": "plant_name_eia",
            "Plant State": "state",
            "County": "county",
            "Generator ID": "generator_id",
            "Entity Name": "entity_name",
            "Nameplate Capacity (MW)": "nameplate_mw",
            "Net Summer Capacity (MW)": "summer_mw",
            "Net Winter Capacity (MW)": "winter_mw",
            "Technology": "technology",
            "Energy Source Code": "energy_source_code",
            "Prime Mover Code": "prime_mover_code",
            "Balancing Authority Code": "ba_code",
            "Status": "status",
        }
    )

    for column in ["plant_id_eia", "nameplate_mw", "summer_mw", "winter_mw"]:
        eia[column] = pd.to_numeric(eia[column], errors="coerce")

    eia["name_clean_base"] = eia["plant_name_eia"].map(normalize_name)
    eia["fuel_bucket"] = [
        fuel_bucket_from_eia(technology, energy_source_code, prime_mover_code)
        for technology, energy_source_code, prime_mover_code in zip(
            eia["technology"], eia["energy_source_code"], eia["prime_mover_code"]
        )
    ]
    return eia


def load_eia_860_annual() -> tuple[pd.DataFrame, pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    for sheet_name in ["Operable", "Proposed", "Retired and Canceled"]:
        frame = pd.read_excel(EIA_860_GEN_PATH, sheet_name=sheet_name, header=1)
        frame["annual_sheet_name"] = sheet_name
        frame["annual_sheet_row"] = range(3, 3 + len(frame))
        frame["annual_source_title"] = SHEET_TITLES_860[sheet_name]
        frames.append(frame)

    annual = pd.concat(frames, ignore_index=True).rename(
        columns={
            "Plant Code": "plant_id_eia",
            "Plant Name": "plant_name_eia",
            "State": "state",
            "County": "county",
            "Generator ID": "generator_id",
            "Utility Name": "utility_name_eia",
            "Nameplate Capacity (MW)": "nameplate_mw_2024",
            "Summer Capacity (MW)": "summer_mw_2024",
            "Winter Capacity (MW)": "winter_mw_2024",
            "Technology": "technology_2024",
            "Prime Mover": "prime_mover_code_2024",
            "Status": "status_2024",
        }
    )
    for column in ["plant_id_eia", "nameplate_mw_2024", "summer_mw_2024", "winter_mw_2024"]:
        annual[column] = pd.to_numeric(annual[column], errors="coerce")

    owners = pd.read_excel(EIA_860_OWNER_PATH, sheet_name="Ownership", header=1).rename(
        columns={
            "Plant Code": "plant_id_eia",
            "Generator ID": "generator_id",
            "Owner Name": "owner_name",
            "Percent Owned": "percent_owned",
            "Utility Name": "utility_name_eia",
        }
    )
    owners["plant_id_eia"] = pd.to_numeric(owners["plant_id_eia"], errors="coerce")
    owners["percent_owned"] = pd.to_numeric(owners["percent_owned"], errors="coerce")
    owners["owner_sheet_row"] = range(3, 3 + len(owners))
    return annual, owners


def build_plant_reference(eia: pd.DataFrame) -> pd.DataFrame:
    reference = eia[["plant_id_eia", "plant_name_eia", "name_clean_base", "state"]].drop_duplicates()
    name_counts = (
        reference.groupby("name_clean_base")["plant_id_eia"].nunique().reset_index(name="n_plants")
    )
    return reference.merge(name_counts, on="name_clean_base", how="left")


def candidate_plants_for_workbook(
    workbook_records: pd.DataFrame, plant_reference: pd.DataFrame
) -> pd.DataFrame:
    merged = workbook_records.merge(plant_reference, on="name_clean_base", how="left")
    exact_state = merged["state_hint"].notna() & (merged["state_hint"] == merged["state"])
    unique_name = merged["state_hint"].isna() & (merged["n_plants"] == 1)
    return (
        merged[exact_state | unique_name]
        .sort_values(["plant_id_eia", "plant_name_eia"])
        .drop_duplicates(
            subset=["plant_name", "fuel_category", "unit_type", "summer_cap_mw", "winter_cap_mw"],
            keep="first",
        )
    )


def filter_fuel_candidates(candidates: pd.DataFrame, workbook_fuel: str) -> pd.DataFrame:
    filtered = candidates[candidates["fuel_bucket"] == workbook_fuel].copy()
    if not filtered.empty:
        return filtered
    if workbook_fuel in {"Gas CC", "Gas CT/ST"}:
        fallback = candidates[candidates["fuel_bucket"].isin(["Gas CC", "Gas CT/ST"])].copy()
        if not fallback.empty:
            return fallback
    return candidates.copy()


def build_owner_lookup(owners: pd.DataFrame) -> dict[tuple[int, str], list[dict[str, object]]]:
    grouped: dict[tuple[int, str], list[dict[str, object]]] = {}
    for _, row in owners.iterrows():
        plant_id = to_number(row["plant_id_eia"])
        generator_id = str(row["generator_id"]).strip()
        if plant_id is None or not generator_id:
            continue
        key = (int(plant_id), generator_id)
        grouped.setdefault(key, []).append(
            {
                "owner_name": row["owner_name"],
                "percent_owned": row["percent_owned"],
            }
        )
    return grouped


def build_annual_lookup(annual: pd.DataFrame) -> dict[tuple[int, str], dict[str, object]]:
    grouped: dict[tuple[int, str], dict[str, object]] = {}
    for _, row in annual.iterrows():
        plant_id = to_number(row["plant_id_eia"])
        generator_id = str(row["generator_id"]).strip()
        if plant_id is None or not generator_id:
            continue
        grouped[(int(plant_id), generator_id)] = row.to_dict()
    return grouped


def ownership_text(
    operator_name: object,
    plant_id_eia: int,
    generator_ids: list[str],
    owner_lookup: dict[tuple[int, str], list[dict[str, object]]],
) -> str:
    owners: list[str] = []
    for generator_id in generator_ids:
        for owner in owner_lookup.get((plant_id_eia, generator_id), []):
            owner_name = str(owner["owner_name"]).strip()
            percent_owned = to_number(owner["percent_owned"])
            if owner_name:
                if percent_owned is None:
                    owners.append(owner_name)
                else:
                    owners.append(f"{owner_name} ({format_number(percent_owned)}%)")
    owners = sorted(set(owners))
    operator_text = str(operator_name).strip()
    if owners:
        return f"Owner(s): {'; '.join(owners)}; operator/reporting entity: {operator_text}"
    return f"{operator_text} (operator/reporting entity per EIA 860M)"


def subset_sum_match(candidates: pd.DataFrame, metric_column: str, target: float) -> pd.DataFrame | None:
    if len(candidates) < 2 or len(candidates) > MAX_SUBSET_CANDIDATES:
        return None
    usable = candidates[candidates[metric_column].notna()].copy()
    if len(usable) < 2 or len(usable) > MAX_SUBSET_CANDIDATES:
        return None

    indexes = list(usable.index)
    matches: list[tuple[int, ...]] = []
    for subset_size in range(2, min(len(indexes), MAX_SUBSET_SIZE) + 1):
        for combo in itertools.combinations(indexes, subset_size):
            total = usable.loc[list(combo), metric_column].sum()
            if approx_equal(total, target):
                matches.append(combo)
                if len(matches) > 1:
                    return None
    if len(matches) != 1:
        return None
    return usable.loc[list(matches[0])].copy()


def official_capacity_notes(
    eia_rows: pd.DataFrame,
    annual_lookup: dict[tuple[int, str], dict[str, object]],
) -> list[str]:
    notes: list[str] = []
    for _, row in eia_rows.iterrows():
        plant_id = int(row["plant_id_eia"])
        generator_id = str(row["generator_id"]).strip()
        annual_row = annual_lookup.get((plant_id, generator_id))
        if not annual_row:
            continue
        annual_triplet = (
            format_number(annual_row.get("nameplate_mw_2024")),
            format_number(annual_row.get("summer_mw_2024")),
            format_number(annual_row.get("winter_mw_2024")),
        )
        current_triplet = (
            format_number(row.get("nameplate_mw")),
            format_number(row.get("summer_mw")),
            format_number(row.get("winter_mw")),
        )
        if annual_triplet != current_triplet:
            notes.append(
                f"2024 EIA-860 {annual_row['annual_sheet_name']} row {annual_row['annual_sheet_row']} reports "
                f"nameplate/summer/winter {annual_triplet[0]}/{annual_triplet[1]}/{annual_triplet[2]} MW; "
                f"Jan 2026 860M shows {current_triplet[0]}/{current_triplet[1]}/{current_triplet[2]} MW."
            )
    return notes


def workbook_vs_official_note(workbook_row: pd.Series, eia_rows: pd.DataFrame) -> str:
    official_nameplate = eia_rows["nameplate_mw"].sum(min_count=1)
    official_summer = eia_rows["summer_mw"].sum(min_count=1)
    official_winter = eia_rows["winter_mw"].sum(min_count=1)
    return (
        f"Workbook row(s): summer/winter {format_number(workbook_row['summer_cap_mw'])}/"
        f"{format_number(workbook_row['winter_cap_mw'])} MW; repeated {int(workbook_row['wb_occurrences'])}x. "
        f"EIA totals for matched unit set: nameplate/summer/winter "
        f"{format_number(official_nameplate)}/{format_number(official_summer)}/{format_number(official_winter)} MW."
    )


def build_output_row(
    workbook_row: pd.Series,
    eia_rows: pd.DataFrame,
    owner_lookup: dict[tuple[int, str], list[dict[str, object]]],
    annual_lookup: dict[tuple[int, str], dict[str, object]],
    capacity_value: object,
    capacity_type: str,
    verification_status: str,
    match_note: str,
) -> dict[str, object]:
    first = eia_rows.iloc[0]
    generator_ids = [str(generator_id).strip() for generator_id in eia_rows["generator_id"].tolist()]
    location = f"{str(first['county']).strip()}, {str(first['state']).strip()}".strip(", ")
    plant_id = int(first["plant_id_eia"])
    plant_unit_text = f"{first['plant_name_eia']} / {', '.join(generator_ids)}"
    notes = [
        workbook_vs_official_note(workbook_row, eia_rows),
        match_note,
        (
            f"EIA 860M {first['source_sheet']} sheet row(s): "
            + ", ".join(f"row {int(row)}" for row in eia_rows["sheet_row"].tolist())
            + "."
        ),
    ]
    notes.extend(official_capacity_notes(eia_rows, annual_lookup))
    if verification_status == "verified - discrepancy" and first["fuel_bucket"] in {"Solar", "Wind", "Storage"}:
        notes.append(
            "Workbook capacity appears lower than EIA installed capability; this may reflect a derated or accredited model input rather than installed capacity (inference)."
        )

    return {
        "input generator name": workbook_row["plant_name"],
        "matched plant / unit": plant_unit_text,
        "fuel type": first["fuel_bucket"],
        "location": location,
        "owner / operator": ownership_text(first["entity_name"], plant_id, generator_ids, owner_lookup),
        "capacity value": format_number(capacity_value),
        "capacity type": capacity_type,
        "source organization": "U.S. Energy Information Administration (EIA)",
        "source title": first["source_title"],
        "source date": first["source_date"],
        "source URL": first["source_url"],
        "verification status": verification_status,
        "notes": " ".join(note for note in notes if note),
    }


def build_unverified_row(
    workbook_row: pd.Series,
    notes: str,
    source_row: pd.Series | None = None,
) -> dict[str, object]:
    if source_row is None:
        source_org = ""
        source_title = ""
        source_date = ""
        source_url = ""
    else:
        source_org = "U.S. Energy Information Administration (EIA)"
        source_title = source_row["source_title"]
        source_date = source_row["source_date"]
        source_url = source_row["source_url"]
    return {
        "input generator name": workbook_row["plant_name"],
        "matched plant / unit": "",
        "fuel type": workbook_row["fuel_category"],
        "location": workbook_row["state_hint"] or "",
        "owner / operator": "",
        "capacity value": "",
        "capacity type": "",
        "source organization": source_org,
        "source title": source_title,
        "source date": source_date,
        "source URL": source_url,
        "verification status": "unverified",
        "notes": notes,
    }


def match_records(
    workbook_records: pd.DataFrame,
    eia: pd.DataFrame,
    annual_lookup: dict[tuple[int, str], dict[str, object]],
    owner_lookup: dict[tuple[int, str], list[dict[str, object]]],
) -> pd.DataFrame:
    key_columns = ["plant_name", "fuel_category", "unit_type", "summer_cap_mw", "winter_cap_mw"]
    plant_reference = build_plant_reference(eia)
    plant_matches = candidate_plants_for_workbook(workbook_records, plant_reference)

    matched_key_set = set(
        tuple(values)
        for values in plant_matches[key_columns].itertuples(index=False, name=None)
    )

    output_rows: list[dict[str, object]] = []

    for _, workbook_row in plant_matches.iterrows():
        candidate_rows = eia[eia["plant_id_eia"] == workbook_row["plant_id_eia"]].copy()
        fuel_candidates = filter_fuel_candidates(candidate_rows, workbook_row["fuel_category"])

        exact_sw = fuel_candidates[
            fuel_candidates["summer_mw"].apply(lambda value: approx_equal(value, workbook_row["summer_cap_mw"]))
            & fuel_candidates["winter_mw"].apply(lambda value: approx_equal(value, workbook_row["winter_cap_mw"]))
        ].copy()
        exact_s = fuel_candidates[
            fuel_candidates["summer_mw"].apply(lambda value: approx_equal(value, workbook_row["summer_cap_mw"]))
        ].copy()
        exact_w = fuel_candidates[
            fuel_candidates["winter_mw"].apply(lambda value: approx_equal(value, workbook_row["winter_cap_mw"]))
        ].copy()
        exact_n = fuel_candidates[
            fuel_candidates["nameplate_mw"].apply(
                lambda value: approx_equal(value, workbook_row["summer_cap_mw"])
                or approx_equal(value, workbook_row["winter_cap_mw"])
            )
        ].copy()

        repeated_count = int(workbook_row["wb_occurrences"])

        if repeated_count > 1 and len(exact_sw) == repeated_count and len(exact_sw) > 1:
            for _, unit_row in exact_sw.iterrows():
                output_rows.append(
                    build_output_row(
                        workbook_row,
                        unit_row.to_frame().T,
                        owner_lookup,
                        annual_lookup,
                        unit_row["summer_mw"],
                        "summer MW",
                        "verified",
                        "Repeated workbook record count matches the number of exact EIA generator-level summer/winter matches.",
                    )
                )
            continue

        if repeated_count > 1 and len(exact_s) == repeated_count and len(exact_s) > 1:
            for _, unit_row in exact_s.iterrows():
                output_rows.append(
                    build_output_row(
                        workbook_row,
                        unit_row.to_frame().T,
                        owner_lookup,
                        annual_lookup,
                        unit_row["summer_mw"],
                        "summer MW",
                        "verified",
                        "Repeated workbook record count matches the number of exact EIA generator-level summer matches.",
                    )
                )
            continue

        if len(exact_sw) == 1:
            output_rows.append(
                build_output_row(
                    workbook_row,
                    exact_sw,
                    owner_lookup,
                    annual_lookup,
                    exact_sw.iloc[0]["summer_mw"],
                    "summer MW",
                    "verified",
                    "Unique EIA generator matched on plant name, fuel, net summer MW, and net winter MW.",
                )
            )
            continue

        if len(exact_s) == 1:
            output_rows.append(
                build_output_row(
                    workbook_row,
                    exact_s,
                    owner_lookup,
                    annual_lookup,
                    exact_s.iloc[0]["summer_mw"],
                    "summer MW",
                    "verified",
                    "Unique EIA generator matched on plant name, fuel, and net summer MW.",
                )
            )
            continue

        if len(exact_w) == 1:
            output_rows.append(
                build_output_row(
                    workbook_row,
                    exact_w,
                    owner_lookup,
                    annual_lookup,
                    exact_w.iloc[0]["winter_mw"],
                    "winter MW",
                    "verified",
                    "Unique EIA generator matched on plant name, fuel, and net winter MW.",
                )
            )
            continue

        if len(exact_n) == 1:
            output_rows.append(
                build_output_row(
                    workbook_row,
                    exact_n,
                    owner_lookup,
                    annual_lookup,
                    exact_n.iloc[0]["nameplate_mw"],
                    "nameplate MW",
                    "verified",
                    "Unique EIA generator matched on plant name, fuel, and nameplate MW.",
                )
            )
            continue

        summer_target = to_number(workbook_row["summer_cap_mw"]) or 0.0
        winter_target = to_number(workbook_row["winter_cap_mw"]) or 0.0

        summer_subset = subset_sum_match(fuel_candidates, "summer_mw", summer_target)
        if summer_subset is not None:
            output_rows.append(
                build_output_row(
                    workbook_row,
                    summer_subset,
                    owner_lookup,
                    annual_lookup,
                    summer_subset["summer_mw"].sum(),
                    "summer MW",
                    "verified",
                    "Workbook record appears to represent a plant-level aggregation of multiple EIA generators whose net summer MW sum matches.",
                )
            )
            continue

        winter_subset = subset_sum_match(fuel_candidates, "winter_mw", winter_target)
        if winter_subset is not None:
            output_rows.append(
                build_output_row(
                    workbook_row,
                    winter_subset,
                    owner_lookup,
                    annual_lookup,
                    winter_subset["winter_mw"].sum(),
                    "winter MW",
                    "verified",
                    "Workbook record appears to represent a plant-level aggregation of multiple EIA generators whose net winter MW sum matches.",
                )
            )
            continue

        nameplate_subset = subset_sum_match(
            fuel_candidates,
            "nameplate_mw",
            summer_target or winter_target,
        )
        if nameplate_subset is not None:
            output_rows.append(
                build_output_row(
                    workbook_row,
                    nameplate_subset,
                    owner_lookup,
                    annual_lookup,
                    nameplate_subset["nameplate_mw"].sum(),
                    "nameplate MW",
                    "verified",
                    "Workbook record appears to represent a plant-level aggregation of multiple EIA generators whose nameplate MW sum matches.",
                )
            )
            continue

        if len(fuel_candidates) == 1:
            official_row = fuel_candidates.iloc[0]
            output_rows.append(
                build_output_row(
                    workbook_row,
                    fuel_candidates,
                    owner_lookup,
                    annual_lookup,
                    official_row["summer_mw"],
                    "summer MW",
                    "verified - discrepancy",
                    "Plant and fuel match are unique in EIA, but workbook capacity does not equal the official EIA capacity fields.",
                )
            )
            continue

        candidate_text = ", ".join(
            f"{row.generator_id} (summer {format_number(row.summer_mw)} MW)"
            for row in fuel_candidates[["generator_id", "summer_mw"]].itertuples(index=False)
        )
        output_rows.append(
            build_unverified_row(
                workbook_row,
                notes=(
                    f"Exact EIA plant match found ({workbook_row['plant_name_eia']}), but unit-level assignment is ambiguous. "
                    f"Workbook row(s): summer/winter {format_number(workbook_row['summer_cap_mw'])}/"
                    f"{format_number(workbook_row['winter_cap_mw'])} MW; repeated {repeated_count}x. "
                    f"Fuel-compatible EIA candidates: {candidate_text}."
                ),
                source_row=fuel_candidates.iloc[0] if not fuel_candidates.empty else candidate_rows.iloc[0],
            )
        )

    unmatched_records = workbook_records[
        ~workbook_records[key_columns].apply(tuple, axis=1).isin(matched_key_set)
    ].copy()
    for _, workbook_row in unmatched_records.iterrows():
        output_rows.append(
            build_unverified_row(
                workbook_row,
                notes=(
                    "No confident exact-name EIA 860M plant match found after stripping trailing location text. "
                    f"Workbook row(s): summer/winter {format_number(workbook_row['summer_cap_mw'])}/"
                    f"{format_number(workbook_row['winter_cap_mw'])} MW; repeated {int(workbook_row['wb_occurrences'])}x."
                ),
            )
        )

    output = pd.DataFrame(output_rows)
    return output.sort_values(
        ["verification status", "input generator name", "matched plant / unit"]
    ).reset_index(drop=True)


def write_markdown_summary(output: pd.DataFrame) -> None:
    status_counts = output["verification status"].value_counts().to_dict()
    total_rows = len(output)
    verified_rows = sum(
        count for status, count in status_counts.items() if str(status).startswith("verified")
    )
    discrepancy_rows = status_counts.get("verified - discrepancy", 0)
    unverified_rows = status_counts.get("unverified", 0)

    naming_ambiguity_examples = output[
        output["verification status"] == "unverified"
    ]["input generator name"].drop_duplicates().head(15)
    discrepancy_examples = output[
        output["verification status"] == "verified - discrepancy"
    ][["input generator name", "notes"]].head(15)

    lines = [
        "# Official Capacity Verification",
        "",
        f"Full table: `{OUTPUT_CSV}`",
        "",
        "## Source Notes",
        "",
        f"- Output rows: {total_rows:,}. Verified rows: {verified_rows:,}. Verified but capacity-discrepant rows: {discrepancy_rows:,}. Unverified rows: {unverified_rows:,}.",
        "- Methodology: repeated identical workbook rows were collapsed into a single input record unless the repeated count matched the number of distinct official EIA generators with the same plant/fuel/capacity signature; those cases were re-expanded to generator-level rows.",
        "- Naming ambiguities: many unresolved records are portfolio-style solar or storage projects whose workbook names do not exactly match EIA plant names. These remain `unverified` rather than being force-matched.",
        "- Conflicting capacity values: when 2024 EIA-860 and January 2026 EIA-860M differed, the January 2026 EIA-860M value was treated as the more authoritative current record and the older 2024 value was noted in the row notes.",
        "- Capacity discrepancies: a large share of solar, wind, and storage records match an official plant/unit name but not the workbook MW. Where the plant/unit match is still unique, those rows are marked `verified - discrepancy`; the lower workbook MW may reflect derated, accredited, or modeled capability rather than installed EIA capacity (inference).",
        "- Missing or unavailable official documentation: records with no confident EIA exact-name match remain `unverified`. Additional owner/operator filings, PJM documents, or state records would be needed to push coverage higher for those names.",
        "",
        "Naming ambiguity examples:",
    ]
    for name in naming_ambiguity_examples:
        lines.append(f"- {name}")

    lines.extend(["", "Conflicting/discrepant official-capacity examples:"])
    for _, row in discrepancy_examples.iterrows():
        lines.append(f"- {row['input generator name']}: {row['notes']}")

    lines.extend(
        [
            "",
            "## Official Sources Used",
            "",
            f"- U.S. Energy Information Administration (EIA), *{SHEET_TITLES_860M['Operating']}* and companion sheets in January 2026 EIA-860M workbook. URL: {EIA_860M_URL}",
            f"- U.S. Energy Information Administration (EIA), *{SHEET_TITLES_860['Operable']}* and companion sheets in the 2024 Form EIA-860 data files. URL: {EIA_860_URL}",
            f"- U.S. Energy Information Administration (EIA), *{SHEET_TITLES_860['Ownership']}*. URL: {EIA_860_URL}",
        ]
    )

    OUTPUT_MD.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    ensure_official_sources()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    workbook_records = load_workbook_records()
    eia = load_eia_860m()
    annual, owners = load_eia_860_annual()
    annual_lookup = build_annual_lookup(annual)
    owner_lookup = build_owner_lookup(owners)

    output = match_records(workbook_records, eia, annual_lookup, owner_lookup)
    output.to_csv(OUTPUT_CSV, index=False)
    write_markdown_summary(output)

    print(f"Wrote {OUTPUT_CSV} ({len(output):,} rows)")
    print(f"Wrote {OUTPUT_MD}")
    print(output['verification status'].value_counts().to_string())


if __name__ == "__main__":
    main()
