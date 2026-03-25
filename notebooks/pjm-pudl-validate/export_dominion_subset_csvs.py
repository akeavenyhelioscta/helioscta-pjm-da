from __future__ import annotations

import re
from difflib import SequenceMatcher
from pathlib import Path

import numpy as np
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
RTO_DIR = BASE_DIR / "csvs" / "RTO"
OUT_DIR = BASE_DIR / "csvs" / "Dominion"

DOMINION_HUB = "PJM Dominion"
DOMINION_ZONE = "DOM"
PROXY_STATES = ["MD", "NC", "VA"]
ROMAN_MAP = {"i": "1", "ii": "2", "iii": "3", "iv": "4", "v": "5", "vi": "6"}
STOPWORDS = {
    "center",
    "co",
    "company",
    "corp",
    "dominion",
    "energy",
    "facility",
    "farm",
    "inc",
    "llc",
    "plant",
    "power",
    "project",
    "solar",
    "spower",
    "station",
}
ALIAS_STOPWORDS = STOPWORDS | {
    "baywa",
    "brookfield",
    "brink",
    "capital",
    "cypress",
    "duke",
    "ecoplexus",
    "engie",
    "navisun",
    "pv",
    "renew",
    "sol",
    "sunenergy",
    "sunenergy1",
    "tpe",
    "virginia",
}


def read_csv(name: str) -> pd.DataFrame:
    return pd.read_csv(RTO_DIR / name)


def normalize_name(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.lower()
        .str.replace(r"[^a-z0-9\s]", "", regex=True)
        .str.strip()
    )


def clean_scalar(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9\s]", " ", str(text).lower())).strip()


def fuzzy_tokens(text: str) -> list[str]:
    tokens = []
    for token in clean_scalar(text).replace("photovoltaic", "solar").split():
        token = ROMAN_MAP.get(token, token)
        if token in STOPWORDS or token in {"md", "nc", "va"}:
            continue
        tokens.append(token)
    return tokens


def fuzzy_name(text: str) -> str:
    return " ".join(fuzzy_tokens(text))


def alias_tokens(text: str) -> list[str]:
    text = re.sub(r"\([^)]*\)", " ", str(text))
    tokens = []
    for token in clean_scalar(text).replace("photovoltaic", "solar").split():
        token = ROMAN_MAP.get(token, token)
        if token in ALIAS_STOPWORDS or token in {"md", "nc", "va"}:
            continue
        if token.isdigit():
            continue
        tokens.append(token)
    return tokens


def alias_key(text: str) -> str:
    return " ".join(alias_tokens(text))


def extract_state(text: str) -> str | None:
    match = re.search(r"\b(MD|NC|VA)\b", str(text))
    return match.group(1) if match else None


def fuel_match(workbook_fuel: str, pudl_fuel: str) -> bool:
    return workbook_fuel == pudl_fuel or workbook_fuel == "Other"


def seq_ratio(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    return SequenceMatcher(None, left, right).ratio()


def overlap_score(left: list[str], right: list[str]) -> float:
    left_set = set(left)
    right_set = set(right)
    if not left_set or not right_set:
        return 0.0
    return len(left_set & right_set) / len(left_set | right_set)


def fuzzy_token_score(left: list[str], right: list[str]) -> float:
    if not left or not right:
        return 0.0
    if len(left) > len(right):
        left, right = right, left
    scores = []
    for token in left:
        scores.append(max(seq_ratio(token, other) for other in right))
    return float(sum(scores) / len(scores))


def cap_score(wb_mw: float, pudl_mw: float) -> float:
    if wb_mw <= 0 or pudl_mw <= 0:
        return 0.0
    return min(wb_mw, pudl_mw) / max(wb_mw, pudl_mw)


def save_csv(
    manifest: list[dict[str, object]], file_name: str, df: pd.DataFrame, description: str
) -> None:
    path = OUT_DIR / file_name
    df.to_csv(path, index=False)
    manifest.append(
        {"file_name": file_name, "rows": len(df), "columns": len(df.columns), "description": description}
    )
    print(f"Wrote {file_name} ({len(df):,} rows)")


def finalize_matches(matches: pd.DataFrame) -> pd.DataFrame:
    if len(matches) == 0:
        return matches
    matches = matches.copy()
    matches["cap_diff_mw"] = matches["wb_summer_mw"] - matches["pudl_summer_mw"]
    matches["cap_diff_pct"] = (
        matches["cap_diff_mw"] / matches["pudl_summer_mw"].replace(0, np.nan) * 100
    ).round(1)
    return matches


def build_best_matches(
    workbook_plants: pd.DataFrame, pudl_plants: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    exact = workbook_plants.merge(
        pudl_plants,
        on="name_clean",
        how="inner",
        suffixes=("_wb", "_pudl"),
    )
    exact = exact[exact.apply(lambda row: fuel_match(row["wb_fuel"], row["pudl_fuel_cat"]), axis=1)]
    if len(exact) > 0:
        exact["match_method"] = "exact"
        exact["match_score"] = 1.0
        exact["name_score"] = 1.0
        exact["token_overlap_score"] = 1.0
        exact["token_fuzzy_score"] = 1.0
        exact["capacity_score"] = exact.apply(
            lambda row: cap_score(row["wb_summer_mw"], row["pudl_summer_mw"]), axis=1
        )
        exact = exact.sort_values(
            ["capacity_score", "wb_summer_mw"], ascending=[False, False]
        ).drop_duplicates("plant_name").drop_duplicates("plant_name_eia")

    wb_unmatched = workbook_plants[
        ~workbook_plants["plant_name"].isin(set(exact["plant_name"]))
    ].copy()
    pudl_unmatched = pudl_plants[
        ~pudl_plants["plant_name_eia"].isin(set(exact["plant_name_eia"]))
    ].copy()

    candidates: list[dict[str, object]] = []
    for wb_row in wb_unmatched.itertuples(index=False):
        pool = pudl_unmatched.copy()
        if wb_row.wb_state_guess:
            state_pool = pool[pool["state"] == wb_row.wb_state_guess]
            if len(state_pool) > 0:
                pool = state_pool
        fuel_pool = pool[pool["pudl_fuel_cat"].apply(lambda fuel: fuel_match(wb_row.wb_fuel, fuel))]
        if len(fuel_pool) > 0:
            pool = fuel_pool
        for pudl_row in pool.itertuples(index=False):
            name_score = seq_ratio(wb_row.fuzzy_name, pudl_row.fuzzy_name)
            token_overlap = overlap_score(wb_row.fuzzy_tokens, pudl_row.fuzzy_tokens)
            token_fuzzy = fuzzy_token_score(wb_row.fuzzy_tokens, pudl_row.fuzzy_tokens)
            capacity = cap_score(wb_row.wb_summer_mw, pudl_row.pudl_summer_mw)
            score = 0.5 * name_score + 0.2 * token_fuzzy + 0.15 * token_overlap + 0.15 * capacity
            if score < 0.72:
                continue
            if token_fuzzy < 0.55 and name_score < 0.82:
                continue
            candidates.append(
                {
                    "plant_name": wb_row.plant_name,
                    "wb_summer_mw": wb_row.wb_summer_mw,
                    "wb_units": wb_row.wb_units,
                    "wb_fuel": wb_row.wb_fuel,
                    "wb_hub": wb_row.wb_hub,
                    "wb_state_guess": wb_row.wb_state_guess,
                    "name_clean": wb_row.name_clean,
                    "plant_id_eia": pudl_row.plant_id_eia,
                    "plant_name_eia": pudl_row.plant_name_eia,
                    "pudl_summer_mw": pudl_row.pudl_summer_mw,
                    "pudl_units": pudl_row.pudl_units,
                    "pudl_fuel": pudl_row.pudl_fuel,
                    "pudl_fuel_cat": pudl_row.pudl_fuel_cat,
                    "state": pudl_row.state,
                    "plant_id_pudl": pudl_row.plant_id_pudl,
                    "balancing_authority_code_eia": pudl_row.balancing_authority_code_eia,
                    "match_method": "fuzzy",
                    "match_score": round(score, 4),
                    "name_score": round(name_score, 4),
                    "token_overlap_score": round(token_overlap, 4),
                    "token_fuzzy_score": round(token_fuzzy, 4),
                    "capacity_score": round(capacity, 4),
                }
            )

    fuzzy = pd.DataFrame(candidates)
    if len(fuzzy) > 0:
        fuzzy = fuzzy.sort_values(
            ["match_score", "capacity_score", "name_score", "wb_summer_mw"],
            ascending=[False, False, False, False],
        )
        taken_wb: set[str] = set()
        taken_pudl: set[str] = set()
        keep = []
        for row in fuzzy.itertuples(index=False):
            if row.plant_name in taken_wb or row.plant_name_eia in taken_pudl:
                continue
            keep.append(row._asdict())
            taken_wb.add(row.plant_name)
            taken_pudl.add(row.plant_name_eia)
        fuzzy = pd.DataFrame(keep)

    combined = pd.concat([exact, fuzzy], ignore_index=True, sort=False)
    combined = finalize_matches(combined)
    wb_unmatched_after = workbook_plants[
        ~workbook_plants["plant_name"].isin(set(combined["plant_name"]))
    ].copy()
    pudl_missing_after = pudl_plants[
        ~pudl_plants["plant_name_eia"].isin(set(combined["plant_name_eia"]))
    ].copy()
    return finalize_matches(exact), finalize_matches(fuzzy), combined, wb_unmatched_after, pudl_missing_after


def capacity_by_fuel(workbook_units: pd.DataFrame, pudl_generators: pd.DataFrame) -> pd.DataFrame:
    wb = workbook_units.groupby("fuel_category")["summer_cap_mw"].agg(["sum", "count"]).rename(
        columns={"sum": "wb_mw", "count": "wb_units"}
    )
    pudl = pudl_generators.groupby("fuel_cat_wb")["summer_capacity_mw"].agg(["sum", "count"]).rename(
        columns={"sum": "pudl_mw", "count": "pudl_units"}
    )
    out = wb.join(pudl, how="outer").fillna(0).reset_index().rename(columns={"index": "fuel_category"})
    out["diff_mw"] = out["wb_mw"] - out["pudl_mw"]
    out["diff_pct"] = (out["diff_mw"] / out["pudl_mw"].replace(0, np.nan) * 100).round(1)
    return out.sort_values("wb_mw", ascending=False)


def renewable_compare(workbook_units: pd.DataFrame, pudl_generators: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for resource in ["Wind", "Solar", "Hydro"]:
        wb_mw = workbook_units.loc[workbook_units["fuel_category"] == resource, "summer_cap_mw"].sum()
        pudl_mw = pudl_generators.loc[pudl_generators["fuel_cat_wb"] == resource, "summer_capacity_mw"].sum()
        rows.append(
            {
                "resource": resource,
                "workbook_mw": wb_mw,
                "pudl_mw": pudl_mw,
                "diff_mw": wb_mw - pudl_mw,
                "diff_pct": round((wb_mw - pudl_mw) / pudl_mw * 100, 1) if pudl_mw else np.nan,
            }
        )
    return pd.DataFrame(rows)


def solar_summary(
    workbook_units_dom: pd.DataFrame,
    workbook_plants_dom: pd.DataFrame,
    pudl_generators_dom: pd.DataFrame,
    combined_matches: pd.DataFrame,
    wb_unmatched_after: pd.DataFrame,
    pudl_missing_after: pd.DataFrame,
) -> pd.DataFrame:
    wb_solar_units = workbook_units_dom[workbook_units_dom["fuel_category"] == "Solar"].copy()
    wb_solar_plants = workbook_plants_dom[workbook_plants_dom["wb_fuel"] == "Solar"].copy()
    pudl_solar = pudl_generators_dom[pudl_generators_dom["fuel_cat_wb"] == "Solar"].copy()
    match_solar = combined_matches[combined_matches["wb_fuel"] == "Solar"].copy()
    wb_unmatched_solar = wb_unmatched_after[wb_unmatched_after["wb_fuel"] == "Solar"].copy()
    pudl_unmatched_solar = pudl_missing_after[pudl_missing_after["pudl_fuel_cat"] == "Solar"].copy()
    dg_mw = wb_solar_units.loc[wb_solar_units["unit_type"] == "Solar (DG)", "summer_cap_mw"].sum()
    rows = [
        ["Workbook DOM solar total", float(wb_solar_units["summer_cap_mw"].sum()), "MW", f"{len(wb_solar_plants):,} workbook plants"],
        ["Workbook DOM solar DG", float(dg_mw), "MW", f"{100 * dg_mw / max(wb_solar_units['summer_cap_mw'].sum(), 1):.1f}% of workbook solar"],
        ["PUDL PJM solar in MD/NC/VA proxy", float(pudl_solar["summer_capacity_mw"].sum()), "MW", f"{pudl_solar['plant_id_eia'].nunique():,} PUDL plants"],
        ["Matched solar workbook side", float(match_solar["wb_summer_mw"].sum()), "MW", f"{len(match_solar):,} matched workbook plants"],
        ["Matched solar PUDL side", float(match_solar["pudl_summer_mw"].sum()), "MW", f"exact {int((match_solar['match_method'] == 'exact').sum())}, fuzzy {int((match_solar['match_method'] == 'fuzzy').sum())}"],
        ["Workbook solar unmatched after fuzzy", float(wb_unmatched_solar["wb_summer_mw"].sum()), "MW", f"{len(wb_unmatched_solar):,} workbook solar plants"],
        ["PUDL solar unmatched after fuzzy", float(pudl_unmatched_solar["pudl_summer_mw"].sum()), "MW", f"{len(pudl_unmatched_solar):,} PUDL solar plants"],
    ]
    return pd.DataFrame(rows, columns=["metric", "value", "unit", "detail"])


def build_solar_alias_crosswalk(
    solar_matches: pd.DataFrame,
    solar_workbook_unmatched: pd.DataFrame,
) -> pd.DataFrame:
    candidates: list[dict[str, object]] = []
    if len(solar_matches) == 0 or len(solar_workbook_unmatched) == 0:
        return pd.DataFrame()

    candidate_pool = solar_matches.copy()
    candidate_pool["alias_key_pudl"] = candidate_pool["plant_name_eia"].apply(alias_key)
    candidate_pool["alias_key_workbook"] = candidate_pool["plant_name"].apply(alias_key)
    candidate_pool["alias_tokens_pudl"] = candidate_pool["plant_name_eia"].apply(alias_tokens)
    candidate_pool["alias_tokens_workbook"] = candidate_pool["plant_name"].apply(alias_tokens)

    for wb_row in solar_workbook_unmatched.itertuples(index=False):
        wb_key = alias_key(wb_row.plant_name)
        wb_tokens = alias_tokens(wb_row.plant_name)
        if not wb_key and not wb_tokens:
            continue

        pool = candidate_pool.copy()
        if wb_row.wb_state_guess:
            state_subset = pool[pool["state"] == wb_row.wb_state_guess]
            if len(state_subset) > 0:
                pool = state_subset

        for cand in pool.itertuples(index=False):
            key_score = max(
                seq_ratio(wb_key, cand.alias_key_pudl),
                seq_ratio(wb_key, cand.alias_key_workbook),
            )
            overlap = max(
                overlap_score(wb_tokens, cand.alias_tokens_pudl),
                overlap_score(wb_tokens, cand.alias_tokens_workbook),
            )
            capacity = max(
                cap_score(wb_row.wb_summer_mw, cand.pudl_summer_mw),
                cap_score(wb_row.wb_summer_mw, cand.wb_summer_mw),
            )
            contains = 1.0 if (
                wb_key
                and (
                    wb_key == cand.alias_key_pudl
                    or wb_key == cand.alias_key_workbook
                    or wb_key in cand.alias_key_pudl
                    or wb_key in cand.alias_key_workbook
                    or cand.alias_key_pudl in wb_key
                    or cand.alias_key_workbook in wb_key
                )
            ) else 0.0
            score = 0.5 * max(key_score, contains) + 0.25 * overlap + 0.25 * capacity
            if contains == 0 and score < 0.72:
                continue
            if contains == 0 and overlap < 0.4 and key_score < 0.75:
                continue

            candidates.append(
                {
                    "workbook_alias_name": wb_row.plant_name,
                    "workbook_alias_mw": wb_row.wb_summer_mw,
                    "workbook_alias_units": wb_row.wb_units,
                    "workbook_alias_state_guess": wb_row.wb_state_guess,
                    "alias_key": wb_key,
                    "canonical_workbook_name": cand.plant_name,
                    "canonical_workbook_mw": cand.wb_summer_mw,
                    "canonical_pudl_name": cand.plant_name_eia,
                    "canonical_pudl_mw": cand.pudl_summer_mw,
                    "canonical_state": cand.state,
                    "canonical_match_method": cand.match_method,
                    "canonical_match_score": cand.match_score,
                    "alias_score": round(score, 4),
                    "alias_key_score": round(key_score, 4),
                    "alias_overlap_score": round(overlap, 4),
                    "alias_capacity_score": round(capacity, 4),
                    "alias_contains": contains,
                    "crosswalk_basis": "alias_key" if contains == 1.0 else "fuzzy_alias",
                }
            )

    alias_candidates = pd.DataFrame(candidates)
    if len(alias_candidates) == 0:
        return alias_candidates

    alias_candidates = alias_candidates.sort_values(
        ["alias_score", "alias_capacity_score", "canonical_pudl_mw"],
        ascending=[False, False, False],
    )
    selected_rows = []
    for alias_name, group in alias_candidates.groupby("workbook_alias_name", sort=False):
        best = group.iloc[0]
        second_score = group.iloc[1]["alias_score"] if len(group) > 1 else -1.0
        if best["alias_contains"] == 0 and best["alias_score"] < 0.76:
            continue
        if best["alias_contains"] == 0 and (best["alias_score"] - second_score) < 0.03:
            continue
        selected_rows.append(best.to_dict())

    crosswalk = pd.DataFrame(selected_rows)
    if len(crosswalk) == 0:
        return crosswalk
    return crosswalk.sort_values(
        ["workbook_alias_mw", "alias_score"], ascending=[False, False]
    ).reset_index(drop=True)


def build_alias_adjusted_solar_comparison(
    solar_matches: pd.DataFrame,
    solar_alias_crosswalk: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if len(solar_matches) == 0:
        return pd.DataFrame()

    alias_lookup: dict[str, pd.DataFrame] = {}
    if len(solar_alias_crosswalk) > 0:
        alias_lookup = {
            name: group.copy()
            for name, group in solar_alias_crosswalk.groupby("canonical_pudl_name", sort=False)
        }

    for pudl_name, group in solar_matches.groupby("plant_name_eia", sort=False):
        aliases = alias_lookup.get(pudl_name, pd.DataFrame())
        workbook_names = group["plant_name"].tolist()
        workbook_mws = pd.to_numeric(group["wb_summer_mw"], errors="coerce").tolist()
        alias_names: list[str] = []
        alias_mws: list[float] = []
        if len(aliases) > 0:
            alias_names = aliases["workbook_alias_name"].tolist()
            alias_mws = pd.to_numeric(aliases["workbook_alias_mw"], errors="coerce").tolist()
        combined_names = workbook_names + alias_names
        combined_mws = workbook_mws + alias_mws
        if not combined_mws:
            continue
        max_idx = int(np.argmax(combined_mws))
        selected_name = combined_names[max_idx]
        selected_mw = float(combined_mws[max_idx])
        raw_sum = float(np.nansum(combined_mws))
        pudl_mw = float(pd.to_numeric(group["pudl_summer_mw"], errors="coerce").max())
        rows.append(
            {
                "canonical_pudl_name": pudl_name,
                "state": group["state"].iloc[0],
                "pudl_mw": pudl_mw,
                "matched_workbook_names": " | ".join(workbook_names),
                "matched_workbook_mw_sum": float(np.nansum(workbook_mws)),
                "alias_workbook_names": " | ".join(alias_names),
                "alias_workbook_mw_sum": float(np.nansum(alias_mws)) if alias_mws else 0.0,
                "workbook_group_raw_mw": raw_sum,
                "workbook_selected_name": selected_name,
                "workbook_selected_mw": selected_mw,
                "selected_source": "alias" if max_idx >= len(workbook_names) else "matched",
                "duplicate_overcount_mw": raw_sum - selected_mw,
                "diff_selected_mw": selected_mw - pudl_mw,
                "diff_selected_pct": round((selected_mw - pudl_mw) / pudl_mw * 100, 1) if pudl_mw else np.nan,
            }
        )

    comparison = pd.DataFrame(rows)
    if len(comparison) == 0:
        return comparison
    return comparison.sort_values(
        ["duplicate_overcount_mw", "workbook_group_raw_mw"], ascending=[False, False]
    ).reset_index(drop=True)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    manifest: list[dict[str, object]] = []

    comparison_years = read_csv("comparison_years.csv")
    workbook_units = read_csv("workbook_stack_active_units.csv")
    workbook_raw = read_csv("workbook_pjm_raw_data.csv")
    workbook_plants = read_csv("workbook_plants_aggregated.csv")
    pudl_plants_reference = read_csv("pudl_pjm_plants_reference.csv")
    pudl_generators_latest = read_csv("pudl_operating_generators_latest.csv")
    pudl_generators_hr = read_csv("pudl_operating_generators_heat_rate_year.csv")
    pudl_plants = read_csv("pudl_plants_aggregated_latest.csv")
    matched_exact_all = read_csv("plant_capacity_matches_exact_name.csv")
    workbook_unmatched_exact_all = read_csv("workbook_plants_unmatched_exact_name.csv")
    pudl_missing_exact_all = read_csv("pudl_plants_missing_from_workbook_all.csv")
    pudl_missing_exact_100_all = read_csv("pudl_plants_missing_from_workbook_ge_100mw.csv")
    heat_rate_compare = read_csv("heat_rate_compare_exact_name.csv")
    heat_rate_outliers = read_csv("heat_rate_outliers_gt_20pct.csv")
    retired_in_workbook = read_csv("retired_generators_found_in_workbook.csv")

    workbook_units["summer_cap_mw"] = pd.to_numeric(workbook_units["summer_cap_mw"], errors="coerce")
    workbook_units["name_clean"] = normalize_name(workbook_units["plant_name"])
    workbook_raw["name_clean"] = normalize_name(workbook_raw["plant_name"])
    workbook_plants["name_clean"] = normalize_name(workbook_plants["plant_name"])
    workbook_plants["wb_state_guess"] = workbook_plants["plant_name"].apply(extract_state)
    workbook_plants["fuzzy_tokens"] = workbook_plants["plant_name"].apply(fuzzy_tokens)
    workbook_plants["fuzzy_name"] = workbook_plants["plant_name"].apply(fuzzy_name)

    if "name_clean" not in pudl_generators_latest.columns:
        pudl_generators_latest["name_clean"] = normalize_name(pudl_generators_latest["plant_name_eia"])
    if "name_clean" not in pudl_generators_hr.columns:
        pudl_generators_hr["name_clean"] = normalize_name(pudl_generators_hr["plant_name_eia"])
    if "name_clean" not in pudl_plants.columns:
        pudl_plants["name_clean"] = normalize_name(pudl_plants["plant_name_eia"])

    pudl_generators_latest["summer_capacity_mw"] = pd.to_numeric(pudl_generators_latest["summer_capacity_mw"], errors="coerce")
    pudl_plants["fuzzy_tokens"] = pudl_plants["plant_name_eia"].apply(fuzzy_tokens)
    pudl_plants["fuzzy_name"] = pudl_plants["plant_name_eia"].apply(fuzzy_name)

    pudl_fuel_cat = (
        pudl_generators_latest.groupby("plant_id_eia")["fuel_cat_wb"]
        .agg(lambda s: s.mode().iat[0] if not s.mode().empty else s.iloc[0])
        .rename("pudl_fuel_cat")
        .reset_index()
    )
    pudl_plants = pudl_plants.merge(pudl_fuel_cat, on="plant_id_eia", how="left")
    pudl_plants["pudl_fuel_cat"] = pudl_plants["pudl_fuel_cat"].fillna("Other")

    workbook_units_dom = workbook_units[workbook_units["power_hub"] == DOMINION_HUB].copy()
    workbook_raw_dom = workbook_raw[
        (workbook_raw["zone"] == DOMINION_ZONE) | (workbook_raw["power_hub"] == DOMINION_HUB)
    ].copy()
    workbook_plants_dom = workbook_plants[workbook_plants["wb_hub"] == DOMINION_HUB].copy()
    pudl_generators_latest_dom = pudl_generators_latest[pudl_generators_latest["state"].isin(PROXY_STATES)].copy()
    pudl_generators_hr_dom = pudl_generators_hr[pudl_generators_hr["state"].isin(PROXY_STATES)].copy()
    pudl_plants_dom = pudl_plants[pudl_plants["state"].isin(PROXY_STATES)].copy()

    matched_exact_dom = matched_exact_all[matched_exact_all["wb_hub"] == DOMINION_HUB].copy()
    workbook_unmatched_exact_dom = workbook_unmatched_exact_all[
        workbook_unmatched_exact_all["wb_hub"] == DOMINION_HUB
    ].copy()
    pudl_missing_exact_dom = pudl_missing_exact_all[pudl_missing_exact_all["state"].isin(PROXY_STATES)].copy()
    pudl_missing_exact_100_dom = pudl_missing_exact_100_all[
        pudl_missing_exact_100_all["state"].isin(PROXY_STATES)
    ].copy()

    exact_dom, fuzzy_dom, combined_dom, wb_unmatched_after_dom, pudl_missing_after_dom = build_best_matches(
        workbook_plants_dom, pudl_plants_dom
    )
    pudl_missing_after_100_dom = pudl_missing_after_dom[
        pudl_missing_after_dom["pudl_summer_mw"] >= 100
    ].copy()

    solar_matches_dom = combined_dom[combined_dom["wb_fuel"] == "Solar"].copy()
    solar_wb_unmatched_dom = wb_unmatched_after_dom[wb_unmatched_after_dom["wb_fuel"] == "Solar"].copy()
    solar_pudl_missing_dom = pudl_missing_after_dom[pudl_missing_after_dom["pudl_fuel_cat"] == "Solar"].copy()
    solar_alias_crosswalk_dom = build_solar_alias_crosswalk(
        solar_matches_dom, solar_wb_unmatched_dom
    )
    solar_alias_adjusted_dom = build_alias_adjusted_solar_comparison(
        solar_matches_dom, solar_alias_crosswalk_dom
    )
    solar_alias_names = (
        set(solar_alias_crosswalk_dom["workbook_alias_name"].tolist())
        if len(solar_alias_crosswalk_dom) > 0
        else set()
    )
    solar_wb_unmatched_after_alias_dom = solar_wb_unmatched_dom[
        ~solar_wb_unmatched_dom["plant_name"].isin(solar_alias_names)
    ].copy()
    dominion_solar_summary = solar_summary(
        workbook_units_dom,
        workbook_plants_dom,
        pudl_generators_latest_dom,
        combined_dom,
        wb_unmatched_after_dom,
        pudl_missing_after_dom,
    )
    alias_duplicate_mw = float(
        solar_alias_adjusted_dom["duplicate_overcount_mw"].sum()
    ) if len(solar_alias_adjusted_dom) > 0 else 0.0
    workbook_solar_raw_total = float(
        workbook_units_dom.loc[workbook_units_dom["fuel_category"] == "Solar", "summer_cap_mw"].sum()
    )
    matched_adjusted_mw = float(
        solar_alias_adjusted_dom["workbook_selected_mw"].sum()
    ) if len(solar_alias_adjusted_dom) > 0 else float(solar_matches_dom["wb_summer_mw"].sum())
    dominion_solar_summary = pd.concat(
        [
            dominion_solar_summary,
            pd.DataFrame(
                [
                    {
                        "metric": "Workbook solar alias duplicate MW identified",
                        "value": alias_duplicate_mw,
                        "unit": "MW",
                        "detail": f"{len(solar_alias_crosswalk_dom):,} alias rows mapped to canonical projects",
                    },
                    {
                        "metric": "Workbook DOM solar alias-adjusted total",
                        "value": workbook_solar_raw_total - alias_duplicate_mw,
                        "unit": "MW",
                        "detail": "Raw workbook solar less duplicate alias overcount",
                    },
                    {
                        "metric": "Matched solar workbook side alias-adjusted",
                        "value": matched_adjusted_mw,
                        "unit": "MW",
                        "detail": "One-plant-per-project workbook selection after alias rollup",
                    },
                    {
                        "metric": "Workbook solar unmatched after alias crosswalk",
                        "value": float(solar_wb_unmatched_after_alias_dom["wb_summer_mw"].sum()),
                        "unit": "MW",
                        "detail": f"{len(solar_wb_unmatched_after_alias_dom):,} workbook solar plants",
                    },
                ]
            ),
        ],
        ignore_index=True,
    )

    heat_names = set(combined_dom["name_clean"])
    if len(heat_rate_compare) > 0 and "name_clean" not in heat_rate_compare.columns:
        heat_rate_compare["name_clean"] = normalize_name(heat_rate_compare["plant_name"])
    if len(heat_rate_outliers) > 0 and "name_clean" not in heat_rate_outliers.columns:
        heat_rate_outliers["name_clean"] = normalize_name(heat_rate_outliers["plant_name"])
    if len(retired_in_workbook) > 0 and "name_clean" not in retired_in_workbook.columns:
        retired_in_workbook["name_clean"] = normalize_name(retired_in_workbook["plant_name_eia"])
    heat_rate_compare_dom = heat_rate_compare[heat_rate_compare["name_clean"].isin(heat_names)].copy()
    heat_rate_outliers_dom = heat_rate_outliers[heat_rate_outliers["name_clean"].isin(heat_names)].copy()
    retired_in_workbook_dom = retired_in_workbook[
        retired_in_workbook["name_clean"].isin(set(workbook_plants_dom["name_clean"]))
    ].copy()

    dom_plant_ids = pd.to_numeric(pudl_generators_latest_dom["plant_id_eia"], errors="coerce").dropna()
    pudl_plants_reference_dom = pudl_plants_reference[
        pd.to_numeric(pudl_plants_reference["plant_id_eia"], errors="coerce").isin(dom_plant_ids)
    ].copy()

    cap_dom = capacity_by_fuel(workbook_units_dom, pudl_generators_latest_dom)
    renew_dom = renewable_compare(workbook_units_dom, pudl_generators_latest_dom)
    max_diff = float(cap_dom["diff_pct"].dropna().abs().max()) if len(cap_dom["diff_pct"].dropna()) else 0.0
    cap_status = "PASS" if max_diff < 10 else "WARN" if max_diff < 25 else "FAIL"

    validation = pd.DataFrame(
        [
            ["Subset Definition", "INFO", f"Workbook hub {DOMINION_HUB}; zone {DOMINION_ZONE}; proxy states {', '.join(PROXY_STATES)}"],
            ["PJM Plant Filter", "INFO", "Inherited from RTO export: iso_rto_code == PJM OR balancing_authority_code_eia == PJM"],
            ["Capacity Totals by Fuel", cap_status, f"Max difference: {max_diff:.1f}%"],
            ["Plant-Level Match", "WARN", f"Best available matches: {len(combined_dom):,} of {len(workbook_plants_dom):,} workbook plants (exact {len(exact_dom):,}, fuzzy {len(fuzzy_dom):,})"],
            ["DOM Solar", "INFO", f"Workbook solar raw {workbook_solar_raw_total:,.1f} MW; alias-adjusted {workbook_solar_raw_total - alias_duplicate_mw:,.1f} MW; PUDL proxy solar {float(pudl_generators_latest_dom.loc[pudl_generators_latest_dom['fuel_cat_wb'] == 'Solar', 'summer_capacity_mw'].sum()):,.1f} MW"],
            ["Retired Units in Workbook", "WARN" if len(retired_in_workbook_dom) else "PASS", f"{len(retired_in_workbook_dom):,} retired generators matched to Dominion workbook names"],
        ],
        columns=["check", "status", "detail"],
    )

    subset_definition = pd.DataFrame(
        [
            {
                "subset_label": "Dominion",
                "workbook_filter": f"power_hub = {DOMINION_HUB}; zone = {DOMINION_ZONE}",
                "pudl_filter": "Proxy states MD/NC/VA within the patched PJM RTO export",
                "fuzzy_match_rule": "Exact normalized name first, then one-to-one fuzzy matching using name tokens and capacity",
                "proxy_states": ",".join(PROXY_STATES),
            }
        ]
    )
    comparison_years["subset_label"] = "Dominion"
    comparison_years["pudl_proxy_states"] = ",".join(PROXY_STATES)

    save_csv(manifest, "subset_definition.csv", subset_definition, "Dominion subset and fuzzy match rule.")
    save_csv(manifest, "comparison_years.csv", comparison_years, "Reference years and filter metadata.")
    save_csv(manifest, "dominion_solar_summary.csv", dominion_solar_summary, "Dominion solar summary after patched PJM filter and fuzzy matching.")
    save_csv(manifest, "dominion_solar_alias_crosswalk.csv", solar_alias_crosswalk_dom, "Explicit Dominion solar alias crosswalk from unmatched workbook names to canonical matched PUDL projects.")
    save_csv(manifest, "dominion_solar_alias_adjusted_comparison.csv", solar_alias_adjusted_dom, "One-plant-per-project Dominion solar comparison after applying the alias crosswalk.")
    save_csv(manifest, "dominion_solar_matches.csv", solar_matches_dom, "Dominion solar matches after exact and fuzzy matching.")
    save_csv(manifest, "dominion_solar_workbook_unmatched_after_fuzzy.csv", solar_wb_unmatched_dom, "Dominion workbook solar plants still unmatched after fuzzy matching.")
    save_csv(manifest, "dominion_solar_workbook_unmatched_after_alias_crosswalk.csv", solar_wb_unmatched_after_alias_dom, "Dominion workbook solar plants still unmatched after the alias crosswalk is applied.")
    save_csv(manifest, "dominion_solar_pudl_missing_after_fuzzy.csv", solar_pudl_missing_dom, "Dominion PUDL solar plants still unmatched after fuzzy matching.")
    save_csv(manifest, "validation_summary_snapshot.csv", validation, "Dominion validation snapshot.")
    save_csv(manifest, "capacity_by_fuel_comparison.csv", cap_dom, "Dominion capacity comparison by fuel.")
    save_csv(manifest, "renewable_capacity_comparison.csv", renew_dom, "Dominion renewable comparison.")
    save_csv(manifest, "plant_capacity_matches_best_available.csv", combined_dom, "Dominion plant matches using exact then fuzzy matching.")
    save_csv(manifest, "plant_capacity_matches_exact_name.csv", exact_dom, "Dominion exact-name plant matches.")
    save_csv(manifest, "plant_capacity_matches_fuzzy_only.csv", fuzzy_dom, "Dominion fuzzy-only plant matches.")
    save_csv(manifest, "workbook_plants_unmatched_exact_name.csv", workbook_unmatched_exact_dom, "Dominion workbook plants unmatched under exact-name logic.")
    save_csv(manifest, "workbook_plants_unmatched_after_fuzzy.csv", wb_unmatched_after_dom.sort_values("wb_summer_mw", ascending=False), "Dominion workbook plants still unmatched after fuzzy matching.")
    save_csv(manifest, "pudl_plants_missing_from_workbook_all.csv", pudl_missing_exact_dom, "Dominion proxy-state PUDL plants missing under exact-name logic.")
    save_csv(manifest, "pudl_plants_missing_from_workbook_ge_100mw.csv", pudl_missing_exact_100_dom, "Dominion proxy-state PUDL plants >=100 MW missing under exact-name logic.")
    save_csv(manifest, "pudl_plants_missing_from_workbook_after_fuzzy_all.csv", pudl_missing_after_dom.sort_values("pudl_summer_mw", ascending=False), "Dominion proxy-state PUDL plants still unmatched after fuzzy matching.")
    save_csv(manifest, "pudl_plants_missing_from_workbook_after_fuzzy_ge_100mw.csv", pudl_missing_after_100_dom, "Dominion proxy-state PUDL plants >=100 MW still unmatched after fuzzy matching.")
    save_csv(manifest, "workbook_plants_aggregated.csv", workbook_plants_dom, "Dominion workbook plants aggregated.")
    save_csv(manifest, "workbook_stack_active_units.csv", workbook_units_dom, "Dominion workbook active units.")
    save_csv(manifest, "workbook_pjm_raw_data.csv", workbook_raw_dom, "Dominion workbook raw data.")
    save_csv(manifest, "pudl_operating_generators_latest.csv", pudl_generators_latest_dom, "Dominion proxy-state PUDL generators in latest year.")
    save_csv(manifest, "pudl_operating_generators_heat_rate_year.csv", pudl_generators_hr_dom, "Dominion proxy-state PUDL generators in heat-rate year.")
    save_csv(manifest, "pudl_plants_aggregated_latest.csv", pudl_plants_dom, "Dominion proxy-state PUDL plants aggregated.")
    save_csv(manifest, "heat_rate_compare_exact_name.csv", heat_rate_compare_dom, "Dominion heat-rate compare rows tied to matched names.")
    save_csv(manifest, "heat_rate_outliers_gt_20pct.csv", heat_rate_outliers_dom, "Dominion heat-rate outliers tied to matched names.")
    save_csv(manifest, "retired_generators_found_in_workbook.csv", retired_in_workbook_dom, "Retired PUDL generators whose names appear in Dominion workbook plants.")
    save_csv(manifest, "pudl_pjm_plants_reference.csv", pudl_plants_reference_dom, "PUDL plant reference rows tied to the Dominion subset.")

    manifest_df = pd.DataFrame(manifest).sort_values("file_name").reset_index(drop=True)
    save_csv(manifest, "csv_export_manifest.csv", manifest_df, "Manifest of Dominion CSV exports.")


if __name__ == "__main__":
    main()
