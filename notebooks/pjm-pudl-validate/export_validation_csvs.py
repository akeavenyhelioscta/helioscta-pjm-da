from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
OUT_DIR = BASE_DIR / "csvs" / "RTO"
WORKBOOK = (BASE_DIR / "../../.excel/PJM_Stack_Model_v1_2026_mar_10.xlsx").resolve()
PARQUET_PATH = "s3://pudl.catalyst.coop/nightly"


def read_pudl(table_name: str, columns: list[str] | None = None) -> pd.DataFrame:
    path = f"{PARQUET_PATH}/{table_name}.parquet"
    return pd.read_parquet(path, columns=columns, dtype_backend="pyarrow")


def normalize_name(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.lower()
        .str.replace(r"[^a-z0-9\s]", "", regex=True)
        .str.strip()
    )


def add_fuel_category(df: pd.DataFrame) -> pd.DataFrame:
    fuel_col = df["fuel_type_code_pudl"].astype(str).str.lower().str.strip()
    pm_col = df["prime_mover_code"].astype(str).str.upper().str.strip()
    cc_prime_movers = {"CA", "CS", "CT"}

    conditions = [
        fuel_col == "nuclear",
        fuel_col == "coal",
        (fuel_col == "gas") & (pm_col.isin(cc_prime_movers)),
        fuel_col == "gas",
        fuel_col == "oil",
        fuel_col == "solar",
        fuel_col == "wind",
        fuel_col == "hydro",
        fuel_col == "waste",
    ]
    choices = [
        "Nuclear",
        "Coal",
        "Gas CC",
        "Gas CT/ST",
        "Oil",
        "Solar",
        "Wind",
        "Hydro",
        "Biomass",
    ]

    enriched = df.copy()
    enriched["fuel_cat_wb"] = np.select(conditions, choices, default="Other")
    return enriched


def save_csv(
    manifest: list[dict[str, object]],
    file_name: str,
    df: pd.DataFrame,
    description: str,
) -> None:
    path = OUT_DIR / file_name
    df.to_csv(path, index=False)
    manifest.append(
        {
            "file_name": file_name,
            "rows": len(df),
            "columns": len(df.columns),
            "description": description,
        }
    )
    print(f"Wrote {file_name} ({len(df):,} rows)")


def build_pjm_reference(plants_eia: pd.DataFrame) -> pd.DataFrame:
    reference = plants_eia[
        (plants_eia["iso_rto_code"] == "PJM")
        | (plants_eia["balancing_authority_code_eia"] == "PJM")
    ].copy()

    iso_mask = (reference["iso_rto_code"] == "PJM").fillna(False).to_numpy()
    ba_mask = (reference["balancing_authority_code_eia"] == "PJM").fillna(False).to_numpy()
    reference["pjm_filter_basis"] = np.select(
        [
            iso_mask & ba_mask,
            iso_mask,
            ba_mask,
        ],
        ["iso_rto_and_ba", "iso_rto_only", "balancing_authority_only"],
        default="other",
    )

    reference = (
        reference.sort_values(
            ["plant_id_eia", "pjm_filter_basis", "report_date"],
            ascending=[True, True, False],
        )
        .drop_duplicates(subset=["plant_id_eia"], keep="first")
        .reset_index(drop=True)
    )
    return reference


def main() -> None:
    if not WORKBOOK.exists():
        raise FileNotFoundError(f"Workbook not found: {WORKBOOK}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    manifest: list[dict[str, object]] = []

    stack_raw = pd.read_excel(
        WORKBOOK,
        sheet_name="Stack Model",
        header=2,
        usecols="A:Q",
    )
    stack_raw = stack_raw.dropna(subset=["Plant Name"])
    stack_raw = stack_raw.rename(
        columns={
            stack_raw.columns[0]: "power_hub",
            stack_raw.columns[1]: "plant_name",
            stack_raw.columns[2]: "fuel_category",
            stack_raw.columns[3]: "unit_type",
            stack_raw.columns[4]: "summer_cap_mw",
            stack_raw.columns[5]: "min_load_mw",
            stack_raw.columns[6]: "heat_rate",
            stack_raw.columns[7]: "fuel_hub",
            stack_raw.columns[8]: "fuel_price",
            stack_raw.columns[9]: "fuel_cost_orig",
            stack_raw.columns[10]: "vom",
            stack_raw.columns[11]: "carbon_cost",
            stack_raw.columns[12]: "mc_orig",
            stack_raw.columns[13]: "cum_cap_hub",
            stack_raw.columns[14]: "must_run",
            stack_raw.columns[15]: "on_off",
            stack_raw.columns[16]: "cum_cap_system",
        }
    )

    workbook_units = stack_raw.copy()
    for column in [
        "heat_rate",
        "fuel_price",
        "vom",
        "carbon_cost",
        "summer_cap_mw",
        "min_load_mw",
    ]:
        workbook_units[column] = pd.to_numeric(workbook_units[column], errors="coerce")
    workbook_units["must_run_flag"] = (
        workbook_units["must_run"].astype(str).str.strip().str.lower() == "yes"
    )
    workbook_units["name_clean"] = normalize_name(workbook_units["plant_name"])

    workbook_active_units = workbook_units[workbook_units["on_off"] == 1].copy()

    raw_df = pd.read_excel(WORKBOOK, sheet_name="PJM Raw Data", header=1)
    raw_df = raw_df.dropna(subset=[raw_df.columns[0]])
    raw_df = raw_df.rename(
        columns={
            raw_df.columns[0]: "plant_name",
            raw_df.columns[1]: "fuel_category",
            raw_df.columns[2]: "unit_type",
            raw_df.columns[3]: "power_hub",
            raw_df.columns[4]: "zone",
            raw_df.columns[5]: "fuel_hub",
            raw_df.columns[6]: "summer_cap_mw",
            raw_df.columns[7]: "winter_cap_mw",
            raw_df.columns[8]: "cap_factor",
            raw_df.columns[9]: "heat_rate",
            raw_df.columns[10]: "vom",
            raw_df.columns[11]: "fom",
            raw_df.columns[12]: "min_load_factor",
            raw_df.columns[13]: "cold_start_hrs",
            raw_df.columns[14]: "so2_factor",
            raw_df.columns[15]: "on_off",
            raw_df.columns[16]: "baseload_mw",
            raw_df.columns[17]: "carbon_mkt",
            raw_df.columns[18]: "so2_mkt",
        }
    )
    raw_df["name_clean"] = normalize_name(raw_df["plant_name"])

    workbook_plants = (
        workbook_active_units.groupby("plant_name")
        .agg(
            wb_summer_mw=("summer_cap_mw", "sum"),
            wb_units=("summer_cap_mw", "count"),
            wb_fuel=("fuel_category", "first"),
            wb_hub=("power_hub", "first"),
        )
        .reset_index()
    )
    workbook_plants["name_clean"] = normalize_name(workbook_plants["plant_name"])

    plants_eia = read_pudl(
        "core_eia860__scd_plants",
        columns=[
            "plant_id_eia",
            "report_date",
            "iso_rto_code",
            "balancing_authority_code_eia",
        ],
    )
    pjm_plants_eia = build_pjm_reference(plants_eia)
    pjm_plant_ids = pjm_plants_eia["plant_id_eia"].unique()

    gens_all = read_pudl("out_eia__yearly_generators")
    gens_pjm = gens_all[gens_all["plant_id_eia"].isin(pjm_plant_ids)].copy()
    gens_pjm["report_date"] = pd.to_datetime(gens_pjm["report_date"])

    latest_year = gens_pjm["report_date"].max()
    latest_hr_year = gens_pjm.loc[
        gens_pjm["unit_heat_rate_mmbtu_per_mwh"].notna(), "report_date"
    ].max()

    gens_latest = gens_pjm[gens_pjm["report_date"] == latest_year].copy()
    gens_operating = add_fuel_category(
        gens_latest[gens_latest["operational_status"] == "existing"].copy()
    )
    gens_operating["name_clean"] = normalize_name(gens_operating["plant_name_eia"])

    gens_hr = gens_pjm[gens_pjm["report_date"] == latest_hr_year].copy()
    gens_hr_operating = add_fuel_category(
        gens_hr[gens_hr["operational_status"] == "existing"].copy()
    )
    gens_hr_operating["name_clean"] = normalize_name(gens_hr_operating["plant_name_eia"])

    pudl_plants = (
        gens_operating.groupby(["plant_id_eia", "plant_name_eia"])
        .agg(
            pudl_summer_mw=("summer_capacity_mw", "sum"),
            pudl_units=("generator_id", "count"),
            pudl_fuel=("fuel_type_code_pudl", "first"),
            state=("state", "first"),
            plant_id_pudl=("plant_id_pudl", "first"),
            balancing_authority_code_eia=("balancing_authority_code_eia", "first"),
        )
        .reset_index()
    )
    pudl_plants["name_clean"] = normalize_name(pudl_plants["plant_name_eia"])

    wb_cap = (
        workbook_active_units.groupby("fuel_category")["summer_cap_mw"]
        .agg(["sum", "count"])
        .rename(columns={"sum": "wb_mw", "count": "wb_units"})
    )
    pudl_cap = (
        gens_operating.groupby("fuel_cat_wb")["summer_capacity_mw"]
        .agg(["sum", "count"])
        .rename(columns={"sum": "pudl_mw", "count": "pudl_units"})
    )
    capacity_by_fuel = wb_cap.join(pudl_cap, how="outer").fillna(0).reset_index()
    capacity_by_fuel = capacity_by_fuel.rename(columns={"index": "fuel_category"})
    capacity_by_fuel["diff_mw"] = capacity_by_fuel["wb_mw"] - capacity_by_fuel["pudl_mw"]
    capacity_by_fuel["diff_pct"] = (
        capacity_by_fuel["diff_mw"]
        / capacity_by_fuel["pudl_mw"].replace(0, np.nan)
        * 100
    ).round(1)
    capacity_by_fuel = capacity_by_fuel.sort_values("wb_mw", ascending=False)

    matched_plants = workbook_plants.merge(
        pudl_plants,
        on="name_clean",
        how="inner",
        suffixes=("_wb", "_pudl"),
    )
    matched_plants["cap_diff_mw"] = (
        matched_plants["wb_summer_mw"] - matched_plants["pudl_summer_mw"]
    )
    matched_plants["cap_diff_pct"] = (
        matched_plants["cap_diff_mw"]
        / matched_plants["pudl_summer_mw"].replace(0, np.nan)
        * 100
    ).round(1)
    matched_plants = matched_plants.sort_values("cap_diff_mw", key=abs, ascending=False)

    workbook_unmatched = workbook_plants[
        ~workbook_plants["name_clean"].isin(matched_plants["name_clean"])
    ].sort_values("wb_summer_mw", ascending=False)

    wb_names = set(workbook_active_units["name_clean"])
    missing_from_workbook = gens_operating[
        ~gens_operating["name_clean"].isin(wb_names)
    ].copy()
    missing_plants = (
        missing_from_workbook.groupby(["plant_name_eia", "plant_id_eia"])
        .agg(
            total_mw=("summer_capacity_mw", "sum"),
            fuel=("fuel_type_code_pudl", "first"),
            state=("state", "first"),
            plant_id_pudl=("plant_id_pudl", "first"),
            balancing_authority_code_eia=("balancing_authority_code_eia", "first"),
        )
        .reset_index()
        .sort_values("total_mw", ascending=False)
    )
    missing_plants_100mw = missing_plants[missing_plants["total_mw"] >= 100].copy()

    thermal_fuels = ["Nuclear", "Coal", "Gas CC", "Gas CT/ST", "Oil"]
    workbook_thermal = workbook_active_units[
        workbook_active_units["fuel_category"].isin(thermal_fuels)
        & (workbook_active_units["heat_rate"] > 0)
    ].copy()
    pudl_thermal = gens_hr_operating[
        gens_hr_operating["fuel_cat_wb"].isin(thermal_fuels)
        & (gens_hr_operating["unit_heat_rate_mmbtu_per_mwh"].notna())
        & (gens_hr_operating["unit_heat_rate_mmbtu_per_mwh"] > 0)
    ].copy()

    workbook_hr = workbook_thermal[
        ["plant_name", "fuel_category", "summer_cap_mw", "heat_rate", "name_clean"]
    ].copy()
    pudl_hr = (
        pudl_thermal.assign(
            weighted_hr=lambda x: x["unit_heat_rate_mmbtu_per_mwh"]
            * x["summer_capacity_mw"]
        )
        .groupby("plant_name_eia")
        .agg(
            pudl_hr_wavg=("weighted_hr", "sum"),
            pudl_cap_total=("summer_capacity_mw", "sum"),
        )
        .reset_index()
    )
    pudl_hr["pudl_hr"] = pudl_hr["pudl_hr_wavg"] / pudl_hr["pudl_cap_total"].replace(
        0, np.nan
    )
    pudl_hr["name_clean"] = normalize_name(pudl_hr["plant_name_eia"])

    heat_rate_compare = workbook_hr.merge(
        pudl_hr[["plant_name_eia", "name_clean", "pudl_hr"]],
        on="name_clean",
        how="inner",
    )
    heat_rate_compare["hr_diff"] = heat_rate_compare["heat_rate"] - heat_rate_compare["pudl_hr"]
    heat_rate_compare["hr_diff_pct"] = (
        heat_rate_compare["hr_diff"]
        / heat_rate_compare["pudl_hr"].replace(0, np.nan)
        * 100
    ).round(1)
    heat_rate_outliers = heat_rate_compare[
        heat_rate_compare["hr_diff_pct"].abs() > 20
    ].copy()

    retired = gens_latest.copy()
    retired["retirement_dt"] = pd.to_datetime(
        retired["generator_retirement_date"], errors="coerce"
    )
    three_years_ago = latest_year - pd.DateOffset(years=3)
    retired = retired[
        (retired["operational_status"] != "existing")
        & retired["retirement_dt"].notna()
        & (retired["retirement_dt"] > three_years_ago)
    ].copy()
    retired["name_clean"] = normalize_name(retired["plant_name_eia"])
    retired_in_workbook = retired[retired["name_clean"].isin(wb_names)].copy()

    renewable_capacity = pd.DataFrame(
        {
            "resource": ["Wind", "Solar", "Hydro"],
            "workbook_mw": [
                workbook_active_units.loc[
                    workbook_active_units["fuel_category"] == "Wind", "summer_cap_mw"
                ].sum(),
                workbook_active_units.loc[
                    workbook_active_units["fuel_category"] == "Solar", "summer_cap_mw"
                ].sum(),
                workbook_active_units.loc[
                    workbook_active_units["fuel_category"] == "Hydro", "summer_cap_mw"
                ].sum(),
            ],
            "pudl_mw": [
                float(
                    gens_operating.loc[
                        gens_operating["fuel_cat_wb"] == "Wind", "summer_capacity_mw"
                    ].sum()
                ),
                float(
                    gens_operating.loc[
                        gens_operating["fuel_cat_wb"] == "Solar", "summer_capacity_mw"
                    ].sum()
                ),
                float(
                    gens_operating.loc[
                        gens_operating["fuel_cat_wb"] == "Hydro", "summer_capacity_mw"
                    ].sum()
                ),
            ],
        }
    )
    renewable_capacity["diff_mw"] = (
        renewable_capacity["workbook_mw"] - renewable_capacity["pudl_mw"]
    )
    renewable_capacity["diff_pct"] = (
        renewable_capacity["diff_mw"] / renewable_capacity["pudl_mw"] * 100
    ).round(1)

    validation_summary = pd.DataFrame(
        [
            {
                "check": "PJM Plant Filter",
                "status": "INFO",
                "detail": (
                    "Union filter used: iso_rto_code == PJM OR "
                    "balancing_authority_code_eia == PJM"
                ),
            },
            {
                "check": "Capacity Totals by Fuel",
                "status": "FAIL"
                if capacity_by_fuel["diff_pct"].dropna().abs().max() >= 25
                else "WARN"
                if capacity_by_fuel["diff_pct"].dropna().abs().max() >= 10
                else "PASS",
                "detail": (
                    f"Max difference: "
                    f"{float(capacity_by_fuel['diff_pct'].dropna().abs().max()):.1f}%"
                ),
            },
            {
                "check": "Plant-Level Capacity Match",
                "status": "WARN",
                "detail": (
                    f"Exact name matches: {len(matched_plants):,} of "
                    f"{len(workbook_plants):,} workbook plants"
                ),
            },
            {
                "check": "Heat Rate Comparison Year",
                "status": "INFO",
                "detail": (
                    f"Latest generator year {latest_year.date()}, "
                    f"heat rate year {latest_hr_year.date()}"
                ),
            },
            {
                "check": "Retired Units in Workbook",
                "status": "WARN" if len(retired_in_workbook) else "PASS",
                "detail": (
                    f"{len(retired_in_workbook):,} retired generators matched to "
                    "workbook names"
                ),
            },
        ]
    )

    comparison_years = pd.DataFrame(
        [
            {
                "latest_generator_year": latest_year.date().isoformat(),
                "latest_heat_rate_year": latest_hr_year.date().isoformat(),
                "workbook_file": str(WORKBOOK),
                "pudl_bucket": PARQUET_PATH,
                "pjm_filter_rule": (
                    "iso_rto_code == PJM OR balancing_authority_code_eia == PJM"
                ),
                "pjm_reference_plants": len(pjm_plants_eia),
                "pjm_reference_iso_only": int(
                    (pjm_plants_eia["pjm_filter_basis"] == "iso_rto_only").sum()
                ),
                "pjm_reference_ba_only": int(
                    (pjm_plants_eia["pjm_filter_basis"] == "balancing_authority_only").sum()
                ),
                "pjm_reference_iso_and_ba": int(
                    (pjm_plants_eia["pjm_filter_basis"] == "iso_rto_and_ba").sum()
                ),
            }
        ]
    )

    save_csv(
        manifest,
        "comparison_years.csv",
        comparison_years,
        "Reference years, PJM filter rule, and source locations used for the exports.",
    )
    save_csv(
        manifest,
        "workbook_stack_active_units.csv",
        workbook_active_units,
        "Workbook Stack Model rows with on_off = 1.",
    )
    save_csv(
        manifest,
        "workbook_pjm_raw_data.csv",
        raw_df,
        "Workbook PJM Raw Data sheet with normalized column names.",
    )
    save_csv(
        manifest,
        "workbook_plants_aggregated.csv",
        workbook_plants,
        "Workbook active units aggregated to plant name.",
    )
    save_csv(
        manifest,
        "pudl_pjm_plants_reference.csv",
        pjm_plants_eia,
        "PJM plant identifiers using the union PJM filter basis from core_eia860__scd_plants.",
    )
    save_csv(
        manifest,
        "pudl_operating_generators_latest.csv",
        gens_operating,
        "Latest-year operating PJM generators from PUDL using the union PJM plant filter.",
    )
    save_csv(
        manifest,
        "pudl_operating_generators_heat_rate_year.csv",
        gens_hr_operating,
        "Operating PJM generators from the latest year with populated heat rates using the union PJM plant filter.",
    )
    save_csv(
        manifest,
        "pudl_plants_aggregated_latest.csv",
        pudl_plants,
        "Latest-year operating PJM generators aggregated to plant name.",
    )
    save_csv(
        manifest,
        "capacity_by_fuel_comparison.csv",
        capacity_by_fuel,
        "Workbook vs PUDL summer capacity by workbook fuel category.",
    )
    save_csv(
        manifest,
        "renewable_capacity_comparison.csv",
        renewable_capacity,
        "Workbook vs PUDL renewable capacity totals.",
    )
    save_csv(
        manifest,
        "plant_capacity_matches_exact_name.csv",
        matched_plants,
        "Workbook and PUDL plants joined on normalized exact plant name.",
    )
    save_csv(
        manifest,
        "workbook_plants_unmatched_exact_name.csv",
        workbook_unmatched,
        "Workbook plants with no exact normalized name match in latest-year PUDL.",
    )
    save_csv(
        manifest,
        "pudl_plants_missing_from_workbook_all.csv",
        missing_plants,
        "PUDL operating plants whose normalized names are not in the workbook.",
    )
    save_csv(
        manifest,
        "pudl_plants_missing_from_workbook_ge_100mw.csv",
        missing_plants_100mw,
        "Subset of missing PUDL plants with at least 100 MW of summer capacity.",
    )
    save_csv(
        manifest,
        "heat_rate_compare_exact_name.csv",
        heat_rate_compare,
        "Workbook thermal units joined to PUDL plant heat rates on normalized exact plant name.",
    )
    save_csv(
        manifest,
        "heat_rate_outliers_gt_20pct.csv",
        heat_rate_outliers,
        "Heat rate comparisons where workbook and PUDL differ by more than 20%.",
    )
    save_csv(
        manifest,
        "retired_generators_found_in_workbook.csv",
        retired_in_workbook,
        "Recently retired PUDL generators whose plant names still appear in the workbook.",
    )
    save_csv(
        manifest,
        "validation_summary_snapshot.csv",
        validation_summary,
        "Compact summary of the main discrepancy checks.",
    )

    manifest_df = pd.DataFrame(manifest).sort_values("file_name").reset_index(drop=True)
    save_csv(
        manifest,
        "csv_export_manifest.csv",
        manifest_df,
        "Index of generated CSV exports.",
    )


if __name__ == "__main__":
    main()
