# PUDL Data Sources for PJM Stack Model Validation

> Assessment date: 2026-03-11
> Workbook: `.excel/PJM_Stack_Model_v1_2026_mar_10.xlsx`
> PUDL examples: `.repos/pudl-examples/`
> PUDL docs: https://catalystcoop-pudl.readthedocs.io/en/latest/data_dictionaries/pudl_db.html
> PUDL data viewer: https://data.catalyst.coop

---

## Executive Summary

The PJM Stack Model workbook is a 4,557-unit economic dispatch model across 17 PJM hubs. It draws on plausible capacity, fuel type, and heat rate assumptions. However, **there is a critical unit-conversion bug in the fuel cost formula** that renders all clearing prices ~1000x too low, and **carbon costs are hardcoded to zero** despite RGGI parameters being defined.

PUDL provides excellent publicly-available data sources for validating every major input -- capacity, heat rates, fuel costs, and operational status -- with the exception of generator-level VOM, which requires an external source like NREL ATB.

---

## Workbook Structure

| Sheet | Purpose | Key Details |
|---|---|---|
| **Forecast Inputs** | Hourly load, wind, solar by hub | 17 PJM zones; wind 11,122 MW; solar 15,523 MW installed |
| **Hourly Dispatch** | Clearing price model | MINIFS lookup into Stack Model; currently returns N/A due to formula bug |
| **Ramp Analysis** | Narrative dispatch walkthrough | Describes realistic prices ($18-45/MWh gas) that **don't match model output** |
| **Dispatch Dashboard** | Single-load-point hub dispatch | 130 GW test load, 70.5 GW must-run |
| **Assumptions** | Fuel prices, carbon, emissions factors | Gas $2.45-2.90/MMBtu; Coal $1.20-2.80; Oil $9.50-17.00; RGGI $15/ton |
| **Stack Model** | **Core:** 4,557 units sorted by MC | Summer cap, HR, fuel hub, fuel cost, VOM, MC, cumulative capacity |
| **Hub Summary** | Capacity by fuel by hub | Total PJM: 217,990 MW |
| **Price Sensitivity** | Gas price sensitivity tables | Shows negligible impact (consequence of the bug) |
| **PJM Raw Data** | Source unit-level data | Includes winter cap, cap factor, FOM, min load factor, cold start hrs, SO2, carbon market flags |

### Aggregate Capacity by Fuel Type

| Fuel | Units | Total MW | Must-Run | Avg HR | MC Range ($/MWh) |
|---|---|---|---|---|---|
| Gas CC | 313 | 59,512 | 11 | 8.48 | $1.50 - $4.42 |
| Coal | 142 | 46,240 | 17 | 11.32 | $2.57 - $6.30 |
| Gas CT/ST | 685 | 36,826 | 153 | 12.10 | $2.85 - $7.63 |
| Nuclear | 31 | 32,672 | 31 | 9.57 | $1.55 - $1.95 |
| Solar | 1,619 | 15,523 | 1,619 | -- | $1.00 - $1.12 |
| Wind | 146 | 11,122 | 146 | -- | $2.50 - $5.00 |
| Hydro | 308 | 8,375 | 308 | -- | $1.12 - $10.42 |
| Oil | 576 | 4,920 | 235 | 13.14 | $3.50 - $7.35 |
| Biomass | 652 | 2,004 | 652 | 13.28 | $2.00 - $9.25 |
| Storage | 80 | 686 | 80 | -- | $5.00 - $5.40 |
| Other | 5 | 113 | 5 | 5.32 | $3.50 - $6.00 |

---

## Critical Bug: Fuel Cost Formula

Stack Model column J formula:

```
=IF(G>0, I*G/1000, 0)
```

Where G = Heat Rate (labeled BTU/kWh) and I = Fuel Price ($/MMBtu). The values in column G (e.g. 8.79, 11.32) are clearly in **MMBtu/MWh**, not BTU/kWh (which would be 8,790 and 11,320). The `/1000` makes all fuel costs ~1000x too low:

- Gas CC example: $2.90 x 8.79 / 1000 = **$0.025/MWh** (wrong) vs $2.90 x 8.79 = **$25.49/MWh** (correct)
- Coal example: $1.20 x 11.32 / 1000 = **$0.014/MWh** (wrong) vs $1.20 x 11.32 = **$13.58/MWh** (correct)

**Fix:** Either change formula to `=IF(G>0, I*G, 0)` or multiply all heat rate values by 1000 to convert to BTU/kWh.

### Carbon Costs Hardcoded to Zero

Column L = `0` for every row, despite RGGI price ($15/ton) and emissions factors (gas=0.053, coal=0.097, oil=0.075 tons CO2/MMBtu) being defined in Assumptions. PJM Raw Data column R flags RGGI-participating units.

**Fix:** `=IF(Carbon_Mkt="RGGI", RGGI_Price * Emissions_Factor * HR, 0)`

---

## PUDL Data Sources for Validation

### How to Access PUDL Data

All data is distributed as Apache Parquet files from AWS S3:

```python
import pandas as pd

PARQUET_PATH = "s3://pudl.catalyst.coop/nightly"

def read_parquet(table_name: str) -> pd.DataFrame:
    return pd.read_parquet(
        f"{PARQUET_PATH}/{table_name}.parquet",
        dtype_backend="pyarrow"
    )
```

### Identifying PJM Plants

```python
plants = read_parquet("core_eia860__scd_plants")
pjm_plant_ids = plants.loc[plants.iso_rto_code == "PJM", "plant_id_eia"].unique()
```

Key table: `core_eia860__scd_plants` -- filter by `iso_rto_code = 'PJM'` or `balancing_authority_code_eia = 'PJM'`. Join other tables on `plant_id_eia`.

---

### Recommended Data Sources Table

| Validation Purpose | Data Source | PUDL Table | Key Fields | Why Useful | Limitations |
|---|---|---|---|---|---|
| **Generator capacity** | EIA 860+923 | `out_eia__yearly_generators` | `summer_capacity_mw`, `winter_capacity_mw`, `capacity_mw`, `minimum_load_mw` | Primary source for all 4,557 unit capacities. Filterable by BA. | May differ from PJM-tested ICAP values. |
| **Fuel type & prime mover** | EIA 860 | `out_eia__yearly_generators` | `energy_source_code_1`, `fuel_type_code_pudl`, `prime_mover_code`, `technology_description` | Confirm fuel category and unit type assignments. | Multi-fuel plants may shift primary fuel seasonally. |
| **Operational status** | EIA 860 | `out_eia__yearly_generators` | `operational_status`, `generator_retirement_date`, `planned_generator_retirement_date` | Verify model doesn't include retired units or miss new capacity. | Annual reporting lag. |
| **Heat rates (annual)** | EIA 860+923 | `out_eia__yearly_generators` | `unit_heat_rate_mmbtu_per_mwh` | Cross-check workbook heat rates. | Implied from fuel/gen; noisy for low-CF units. |
| **Heat rates (monthly)** | EIA 860+923 | `out_eia__monthly_generators` | `unit_heat_rate_mmbtu_per_mwh`, `net_generation_mwh`, `capacity_factor` | Monthly granularity on HR and CF. | Same caveats as annual. |
| **Heat rates (hourly)** | EPA CEMS | `core_epacems__hourly_emissions` | `heat_content_mmbtu`, `gross_load_mw`, `operating_time_hours` | Highest-resolution HR data. Compute `heat_content / (load * time)`. | Gross gen only; fossil >25 MW only. Link via `core_epa__assn_eia_epacamd`. |
| **Fuel delivery costs** | EIA 923 Sch.2 | `out_eia923__fuel_receipts_costs` | `fuel_cost_per_mmbtu`, `energy_source_code`, `supplier_name`, `sulfur_content_pct` | Validate gas/coal price assumptions against actual deliveries. | ~1/3 gas prices redacted (2009-2021). Delivery-level, not hub-level. |
| **Non-fuel O&M** | FERC Form 1 | `out_ferc1__yearly_steam_plants_sched402` | `opex_nonfuel_per_mwh`, `opex_total_nonfuel`, `capacity_mw` | Only public source for non-fuel operating expenses. | Plant-level only; mix of fixed + variable costs. |
| **PJM hourly gen by fuel** | EIA 930 | `core_eia930__hourly_net_generation_by_energy_source` | `generation_energy_source`, `net_generation_mwh` | Validate dispatch outputs against actual PJM fuel mix. | BA-level aggregate only. |
| **Renewable capacity factors** | VCE RARE | `out_vcerare__hourly_available_capacity_factor` | `capacity_factor_solar_pv`, `capacity_factor_onshore_wind`, `county_id_fips` | Validate wind/solar CF assumptions (row 6 of Forecast Inputs). | Modeled (HRRR), not metered. 136M rows; filter by PJM counties. |
| **Ownership shares** | EIA 860 Sch.4 | `out_eia860__yearly_ownership` | `fraction_owned`, `owner_utility_id_eia` | Pro-rate jointly-owned capacity (esp. nuclear). | Only covers jointly/third-party owned generators. |
| **Bilateral prices** | FERC EQR | `core_ferceqr__transactions` | `price`, `product_name`, `point_of_delivery_balancing_authority` | Validate clearing prices against actual PJM bilateral market. | 83 GB table; bilateral only (not pool/spot). |
| **Hourly BA demand** | EIA 930 | `core_eia930__hourly_operations` | `demand_reported_mwh`, `demand_forecast_mwh`, `net_generation_mwh` | Validate load forecast inputs. | BA-level only; no zone breakdown. |

---

## Workbook Input to PUDL Mapping

| Workbook Input (Sheet -> Column) | PUDL Validation Table | Match Strategy |
|---|---|---|
| Summer Cap MW (Stack Model -> E) | `out_eia__yearly_generators` -> `summer_capacity_mw` | Join on plant name + generator ID; compare MW. |
| Min Load MW (Stack Model -> F) | `out_eia__yearly_generators` -> `minimum_load_mw` | Direct field; compare against formula `=E*factor`. |
| Heat Rate (Stack Model -> G) | `out_eia__yearly_generators` -> `unit_heat_rate_mmbtu_per_mwh` | **Reconcile units first** -- PUDL reports MMBtu/MWh; workbook header says BTU/kWh but values are MMBtu/MWh. |
| Fuel Price $/MMBtu (Assumptions) | `out_eia923__fuel_receipts_costs` -> `fuel_cost_per_mmbtu` | Aggregate recent deliveries by fuel to PJM plants; compare to hub prices. |
| VOM $/MWh (Stack Model -> K) | `out_ferc1__yearly_steam_plants_sched402` -> `opex_nonfuel_per_mwh` | Plant-level proxy only. **Supplement with NREL ATB** for technology defaults. |
| Fuel Category (Stack Model -> C) | `out_eia__yearly_generators` -> `fuel_type_code_pudl`, `prime_mover_code` | Confirm "Gas CC" = prime mover CA/CT+CS; "Coal" = bituminous vs sub-bit. |
| Must-Run (Stack Model -> O) | No PUDL equivalent | **Gap.** Use PJM capacity market results or operational experience. |
| On/Off (Stack Model -> P) | `out_eia__yearly_generators` -> `operational_status` | Confirm "Off" units are retired/mothballed in EIA. |
| Wind/Solar MW (Forecast Inputs -> row 5) | `out_eia__yearly_generators` by `fuel_type_code_pudl` + PJM | Sum capacity by fuel and hub; compare totals. |
| Wind/Solar CF (Forecast Inputs -> row 6) | `out_vcerare__hourly_available_capacity_factor` | Compute average CF by PJM county; compare to hub defaults. |

---

## Recommended Validation Approach

### Step 1: Fix the Unit Conversion Bug

Change Stack Model column J from `=IF(G>0, I*G/1000, 0)` to `=IF(G>0, I*G, 0)` (if heat rates are MMBtu/MWh), OR multiply all heat rate values by 1000. Verify: Gas CC with HR ~8.8 and gas at $2.90 should yield ~$25.50/MWh.

### Step 2: Wire Carbon Costs

Replace `=0` in column L with formula using RGGI price, emissions factor, and heat rate from Assumptions and PJM Raw Data column R.

### Step 3: Pull PUDL Generator Data for PJM

```python
import pandas as pd
PARQUET_PATH = "s3://pudl.catalyst.coop/nightly"

plants = pd.read_parquet(f"{PARQUET_PATH}/core_eia860__scd_plants.parquet")
pjm_plant_ids = plants.loc[plants.iso_rto_code == "PJM", "plant_id_eia"].unique()

gens = pd.read_parquet(f"{PARQUET_PATH}/out_eia__yearly_generators.parquet")
pjm_gens = gens[gens.plant_id_eia.isin(pjm_plant_ids)]
pjm_gens_latest = pjm_gens[pjm_gens.report_date == pjm_gens.report_date.max()]
```

### Step 4: Validate Capacity Totals

Compare Hub Summary totals (217,990 MW total; 32,672 nuclear; 46,240 coal; 59,513 Gas CC) against:

```python
pjm_gens_latest.groupby('fuel_type_code_pudl')['summer_capacity_mw'].sum()
```

Investigate discrepancies >5%.

### Step 5: Validate Heat Rates

Merge workbook data with PUDL on plant name + generator ID. Compare column G against `unit_heat_rate_mmbtu_per_mwh`. Flag units where difference >10%.

For deeper validation, use EPA CEMS:

```python
import duckdb

sql = """
SELECT plant_id_eia, emissions_unit_id_epa,
       AVG(heat_content_mmbtu / NULLIF(gross_load_mw * operating_time_hours, 0)) as avg_heat_rate
FROM 's3://pudl.catalyst.coop/nightly/core_epacems__hourly_emissions.parquet'
WHERE plant_id_eia IN (SELECT UNNEST(?))
  AND year = 2024
  AND operating_time_hours > 0
GROUP BY 1, 2
"""
```

### Step 6: Validate Fuel Cost Assumptions

```python
fuel_costs = pd.read_parquet(f"{PARQUET_PATH}/out_eia923__fuel_receipts_costs.parquet")
pjm_fuel = fuel_costs[fuel_costs.plant_id_eia.isin(pjm_plant_ids)]
recent = pjm_fuel[pjm_fuel.report_date >= "2025-01-01"]
recent.groupby('fuel_type_code_pudl')['fuel_cost_per_mmbtu'].describe()
```

Compare median delivered costs to Assumptions sheet. Map plant locations to gas pricing hubs for differential analysis.

### Step 7: Validate VOM Assumptions

```python
ferc1 = pd.read_parquet(f"{PARQUET_PATH}/out_ferc1__yearly_steam_plants_sched402.parquet")
# Join via plant_id_pudl to EIA data
```

Cross-reference against NREL ATB defaults by technology. Flag entries outside 10th-90th percentile.

### Step 8: Validate Operational Status

Check all On/Off=1 units have `operational_status == 'existing'` in PUDL. Flag retired/planned units still in model and new PJM generators missing from workbook.

### Step 9: End-to-End Dispatch Validation

After fixing fuel cost bug, run the model for a historical day. Compare hourly generation by fuel type against:

```python
eia930 = pd.read_parquet(f"{PARQUET_PATH}/core_eia930__hourly_net_generation_by_energy_source.parquet")
pjm_gen = eia930[eia930.balancing_authority_code_eia == "PJM"]
```

---

## Key Gaps Where PUDL Is Insufficient

| Gap | What's Missing | Recommended Supplement |
|---|---|---|
| **Generator-level VOM** | PUDL has only plant-level FERC O&M; EIA doesn't collect VOM | NREL Annual Technology Baseline (ATB) by technology class |
| **Must-run status** | No EIA/FERC field for must-run designation | PJM capacity market auction results; PJM GATS |
| **PJM hub/zone assignments** | PUDL has BA-level only, not PJM pricing zones | PJM's public generator list or eGADS; manual mapping |
| **Gas pipeline hub pricing** | PUDL fuel costs are delivered, not hub quotes | ICE/CME settlement data; SNL/Platts gas hub indices |
| **Planned outages & derates** | Not in PUDL | PJM eDATA system (OutSchedule); NERC GADS |
| **Energy storage dispatch** | Limited storage data in PUDL | PJM real-time/DA market data for storage bids |
| **Minimum load constraints** | PUDL has `minimum_load_mw` but not economic min-load behavior | PJM Operating Agreement Attachment K; unit parameter sheets |

---

## PUDL Example Notebooks Reference

Located at `.repos/pudl-examples/`:

| Notebook | Relevance to Stack Model |
|---|---|
| `01-pudl-data-access.ipynb` | **High.** Shows how to read all PUDL tables, query EIA 860/923 generators, FERC Form 1 steam plants, and EPA CEMS hourly data. Demonstrates the `read_parquet()` helper and DuckDB for large datasets. |
| `02-state-hourly-electricity-demand.ipynb` | Medium. EIA 930 hourly demand by state. Useful for load forecast validation. |
| `03-eia930-sanity-checks.ipynb` | Medium. EIA 930 data quality. Relevant for dispatch output validation. |
| `04-renewable-generation-profiles.ipynb` | **High.** VCE RARE hourly county-level solar/wind capacity factors. Directly applicable to validating Forecast Inputs wind/solar CF assumptions. |
| `05-ferc714-electricity-demand-forecast-biases.ipynb` | Low. FERC 714 demand forecast biases. Tangential. |
| `06-pudl-imputed-demand.ipynb` | Low. Imputed demand methodology. |
| `07-ferceqr-access.ipynb` | Medium. FERC EQR bilateral transactions. Shows how to query PJM-filtered transaction prices -- useful for clearing price validation after bug fix. |

---

## Key PUDL Table Quick Reference

| Table | Source | Resolution | Primary Use |
|---|---|---|---|
| `out_eia__yearly_generators` | EIA 860+923 | Annual | **Primary.** Capacity, fuel type, HR, fuel cost, status. |
| `out_eia__monthly_generators` | EIA 860+923 | Monthly | Monthly HR, generation, capacity factor. |
| `core_eia860__scd_plants` | EIA 860 | Annual | **PJM filter table.** `iso_rto_code = 'PJM'`. |
| `out_eia923__fuel_receipts_costs` | EIA 923 Sch.2 | Monthly | Fuel delivery costs by plant. |
| `core_eia923__monthly_generation_fuel` | EIA 923 | Monthly | Generation + fuel consumption by plant/fuel/prime mover. |
| `out_ferc1__yearly_steam_plants_sched402` | FERC Form 1 | Annual | Non-fuel O&M costs; plant-level financials. |
| `core_epacems__hourly_emissions` | EPA CEMS | Hourly | Hourly generation, heat content, emissions. Fossil >25 MW. |
| `core_epa__assn_eia_epacamd` | EPA-EIA crosswalk | Annual | Links EPA CEMS units to EIA generators. |
| `core_eia860__assn_boiler_generator` | EIA 860 | Annual | Boiler-to-generator mapping. |
| `core_eia930__hourly_net_generation_by_energy_source` | EIA 930 | Hourly | PJM aggregate generation by fuel type. |
| `core_eia930__hourly_operations` | EIA 930 | Hourly | PJM aggregate demand, generation, interchange. |
| `out_vcerare__hourly_available_capacity_factor` | VCE RARE | Hourly | County-level wind/solar CFs. 136M rows. |
| `out_eia860__yearly_ownership` | EIA 860 Sch.4 | Annual | Ownership fractions for joint-owned generators. |
| `core_ferceqr__transactions` | FERC EQR | Quarterly | Bilateral transaction prices. 83 GB. |
| `core_pudl__assn_eia_pudl_plants` | PUDL | -- | EIA-to-PUDL plant ID mapping (for FERC joins). |
