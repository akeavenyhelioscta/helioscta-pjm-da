# Stack Model Implementation Plan — v1 (2026-03-03)

## Context

Migrate the PJM Stack Model from the Excel workbook (`.SKILLS/stack-model/reference/PJM_Stack_Model_v3.xlsx`) into the existing Next.js 15 + FastAPI backend architecture. The Excel workbook contains 9 sheets that model economic dispatch across ~4,600 PJM generator units, producing hourly clearing prices based on a merit-order supply curve.

---

## 1. Excel Workbook Summary

### Sheet Inventory

| # | Sheet | Rows | Purpose |
|---|-------|------|---------|
| 1 | **Forecast Inputs** | 39 | Hourly load/wind/solar forecasts by 17 PJM hubs → computes Net Load |
| 2 | **Hourly Dispatch** | 33 | 24-hour clearing price calculation via merit-order lookup |
| 3 | **Ramp Analysis** | 49 | Morning ramp-up and evening peak dispatch transitions |
| 4 | **Dispatch Dashboard** | 54 | Executive KPIs: peak load/price, marginal fuel, reserve margin |
| 5 | **Assumptions** | 40 | Fuel prices by hub, emissions costs (RGGI, CSAPR), must-run flags |
| 6 | **Stack Model** | 4,614 | **CORE**: Unit-level generator data ranked by marginal cost |
| 7 | **Hub Summary** | 20 | Aggregated dispatch metrics by power hub |
| 8 | **Price Sensitivity** | 52 | Clearing price vs load/gas price scenario matrix |
| 9 | **PJM Raw Data** | 4,596 | Generator fleet reference data (same domain as Stack Model) |

### Key Columns — Stack Model Sheet

| Column | Description |
|--------|-------------|
| Plant Name | Generator name and location |
| Fuel Category | Nuclear, Coal, Gas CC, Gas CT/ST, Wind, Solar, Hydro, Oil, Other |
| Unit Type | Pulverized Coal, Combined Cycle, Combustion Turbine, Steam Turbine, etc. |
| Fuel Hub | Gas delivery point (Columbia TCO, Transco Z6, etc.) — maps to Assumptions |
| Must Run | Boolean flag (nuclear and some coal = yes) |
| On/Off | User toggle to include/exclude from dispatch |
| Nameplate Cap (MW) | Full capacity |
| Min Stable Load (%) | Minimum operating point as % of nameplate |
| Heat Rate (BTU/kWh) | Fuel efficiency (gas CC ~7,000; gas CT ~11,000; coal ~10,000) |
| Fuel Price ($/MMBtu) | **Formula**: looked up from Assumptions by fuel hub |
| Marginal Cost ($/MWh) | **Formula**: `(Fuel Price × Heat Rate / 1000) + Variable O&M` |
| Variable O&M ($/MWh) | Operating & maintenance cost |
| CO2 Emissions (tons/MWh) | Emissions factor × fuel × heat rate |
| Emissions Cost ($/MWh) | `CO2 Emissions × RGGI Price` |
| Effective MC ($/MWh) | `Marginal Cost + Emissions Cost` — **used for dispatch ranking** |
| Cumulative Capacity (MW) | Running sum for merit-order graphing |
| Dispatch Flag | On margin / inframarginal / above load |

### Key Columns — Assumptions Sheet

- **Gas Prices** ($/MMBtu): Columbia TCO Pool, Dominion South Pt, Tetco M2/M3, Transco Leidy/Z5/Z6, Big Sandy, Central App, Northern App, PRB Coal, Uinta Basin, Gulf Coast Distillate, NY Distillate, NY Jet Fuel
- **Nuclear**: Uranium 308 price
- **Emissions**: RGGI ($/ton CO2), CSAPR SO2 ($/ton)
- **Must-Run Flags**: Nuclear, Hydro, Wind, Solar, Biomass
- **Emissions Factors**: Gas (~0.117), Coal (~0.095), Oil (~0.102) tons CO2/MMBtu

### Data Flow

```
┌─────────────────────────────────────────────┐
│              USER INPUTS                    │
│         (Forecast Inputs Sheet)             │
│   Hourly Load, Wind, Solar by Hub           │
│   → Net Load = Load − Wind − Solar          │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│       DISPATCH ENGINE (Stack Model)         │
│  ~4,600 units sorted by Effective MC        │
│  Fuel prices ← Assumptions Sheet            │
│  Commits cheapest-first until cap ≥ Load    │
│  Marginal unit → Clearing Price             │
└──────────────────┬──────────────────────────┘
                   │
       ┌───────────┼───────────┐
       ▼           ▼           ▼
  Hourly       Hub         Price
  Dispatch     Summary     Sensitivity
       │
       ├──→ Ramp Analysis
       └──→ Dispatch Dashboard
```

### Sheet → Implementation Mapping

| Excel Sheet | Maps To |
|---|---|
| Forecast Inputs | Frontend: `StackModelInputs.tsx` + Meteologica API |
| Assumptions | Frontend: inline editor in Inputs + DB table |
| Stack Model + PJM Raw Data | Database: `stack_model.pjm_generator_units` |
| Hourly Dispatch | Backend: dispatch engine + Dashboard component |
| Dispatch Dashboard + Hub Summary | Frontend: `StackModelDashboard.tsx` |
| Ramp Analysis | Backend: ramp endpoint + Sensitivity component |
| Price Sensitivity | Backend: sensitivity endpoint + Sensitivity component |

---

## 2. Database Design

### Schema: `stack_model`

Dedicated schema for manually-maintained reference data (separate from the dbt-managed `dbt_pjm_v1_*` schemas).

### Table: `stack_model.pjm_generator_units`

Stores ~4,600 generator records from the "Stack Model" and "PJM Raw Data" sheets.

```sql
CREATE SCHEMA IF NOT EXISTS stack_model;

CREATE TABLE stack_model.pjm_generator_units (
    unit_id             SERIAL PRIMARY KEY,
    plant_name          VARCHAR(255) NOT NULL,
    fuel_category       VARCHAR(50)  NOT NULL,
    unit_type           VARCHAR(100),
    fuel_hub            VARCHAR(100),
    must_run            BOOLEAN DEFAULT FALSE,
    on_off              BOOLEAN DEFAULT TRUE,
    nameplate_cap_mw    NUMERIC(10,2) NOT NULL,
    min_stable_load_pct NUMERIC(5,2) DEFAULT 0,
    heat_rate_btu_kwh   NUMERIC(10,2),
    variable_om_mwh     NUMERIC(10,2) DEFAULT 0,
    co2_emissions_tons  NUMERIC(8,4) DEFAULT 0,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_gen_units_fuel ON stack_model.pjm_generator_units (fuel_category);
CREATE INDEX idx_gen_units_mc   ON stack_model.pjm_generator_units (fuel_category, heat_rate_btu_kwh);
```

**Design note:** Fuel Price, Marginal Cost, Emissions Cost, and Effective MC are **not stored** — they are computed at dispatch time by joining with the current fuel price assumptions. This lets users change assumptions without re-uploading unit data.

### Table: `stack_model.fuel_price_assumptions`

```sql
CREATE TABLE stack_model.fuel_price_assumptions (
    assumption_id       SERIAL PRIMARY KEY,
    assumption_set_name VARCHAR(100) NOT NULL DEFAULT 'default',
    fuel_hub            VARCHAR(100) NOT NULL,
    fuel_price_mmbtu    NUMERIC(10,4) NOT NULL,
    rggi_cost_ton       NUMERIC(10,4) DEFAULT 0,
    csapr_cost_ton      NUMERIC(10,4) DEFAULT 0,
    co2_cost_ton        NUMERIC(10,4) DEFAULT 0,
    effective_date      DATE NOT NULL,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (assumption_set_name, fuel_hub, effective_date)
);
```

### Seed Data

One-time loader script reads both Excel sheets and upserts into `pjm_generator_units` using the existing `upsert_to_azure_postgresql()` utility from `backend/src/utils/azure_postgresql.py`. Default fuel prices seeded from the Assumptions sheet.

---

## 3. Meteologica Regional Forecasts (External Dependency)

### Available Data

73 tables in the `meteologica` schema on the existing Azure PostgreSQL instance:

| Category | Key Tables | Measure Column |
|----------|-----------|----------------|
| **Regional Demand** | `usa_pjm_power_demand_forecast_hourly` (RTO), `usa_pjm_midatlantic_*`, `usa_pjm_south_*`, `usa_pjm_west_*` + 50 utility-level tables | `forecast_mw` |
| **DA Pricing** | `usa_pjm_da_power_price_system_forecast_hourly`, 12 hub-level tables (western, eastern, etc.) | `day_ahead_price` |
| **Generation** | `usa_pjm_pv_power_generation_forecast_hourly`, `usa_pjm_wind_*`, `usa_pjm_hydro_*` + regional variants | `forecast_mw` |
| **Observations** | `usa_pjm_power_demand_observation`, `usa_pjm_pv_power_generation_observation`, `usa_pjm_wind_power_generation_observation` | varies |

### Table Schema (consistent across all 73 tables)

| Column | Type | Notes |
|--------|------|-------|
| `content_id` | integer | Forecast content/source ID |
| `content_name` | varchar | Human-readable source name |
| `update_id` | varchar (PK) | Unique forecast update (e.g., `202603030600_post_ECMWF-ENS`) |
| `issue_date` | varchar | When forecast was generated (ISO, UTC) |
| `forecast_period_start` | timestamp | Start of forecast period |
| `forecast_period_end` | timestamp | End of forecast period |
| `utc_offset_from` / `utc_offset_to` | varchar | Timezone info (e.g., `UTC-0400`) |
| `forecast_mw` or `day_ahead_price` | int / double | The forecasted value |
| `created_at` / `updated_at` | timestamptz | Audit timestamps |

### Recommended dbt Staging Models

Create three dbt models in the external `helioscta-backend` repo to normalize and aggregate:

1. **`staging_v1_meteologica_pjm_demand_forecast_hourly`**
   - Unions RTO + 3 regional demand tables
   - Adds `region` column: `'RTO'`, `'MID_ATLANTIC'`, `'SOUTH'`, `'WESTERN'`
   - Deduplicates to latest `issue_date` per forecast period via `ROW_NUMBER()` → `forecast_rank`
   - Normalizes `forecast_period_start` → `date` + `hour_ending` (1-24)

2. **`staging_v1_meteologica_pjm_generation_forecast_hourly`**
   - Unions solar, wind, hydro tables
   - Adds `source` column: `'solar'`, `'wind'`, `'hydro'`
   - Same deduplication and normalization

3. **`staging_v1_meteologica_pjm_da_price_forecast_hourly`**
   - Unions 12 hub-level DA price forecast tables
   - Adds `hub` column derived from table/content name
   - Same deduplication and normalization

These follow the existing pattern of `staging_v1_pjm_*` views — each model is one data domain, independently queryable.

### Key Considerations

- **Multiple forecasts per hour**: Different model runs produce separate `update_id`s. Filter to `forecast_rank = 1` for latest.
- **Timezone handling**: `forecast_period_start` is UTC. Convert to EPT and extract `hour_ending` (1-24) to match existing dbt patterns.
- **Regional hierarchy**: 50+ utility-level demand tables exist. Start with 4 regions (RTO, MidAtlantic, South, West); add granular utility-level later if needed.

---

## 4. Backend Architecture

### New Module: `backend/src/pjm_stack_model/`

Parallel to the existing `pjm_like_day` module:

```
backend/src/pjm_stack_model/
├── __init__.py
├── configs.py                  # schema name, fuel categories, defaults
├── pipeline.py                 # orchestrator: data pull → dispatch → format
├── dispatch.py                 # merit-order dispatch engine
├── sensitivity.py              # load × gas price sensitivity sweep
├── ramp.py                     # morning/evening ramp analysis
├── data/
│   ├── __init__.py
│   ├── generators.py           # pull_generators() from DB
│   ├── assumptions.py          # pull_assumptions() from DB
│   └── forecasts.py            # pull_load_forecast(), pull_gen_forecast()
└── sql/
    ├── generators.sql
    ├── assumptions.sql
    ├── meteologica_demand.sql
    └── meteologica_generation.sql
```

### Dispatch Engine (`dispatch.py`)

Core merit-order algorithm for a single hour:

1. Load generator units from PostgreSQL
2. Compute Effective MC per unit:
   ```
   Fuel Cost       = Heat Rate (BTU/kWh) × Fuel Price ($/MMBtu) / 1000
   Emissions Cost  = CO2 Emissions (tons/MWh) × (RGGI + CSAPR + CO2 cost)
   Effective MC    = Fuel Cost + Variable O&M + Emissions Cost
   ```
3. Subtract renewable generation: `Net Load = Load − Wind − Solar`
4. Filter out `on_off = FALSE` units
5. Force-commit `must_run = TRUE` units first
6. Sort remaining by Effective MC ascending
7. Walk the merit order, accumulating capacity until cumulative MW ≥ net load
8. Marginal unit (last committed) sets the clearing price

Run for each HE 1-24 where load varies → produces 24-hour dispatch schedule.

### Pipeline Orchestrator (`pipeline.py`)

```
run(target_date, hub, assumptions_override) →
  1. pull_generators()
  2. pull_assumptions() + merge user overrides
  3. pull_load_forecast() from Meteologica (or user-supplied hourly values)
  4. pull_gen_forecast() for wind/solar/hydro
  5. dispatch_hour() × 24
  6. aggregate: summary stats, fuel mix, hub breakdown, ramp analysis
  7. return JSON-serializable result dict
```

### FastAPI Endpoints

Add to `backend/src/api.py`:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `POST /stack-dispatch` | POST | Run 24-hour merit-order dispatch for a target date |
| `POST /stack-sensitivity` | POST | Load × gas price sensitivity matrix |
| `POST /stack-ramp` | POST | Morning/evening ramp transition analysis |

**`POST /stack-dispatch` parameters:**
- `target_date` (date, optional — defaults to tomorrow)
- `hub` (str, default `"WESTERN HUB"`)
- Body: `{ "assumptions": { "fuel_hub": price, ... }, "hourly_load": [MW×24], "hourly_wind": [MW×24], "hourly_solar": [MW×24] }`
- If hourly arrays omitted, pulls from Meteologica forecasts

**`POST /stack-sensitivity` parameters:**
- `target_date`, `hub`
- `load_range` (str, e.g., `"-10,+10"` percent)
- `gas_range` (str, e.g., `"-2,+2"` $/MMBtu)
- `steps` (int, default 5)

---

## 5. Frontend Architecture

### API Routes

| Route | Method | Type | Description |
|-------|--------|------|-------------|
| `/api/stack-model/generators` | GET | Direct DB | List generator units with optional fuel_category filter |
| `/api/stack-model/assumptions` | GET | Direct DB | Read fuel price assumptions |
| `/api/stack-model/assumptions` | PUT | Direct DB | Update fuel price assumptions |
| `/api/stack-model/forecasts` | GET | Direct DB | Pull Meteologica demand/generation forecasts |
| `/api/stack-model/dispatch` | POST | Python proxy | Run merit-order dispatch engine |
| `/api/stack-model/sensitivity` | POST | Python proxy | Run sensitivity sweep |
| `/api/stack-model/ramp` | POST | Python proxy | Compute ramp analysis |

Direct DB routes follow the pattern in `frontend/app/api/pjm-lmps-hourly/route.ts` (parameterized queries via `query()` from `lib/db.ts`).

Python proxy routes follow the pattern in `frontend/app/api/pjm-like-day/route.ts` (forward to `PYTHON_API_URL`).

### Components

| Component | File | Maps To (Excel) |
|-----------|------|-----------------|
| **StackModelInputs** | `frontend/components/power/StackModelInputs.tsx` | Forecast Inputs + Assumptions |
| **StackModelDashboard** | `frontend/components/power/StackModelDashboard.tsx` | Dispatch Dashboard + Hub Summary |
| **StackModelCurve** | `frontend/components/power/StackModelCurve.tsx` | Stack Model supply curve |
| **StackModelSensitivity** | `frontend/components/power/StackModelSensitivity.tsx` | Price Sensitivity + Ramp Analysis |

### Component Details

**StackModelInputs.tsx**
- Date picker, hub selector, "Run Dispatch" button
- 24-row table: HE 1-24 × Load MW / Wind MW / Solar MW (pre-populated from Meteologica, user-editable)
- Net Load column computed client-side
- Collapsible Assumptions panel: fuel price inputs per hub + emissions costs
- "Apply & Run" sends full input set to `/api/stack-model/dispatch`

**StackModelDashboard.tsx**
- KPI cards: Peak Load, Peak Price, Marginal Fuel, Reserve Margin, Min/Max Price
- 24-hour dispatch chart (Recharts AreaChart): X=HE, Y=$/MWh, stacked areas by fuel type
- Fuel mix pie chart
- Hub summary table: installed capacity, load, clearing price, marginal unit, generation by fuel

**StackModelCurve.tsx**
- Supply curve chart: X=Cumulative Capacity (MW), Y=Effective MC ($/MWh)
- Bar segments colored by fuel type
- Horizontal line = clearing price, vertical line = load level
- Hover tooltip: Plant Name, Fuel Type, Capacity, Effective MC
- Generator table below: ~4,600 rows, sticky headers, sortable/filterable, heatmap on Effective MC

**StackModelSensitivity.tsx**
- Sensitivity heatmap table: rows=load levels, cols=gas price levels, cells=clearing price
- Color gradient green→red following existing `cellBg()` pattern
- Ramp analysis section: morning ramp-up chart, evening peak chart, unit start/stop transitions

### Navigation Changes

**`frontend/components/Sidebar.tsx`:**
- Extend `ActiveSection` type with: `"stack-inputs" | "stack-dashboard" | "stack-curve" | "stack-sensitivity"`
- Add new NAV_SECTIONS entry:
  ```
  { title: "Stack Model", items: [
    { id: "stack-inputs",       label: "Forecast Inputs",     iconColor: "text-red-400" },
    { id: "stack-dashboard",    label: "Dispatch Dashboard",  iconColor: "text-amber-500" },
    { id: "stack-curve",        label: "Supply Curve",        iconColor: "text-blue-500" },
    { id: "stack-sensitivity",  label: "Sensitivity",         iconColor: "text-pink-400" },
  ]}
  ```

**`frontend/app/HomePageClient.tsx`:**
- Add 4 entries to `SECTION_META`
- Import and conditionally render 4 components

---

## 6. API Response Schemas

### `POST /stack-dispatch` Response

```json
{
  "target_date": "2026-03-04",
  "hub": "WESTERN HUB",
  "assumptions": { "henry_hub": 3.25, "transco_z6": 3.50, "rggi": 15.00 },
  "hourly_dispatch": [
    {
      "hour_ending": 1,
      "load_mw": 85000,
      "wind_mw": 4200,
      "solar_mw": 0,
      "net_load_mw": 80800,
      "clearing_price": 24.50,
      "marginal_unit": "Plant XYZ Unit 2",
      "marginal_fuel": "Gas CC",
      "reserve_margin_pct": 18.5,
      "fuel_mix": {
        "Nuclear": 33000, "Coal": 15000, "Gas CC": 22800,
        "Gas CT": 0, "Wind": 4200, "Solar": 0, "Hydro": 6000
      }
    }
  ],
  "summary": {
    "peak_load_mw": 112000,
    "peak_load_he": 17,
    "peak_price": 48.75,
    "peak_price_he": 17,
    "min_price": 18.20,
    "min_price_he": 4,
    "avg_price": 32.10,
    "dominant_marginal_fuel": "Gas CC"
  }
}
```

### `POST /stack-sensitivity` Response

```json
{
  "target_date": "2026-03-04",
  "base_load_mw": 95000,
  "base_gas_price": 3.25,
  "matrix": [
    { "load_pct_change": -10, "gas_price": 2.00, "clearing_price": 18.50 },
    { "load_pct_change": 0,   "gas_price": 3.25, "clearing_price": 32.10 },
    { "load_pct_change": 10,  "gas_price": 5.00, "clearing_price": 52.80 }
  ]
}
```

---

## 7. Implementation Phases

### Phase 1: Database & Seed Data
- Create `stack_model` schema + two tables
- Build seed data loader script (reads Excel → upserts to PostgreSQL)
- Load generator units and default fuel price assumptions

### Phase 2: Backend Dispatch Engine
- Create `backend/src/pjm_stack_model/` module structure
- Implement `dispatch.py` merit-order engine
- Implement `pipeline.py` orchestrator
- Implement `sensitivity.py` and `ramp.py`
- Add three FastAPI endpoints to `backend/src/api.py`
- Use `PipelineLogger` for all logging

### Phase 3: Frontend API Routes
- Create 3 direct DB routes: generators, assumptions, forecasts
- Create 3 Python proxy routes: dispatch, sensitivity, ramp

### Phase 4: Frontend Components
- Build `StackModelInputs.tsx` with Meteologica pre-population
- Build `StackModelDashboard.tsx` with KPI cards + charts
- Build `StackModelCurve.tsx` with supply curve + generator table
- Build `StackModelSensitivity.tsx` with heatmap + ramp charts
- Update `Sidebar.tsx` and `HomePageClient.tsx`

### Phase 5: Integration & Testing
- Create validation notebook (`validate_stack_model_data.ipynb`) per project conventions
- End-to-end test: frontend inputs → API proxy → FastAPI dispatch → response → rendering
- Compare dispatch outputs against Excel reference for known dates
- Verify Meteologica forecast pull and display

---

## 8. Dependencies & Risks

### Dependencies
- **Meteologica dbt models** must be created in the external `helioscta-backend` repo before the forecasts API route can query aggregated data. Until then, query raw `meteologica.*` tables directly.
- **Generator fleet data** in Excel needs column mapping verification — exact headers for "PJM Raw Data" sheet to be confirmed during Phase 1 seed data loading.
- **dbt build** must be run after adding new staging models to materialize views in the target schema.

### Risks
- **Heat rate accuracy**: Dispatch clearing prices are highly sensitive to heat rates. Generator-specific heat rates in the Excel may be approximations. Add a calibration step comparing dispatch output to historical DA LMPs.
- **Fleet staleness**: The ~4,600 generator records are a point-in-time snapshot. PJM fleet changes (retirements, new builds) require periodic refresh.
- **Must-run simplification**: The boolean must-run flag doesn't capture min up/down time, ramp rates, or start costs. Phase 1 keeps it simple; extend later.
- **Meteologica forecast latency**: If data ingestion lags, the frontend may show stale forecasts. Display a "forecast as of" timestamp.
- **Gas price sensitivity**: $1/MMBtu change ≈ $7-11/MWh at the margin (depending on heat rate). Assumptions panel must clearly show which gas hub prices are active.

---

## 9. File Summary

### New Files

| File | Purpose |
|------|---------|
| `backend/src/pjm_stack_model/__init__.py` | Module init |
| `backend/src/pjm_stack_model/configs.py` | Schema, defaults, fuel categories |
| `backend/src/pjm_stack_model/pipeline.py` | Orchestrator |
| `backend/src/pjm_stack_model/dispatch.py` | Merit-order engine |
| `backend/src/pjm_stack_model/sensitivity.py` | Sensitivity sweep |
| `backend/src/pjm_stack_model/ramp.py` | Ramp analysis |
| `backend/src/pjm_stack_model/data/generators.py` | Pull generators from DB |
| `backend/src/pjm_stack_model/data/assumptions.py` | Pull assumptions from DB |
| `backend/src/pjm_stack_model/data/forecasts.py` | Pull Meteologica forecasts |
| `backend/src/pjm_stack_model/sql/generators.sql` | Generator units query |
| `backend/src/pjm_stack_model/sql/assumptions.sql` | Assumptions query |
| `backend/src/pjm_stack_model/sql/meteologica_demand.sql` | Demand forecast query |
| `backend/src/pjm_stack_model/sql/meteologica_generation.sql` | Generation forecast query |
| `backend/src/pjm_stack_model/load_seed_data.py` | One-time Excel → DB loader |
| `frontend/app/api/stack-model/generators/route.ts` | Direct DB: generator list |
| `frontend/app/api/stack-model/assumptions/route.ts` | Direct DB: fuel prices GET/PUT |
| `frontend/app/api/stack-model/forecasts/route.ts` | Direct DB: Meteologica forecasts |
| `frontend/app/api/stack-model/dispatch/route.ts` | Python proxy: dispatch |
| `frontend/app/api/stack-model/sensitivity/route.ts` | Python proxy: sensitivity |
| `frontend/app/api/stack-model/ramp/route.ts` | Python proxy: ramp |
| `frontend/components/power/StackModelInputs.tsx` | Forecast inputs + assumptions |
| `frontend/components/power/StackModelDashboard.tsx` | KPIs + hourly dispatch chart |
| `frontend/components/power/StackModelCurve.tsx` | Supply curve + generator table |
| `frontend/components/power/StackModelSensitivity.tsx` | Sensitivity heatmap + ramp |

### Modified Files

| File | Change |
|------|--------|
| `backend/src/api.py` | Add 3 FastAPI endpoints |
| `frontend/components/Sidebar.tsx` | Extend `ActiveSection`, add "Stack Model" nav section |
| `frontend/app/HomePageClient.tsx` | Add 4 `SECTION_META` entries + conditional renders |
