# DBT vs Backend: Data Formatting Investigation

## Overview

Analysis of where data formatting should live across the three-layer stack: **DBT views** (Azure PostgreSQL) → **Backend** (FastAPI Python + Next.js API routes) → **Frontend** (React components).

**Current schema:** `dbt_pjm_v1_2026_feb_19` (43 models: 11 source, 19 staging, 3 utility, 3 query)
**DBT repo:** `helioscta-backend/backend/dbt/dbt_azure_postgresql/models/power/dbt_pjm_v1_2026_feb_19/`

---

## Current State: Where Formatting Happens Today

| Layer | What It Does | Examples |
|-------|-------------|----------|
| **DBT source models** | Normalize raw data — hour conversion (0-23 → 1-24), region mapping, dedup, date filtering | `source_v1_pjm_da_hrl_lmps.sql`, `source_v1_pjm_hrl_load_metered.sql` |
| **DBT staging models** | Aggregate hourly → daily with periods, rank forecasts by recency, merge verified/unverified RT LMPs | `staging_v1_pjm_lmps_daily.sql`, `staging_v1_pjm_lmps_rt_hourly.sql` |
| **Next.js API routes** | Thin pass-through queries (DISTINCT + ORDER BY + date filter). Dashboard route is exception: 12 queries in 2 phases | `pjm-lmps-hourly/route.ts`, `dashboard/route.ts` |
| **Python backend** | Like-day ML pipeline: column prefixing for multi-market, inner joins, z-score normalization, JSON serialization | `backend/src/pjm_like_day/pipeline.py` |
| **React components** | Pivot hourly → date×HE1-24 matrix, compute onpeak/offpeak/flat averages, compute MAE/MAPE/bias, heatmap coloring | `PjmLmpsHourlyTable.tsx`, `LoadForecastPerformance.tsx` |

---

## Issues Found

### 1. Schema Name Hardcoded in 13+ Files

`dbt_pjm_v1_2026_feb_19` appears ~33 times across:
- 12 frontend API route files (inline in SQL template literals)
- `backend/src/pjm_like_day/configs.py` (line 6)

A dbt rebuild produces a new schema name and requires updating all of these.

**Fix:** Set PostgreSQL `search_path` at the connection level via env var (`DBT_SCHEMA`). SQL strings drop the schema prefix entirely.

```typescript
// frontend/lib/db.ts
return new Pool({
  // ...existing config...
  options: `-c search_path=${process.env.DBT_SCHEMA || "dbt_pjm_v1_2026_feb_19"},public`,
});
```

```python
# backend/src/utils/azure_postgresql.py
psycopg2.connect(..., options=f"-c search_path={schema},public")
```

### 2. Region Name Inconsistency Between Views

Forecast views use different region names than actuals views:

| Canonical | Forecast Views | Actuals Views |
|-----------|---------------|---------------|
| `RTO` | `RTO_COMBINED` | `RTO` |
| `MIDATL` | `MID_ATLANTIC_REGION` | `MIDATL` |
| `SOUTH` | `SOUTHERN_REGION` | `SOUTH` |
| `WEST` | `WESTERN_REGION` | `WEST` |

Frontend hardcodes this mapping in 4+ route files (`pjm-load-forecast-performance/route.ts` lines 10-15, etc.) and the dashboard route.

**Fix:** Normalize region names inside the DBT forecast source models (`source_v1_pjm_seven_day_load_forecast.sql`, `source_v1_gridstatus_pjm_load_forecast.sql`) with a CASE WHEN. Every downstream consumer gets consistent names.

### 3. Onpeak/Offpeak/Flat Averages Computed in 6 React Components

Identical logic duplicated across `PjmLmpsHourlyTable.tsx`, `PjmLoadRtMeteredHourly.tsx`, `LoadForecastPerformance.tsx`, etc.:
```typescript
const ONPEAK_HOURS = new Set([8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]);
const OFFPEAK_HOURS = new Set([1,2,3,4,5,6,7,24]);
```

DBT already does this for LMPs (`staging_v1_pjm_lmps_daily`) but **not for load data**. Additionally, the frontend ignores weekends/holidays — it applies peak/offpeak to all days, while the DBT date utility (`utils_v1_pjm_dates_daily`) correctly classifies weekends/holidays as flat.

**Fix:** Create daily aggregation views for load types that lack them:
- `staging_v1_pjm_load_rt_metered_daily`
- `staging_v1_pjm_load_rt_prelim_daily`
- `staging_v1_pjm_load_forecast_daily`

Follow the existing `staging_v1_pjm_lmps_daily` pattern, joining with `utils_v1_pjm_dates_hourly` for correct period classification.

### 4. Forecast Performance Metrics Computed in Frontend

Three React components independently calculate MAE, MAPE, bias, DA-window error (~30 lines each in `computeMetrics` functions). This is deterministic math on existing DB data.

**Fix:** Create DBT query models:
- `query_v1_pjm_load_forecast_performance_hourly` — joins forecast (rank=1) with actuals, computes `error_mw`, `abs_error_mw`, `error_pct`
- `query_v1_pjm_load_forecast_performance_daily` — aggregates to daily MAE/MAPE/bias by region/period
- Similar for solar and wind

### 5. Dashboard Two-Phase Query Pattern

`dashboard/route.ts` runs 12 queries in 2 phases. Phase 2 (yesterday comparisons) depends on Phase 1 results to know the current hour_ending.

**Fix:** Use `LAG()` window functions so each query returns both current and yesterday values in one pass. Reduces from 12 queries to 4 parallel queries.

---

## Decision Framework: DBT vs Backend vs Frontend

### Put in DBT when:
- The computation is **deterministic and domain-specific** (period averages, error metrics, region mapping)
- The same logic is **duplicated in multiple consumers** (6 components computing onpeak/offpeak)
- It represents **business rules** not display preferences (NERC holiday handling, forecast ranking)
- The output is **reusable** across frontend, backend, and direct SQL analysis

### Put in Backend (Python/API routes) when:
- The computation requires **ML or statistical methods** (like-day KNN, z-score normalization)
- It involves **cross-request state** or complex orchestration
- It needs **libraries not available in SQL** (scikit-learn, pandas)

### Keep in Frontend when:
- It's **display-specific** — pivoting for table layout, chart data shaping, color coding
- It depends on **user interaction state** — selected metric, visible date range, filter selections
- It's **fast and trivial** — the hourly pivot runs on <2500 rows in <10ms

---

## What Should NOT Move to DBT

| Computation | Current Location | Why It Stays |
|-------------|-----------------|-------------|
| Hourly pivot (date × HE1-24 matrix) | React `useMemo` | Display-specific; depends on which metric column is selected |
| Heatmap color gradients | React `cellBg()` | Rendering concern; depends on visible min/max range |
| Chart series construction | React `useMemo` | Interactive; depends on toggled lines, zoom level |
| Forecast vintage selection | React state | User-driven interactivity |

---

## Materialization Strategy

**Current:** All 43 models are PostgreSQL views (no tables/materialized views).

**Recommendation: No change yet.** Data volumes are modest (~200K hourly rows across all tables), the team is small (1-5 users), and Azure PostgreSQL handles these view queries in <100ms. Views are simpler, always fresh, and avoid stale-data risk.

**Revisit when:**
- Historical data exceeds ~2 years of hourly data per table
- Query response times exceed 500ms at the API level
- Computation-heavy models are added (rolling 30-day performance averages, etc.)

**Priority candidates for future materialization:**
1. Forecast performance daily views (multi-table joins with aggregation)
2. Dashboard KPI summary (if consolidated into a single view)

---

## Implementation Priority

| Priority | Improvement | Effort | Impact |
|----------|-----------|--------|--------|
| **P1** | Schema centralization (`search_path`) | Small — 2 connection files + remove prefixes from 12 routes | Eliminates 13-file update on every dbt rebuild |
| **P1** | Region normalization in DBT forecast models | Small — 2 dbt source models + remove mapping from 5 routes | Single source of truth for region names |
| **P2** | Daily load aggregation views | Medium — 3 new dbt models + update 6 components | Removes duplicated logic, fixes weekend/holiday bug |
| **P2** | Dashboard window functions | Medium — rewrite 12 queries as 4 | Simpler API route, faster page load |
| **P3** | Forecast performance views | Medium-high — 4-6 dbt models + refactor 3 components | Large code reduction, but current approach works |

---

## Cross-Repo Lineage: helioscta-backend ↔ helioscta-pjm-da

### The Problem

The two repos are connected only by the shared Azure PostgreSQL database. There's no formal documentation of which dbt models are consumed by which frontend routes or backend pipelines. If a dbt model changes, there's no way to know what breaks downstream.

### Solution: dbt Exposures

dbt has a built-in feature called **exposures** designed exactly for this. Exposures document downstream consumers of dbt models — dashboards, APIs, ML pipelines — and show them in the `dbt docs` lineage graph.

Add an `exposures.yml` file in the dbt project:

**File:** `helioscta-backend/backend/dbt/dbt_azure_postgresql/models/power/dbt_pjm_v1_2026_feb_19/exposures.yml`

```yaml
version: 2

exposures:

  # --- helioscta-pjm-da: Next.js API Routes ---

  - name: pjm_lmps_hourly_page
    type: application
    description: >
      PJM LMPs Hourly table and chart in helioscta-pjm-da frontend.
      Route: /api/pjm-lmps-hourly
    owner:
      name: Aidan Keaveny
    depends_on:
      - ref('staging_v1_pjm_lmps_hourly')

  - name: pjm_dashboard
    type: dashboard
    description: >
      Main dashboard KPIs (RT load, DA LMP, DA load, forecast load).
      Route: /api/dashboard
    owner:
      name: Aidan Keaveny
    depends_on:
      - ref('staging_v1_pjm_load_rt_metered_hourly')
      - ref('staging_v1_pjm_lmps_hourly')
      - ref('staging_v1_pjm_load_da_hourly')
      - ref('staging_v1_pjm_load_forecast_hourly')

  - name: pjm_load_forecast_performance
    type: application
    description: >
      Load forecast vs actual performance metrics (MAE, MAPE, bias).
      Route: /api/pjm-load-forecast-performance
    owner:
      name: Aidan Keaveny
    depends_on:
      - ref('staging_v1_pjm_load_forecast_hourly')
      - ref('staging_v1_pjm_load_rt_prelim_hourly')

  - name: pjm_load_rt_metered_page
    type: application
    description: >
      RT metered load hourly table and chart.
      Route: /api/pjm-load-rt-metered-hourly
    owner:
      name: Aidan Keaveny
    depends_on:
      - ref('staging_v1_pjm_load_rt_metered_hourly')

  - name: pjm_load_da_page
    type: application
    description: >
      DA load hourly table and chart.
      Route: /api/pjm-load-da-hourly
    owner:
      name: Aidan Keaveny
    depends_on:
      - ref('staging_v1_pjm_load_da_hourly')

  - name: pjm_load_forecast_page
    type: application
    description: >
      Load forecast hourly explorer with vintage selection.
      Route: /api/pjm-load-forecast-hourly
    owner:
      name: Aidan Keaveny
    depends_on:
      - ref('staging_v1_pjm_load_forecast_hourly')
      - ref('staging_v1_gridstatus_pjm_load_forecast_hourly')

  # --- helioscta-pjm-da: Python Backend (Like-Day ML) ---

  - name: pjm_like_day_pipeline
    type: ml
    description: >
      Like-day similarity pipeline in helioscta-pjm-da Python backend.
      Endpoint: POST /like-day
    owner:
      name: Aidan Keaveny
    depends_on:
      - ref('staging_v1_pjm_lmps_hourly')
```

This gives you:
- **Visual lineage** in `dbt docs serve` showing which models feed which applications
- **Impact analysis** — before changing a staging model, see all downstream exposures
- **Ownership tracking** — who owns each consumer

### What This Looks Like

After running `dbt docs generate && dbt docs serve`, the lineage DAG shows:

```
raw sources → source models → staging models → [exposure: pjm_dashboard]
                                              → [exposure: pjm_like_day_pipeline]
                                              → [exposure: pjm_lmps_hourly_page]
```

Exposures appear as green nodes at the end of the DAG, making it immediately clear which models have downstream consumers.

---

## dbt Documentation for helioscta-backend

### Current State

The dbt project has **zero documentation infrastructure**:
- No column descriptions in any `.yml` file
- No doc blocks
- No `dbt docs generate` in any CI/CD pipeline
- `sources.yml` files list table names only (no descriptions, no tests)
- The `.skills/dbt/dbt-preferences.md` has 309 lines of conventions but none are in dbt-native format

### Recommended Documentation Structure

#### Step 1: Add schema.yml Files With Model + Column Descriptions

Create a `schema.yml` alongside each model subdirectory. Example for the PJM staging layer:

**File:** `models/power/dbt_pjm_v1_2026_feb_19/staging/schema.yml`

```yaml
version: 2

models:

  - name: staging_v1_pjm_lmps_hourly
    description: >
      Hourly LMPs combining DA, RT, and DART (DA minus RT) markets.
      RT uses verified prices when available, falls back to unverified.
      Grain: date x hour_ending x hub x market.
    columns:
      - name: date
        description: "Trade date (EPT)"
        tests: [not_null]
      - name: hour_ending
        description: "Hour ending 1-24 (EPT)"
        tests:
          - not_null
          - accepted_values:
              values: [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24]
      - name: hub
        description: "Pricing node (e.g., WESTERN HUB)"
        tests: [not_null]
      - name: market
        description: "da = day-ahead, rt = real-time, dart = DA minus RT spread"
        tests:
          - not_null
          - accepted_values:
              values: ['da', 'rt', 'dart']
      - name: lmp_total
        description: "Total LMP ($/MWh) = energy + congestion + losses"
      - name: lmp_system_energy_price
        description: "System energy component of LMP ($/MWh)"
      - name: lmp_congestion_price
        description: "Congestion component of LMP ($/MWh)"
      - name: lmp_marginal_loss_price
        description: "Marginal loss component of LMP ($/MWh)"

  - name: staging_v1_pjm_load_rt_metered_hourly
    description: >
      Hourly real-time metered load by region. Company-verified data preferred.
      Available from 2014 to present. Grain: date x hour_ending x region.
    columns:
      - name: date
        description: "Trade date (EPT)"
        tests: [not_null]
      - name: hour_ending
        description: "Hour ending 1-24 (EPT)"
        tests: [not_null]
      - name: region
        description: "Load region: RTO, MIDATL, WEST, SOUTH, or utility zone"
        tests: [not_null]
      - name: rt_load_mw
        description: "Real-time metered load (MW)"

  - name: staging_v1_pjm_load_forecast_hourly
    description: >
      7-day load forecast with multiple vintages (forecast revisions).
      Only complete forecasts included (24 hours per forecast_date).
      Ranked by recency: forecast_rank=1 is the latest vintage.
      Grain: forecast_execution_datetime x forecast_date x hour_ending x region.
    columns:
      - name: forecast_rank
        description: "1 = most recent forecast revision for this forecast_date"
      - name: forecast_execution_datetime
        description: "When this forecast was published"
      - name: forecast_date
        description: "The date being forecasted"
      - name: hour_ending
        description: "Hour ending 1-24 (EPT)"
      - name: region
        description: "RTO, MIDATL, WEST, or SOUTH"
      - name: forecast_load_mw
        description: "Forecasted load (MW)"
```

#### Step 2: Add Source Descriptions

Enrich existing `sources.yml` with descriptions:

```yaml
version: 2

sources:
  - name: pjm_v1
    description: "Raw PJM API data ingested by GridStatus and Prefect pipelines"
    schema: pjm
    tables:
      - name: da_hrl_lmps
        description: "Day-ahead hourly LMPs from PJM Data Miner 2 API"
      - name: hourly_load_metered
        description: "Hourly metered load by zone/region from PJM"
      # ... etc
```

#### Step 3: Generate and Serve Docs

```bash
cd helioscta-backend/backend/dbt/dbt_azure_postgresql
dbt docs generate
dbt docs serve --port 8080
```

This produces an interactive website with:
- Full model lineage DAG (source → staging → exposures)
- Searchable column descriptions
- Test results
- Source freshness (if configured)

### Documentation Rollout Priority

| Priority | What | Files to Create/Edit |
|----------|------|---------------------|
| **P1** | Add exposures.yml (cross-repo lineage) | 1 new file |
| **P1** | Add staging schema.yml (most-queried models) | 1 new file covering ~19 staging models |
| **P2** | Enrich sources.yml with descriptions | Edit 4 existing files |
| **P2** | Add source schema.yml | 1 new file covering ~11 source models |
| **P3** | Add utils/query schema.yml | 1 new file covering ~6 models |
| **P3** | Add dbt tests (not_null, unique, accepted_values) | Within schema.yml files |

### All Changes Go in helioscta-backend

All dbt documentation and exposures are added to the dbt project in `helioscta-backend`. The helioscta-pjm-da repo doesn't need any changes for lineage — it's documented as a downstream consumer via exposures.
