# Backend Repository for Data

## Source of Truth
The canonical backend repository for all data pipelines, table schemas, and dbt models is:

```
C:\Users\AidanKeaveny\Documents\github\helioscta-backend\backend
```

## When to Reference
- When writing or updating SQL queries in the frontend API routes
- When verifying table names, column names, data types, or primary keys
- When adding a new data source or panel to the dashboard
- When debugging data issues or missing columns

## Key Locations

### dbt Models (cleaned schemas)
- **PJM cleaned**: `backend/dbt/dbt_azure_postgresql/models/power/pjm_cleaned/`
- **Meteologica cleaned**: `backend/dbt/dbt_azure_postgresql/models/meteologica/meteologica_cleaned/`
- Source definitions: `sources.yml` in each directory

### Data Pipelines (raw ingestion)
- **PJM API pipelines**: `backend/src/power/pjm/` (16 scripts + flows.py)
- **Meteologica API pipelines**: `backend/src/meteologica/pjm/` (100+ scripts)

## MCP Database Connection
Always use the MCP postgres connection (`mcp__postgres__query`) to verify:
- Table existence: `SELECT table_name FROM information_schema.tables WHERE table_schema = '...'`
- Column names: `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '...' AND table_name = '...'`
- Distinct values: `SELECT DISTINCT region FROM ... ORDER BY region`

## Active Schemas
| Schema | Purpose |
|---|---|
| `pjm` | Raw PJM API data |
| `pjm_cleaned` | dbt-transformed PJM data (32 tables) |
| `meteologica` | Raw Meteologica API data |
| `meteologica_cleaned` | dbt-transformed Meteologica data (9 tables) |
| `gridstatus` | GridStatus-ingested PJM data |
