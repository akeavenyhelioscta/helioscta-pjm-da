# Repository Guidelines

## Coding Style & Naming Conventions
- Frontend: follow ESLint config (`next/core-web-vitals`, `next/typescript`) and strict TypeScript settings.
- Use 2-space indentation and keep React components in `PascalCase` (e.g., `CashBalmoTable.tsx`).
- Keep route handlers named `route.ts` inside feature folders under `frontend/app/api/`.
- Backend Python should follow PEP 8: 4-space indentation, `snake_case` for functions/files, clear type hints where practical.

## Testing Guidelines
- Backend uses `pytest` (`pyproject.toml` points to `tests`).
- Name tests `test_*.py` and place them in `backend/tests/`.
- Prioritize API/data-path coverage for DB query and route behavior changes.
- Frontend currently has no committed test runner; at minimum run lint and perform manual UI + API smoke checks.

## Commit & Pull Request Guidelines
- Existing history uses short subjects (for example `Gas EBBs`, `Genscape Watchlists`); prefer clearer scoped summaries.
- Recommended commit format: `<area>: <imperative summary>` (e.g., `frontend: add watchlist filter chips`).
- PRs should include: purpose, affected paths, env/migration changes, validation steps, and screenshots for UI updates.

## Security & Configuration Tips
- Never commit secrets from `.env`, `frontend/.env.local`, `backend/src/.env`, or `scripts/.env*`.
- Keep credentials in local env files or CI/CD secrets; use placeholders in docs/scripts.

## Data & Backend References
- When looking for data, always use MCP connections and refer to the backend repo.
- Backend repo for data pipelines, schemas, and dbt models: [`.claude/standards/backend-repo-for-data.md`](./.claude/standards/backend-repo-for-data.md).
- Backend repo path: `C:\Users\AidanKeaveny\Documents\github\helioscta-backend\backend`

## Agent References
- Frontend theme/style preferences: [`.claude/standards/frontend-styling.md`](./.claude/standards/frontend-styling.md).
- Backend data reference: [`.claude/standards/backend-repo-for-data.md`](./.claude/standards/backend-repo-for-data.md).
- Like-day SQL parameter standard: [`.claude/standards/sql-parameter-standard.md`](./.claude/standards/sql-parameter-standard.md).

## View Model API

The like-day forecast backend exposes structured view models via FastAPI.
Start the server: `cd backend && uvicorn src.api.main:app --reload --port 8000`

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Cache status and dataset count |
| `/views/like_day_forecast_results` | GET | Like-day DA LMP forecast (D+1) with analog days, hourly data, quantile bands, period summaries, and evaluation metrics |
| `/views/like_day_strip_forecast_results` | GET | Like-day strip forecast (D+1 through D+N): multi-day DA LMP forecast with analog days, per-day period summaries, P10/P90 bands, and hourly detail |
| `/views/outage_term_bible` | GET | Historical outage context: current level, seasonal percentile/z-score, YoY comparison, 7-day trend, decomposition by type |
| `/views/load_forecast_vintages` | GET | Load forecast evolution across all regions: per-region vintage summaries, vintage deltas, PJM vs Meteologica spread, cross-region highlights (largest revisions, source disagreements, trend directions) |
| `/views/solar_forecast_vintages` | GET | Solar forecast evolution across all regions: same structure as load vintages but for solar generation (PJM RTO-only + Meteologica all regions) |
| `/views/wind_forecast_vintages` | GET | Wind forecast evolution across all regions: same structure as load vintages but for wind generation (PJM RTO-only + Meteologica all regions) |
| `/views/lmp_7_day_lookback_western_hub` | GET | DA / RT / DART LMP history for Western Hub (last 7 days) with hourly detail and daily period summaries |
| `/views/fuel_mix_7_day_lookback` | GET | Hourly generation by fuel type (gas, coal, nuclear, solar, wind, etc.) for the last 7 days with daily period summaries and dispatchable ramps |
| `/views/outages_forecast_vintages` | GET | Generation outage forecast vintages: how the 7-day outage MW forecast has evolved across the last 8 execution dates, by outage type (total, forced, planned, maintenance) |
| `/views/transmission_outages` | GET | Active ≥230 kV transmission outages: regional summary (counts by voltage tier, risk flags) + notable individual outages (high-risk, 500kV+, new, returning soon) |

### Query parameters
- All view endpoints accept `?format=md|json` (default: `md`). Markdown tables are ~70-80% smaller than JSON and fit in agent context without truncation.
- `/views/like_day_forecast_results?forecast_date=YYYY-MM-DD` — override forecast target date (defaults to tomorrow)
- `/views/like_day_strip_forecast_results?horizon=N` — number of days ahead (default 3, max 7)
- `/views/load_forecast_vintages` — no parameters, returns all regions in one response
- `/views/solar_forecast_vintages` — no parameters, returns all regions in one response
- `/views/wind_forecast_vintages` — no parameters, returns all regions in one response

### How to use as an agent
1. All view endpoints return compact markdown tables by default. Pass `?format=json` only if you need structured data for computation.
2. Query `/views/like_day_forecast_results` for the current forecast — read the period summary and hourly detail tables
3. Query `/views/outage_term_bible` for supply context — read the current levels table for percentile and trend
4. Cross-reference: if outages are elevated (high percentile) and the forecast shows wide quantile bands at peak hours, flag the risk
5. The view models provide **structured facts only** — the agent does the interpretation (e.g., "97th percentile outages + Saturday = possible over-forecast on peak hours")
6. Full OpenAPI spec available at `/openapi.json`

### Cross-referencing views
- **RT LMP spikes or volatile DART spreads** → check `/views/fuel_mix_7_day_lookback` for gas/coal ramp events, renewable drop-offs, or generation shortfalls at those hours
- **RT congestion spikes** → check `/views/transmission_outages` for active high-voltage outages in the affected region, especially newly started or risk-flagged ones
- **Elevated outages** (high percentile in term bible) → check `/views/outages_forecast_vintages` to see if outage levels are rising or falling across recent forecast vintages, then flag supply-side risk if combined with wide forecast quantile bands
- **Large vintage deltas** in load/solar/wind → check if LMP forecast still aligns with revised supply/demand
- **Strip forecast context** → compare the strip's D+2/D+3 forecasts against load and generation vintage trends to see if fundamentals support the term structure shape
