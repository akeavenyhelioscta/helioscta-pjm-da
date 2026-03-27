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
| `/views/forecast_results` | GET | Like-day DA LMP forecast with hourly data, quantile bands, period summaries, and evaluation metrics |
| `/views/outage_term_bible` | GET | Historical outage context: current level, seasonal percentile/z-score, YoY comparison, 7-day trend, decomposition by type |

### Query parameters
- `/views/forecast_results?forecast_date=YYYY-MM-DD` — override forecast target date (defaults to tomorrow)

### How to use as an agent
1. Query `/views/forecast_results` for the current forecast — read `hourly[].forecast`, `summary`, `quantile_coverage`, and `metrics`
2. Query `/views/outage_term_bible` for supply context — read `outage_types.total_outages.month_context.percentile` and `trend_7d`
3. Cross-reference: if outages are elevated (high percentile) and the forecast shows wide quantile bands at peak hours, flag the risk
4. The view models provide **structured facts only** — the agent does the interpretation (e.g., "97th percentile outages + Saturday = possible over-forecast on peak hours")
5. Full OpenAPI spec available at `/openapi.json`
