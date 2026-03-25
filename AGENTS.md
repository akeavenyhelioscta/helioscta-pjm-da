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
