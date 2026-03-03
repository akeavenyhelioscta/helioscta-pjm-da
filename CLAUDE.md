# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PJM energy market analytics platform. Next.js 15 frontend with FastAPI Python backend, both querying Azure PostgreSQL (dbt-generated views in schema `dbt_pjm_v1_2026_feb_19`).

## Commands

### Frontend (from `frontend/`)
```bash
npm run dev          # Dev server on localhost:3000
npm run build        # Production build
npm run lint         # ESLint check
npm run lint:fix     # ESLint auto-fix
```

### Backend (from `backend/`)
```bash
uvicorn src.api:app --reload --port 8000   # Dev server on localhost:8000
```

### Full Stack (from repo root)
```bash
docker compose up --build   # Both services via Docker
```

## Architecture

### Two-Service Design
- **Frontend:** Next.js 15 (App Router) + React 19 + TypeScript + Tailwind CSS + Recharts
- **Backend:** FastAPI (Python 3.12) + scikit-learn + pandas
- **Communication:** Next.js API routes proxy to FastAPI (via `PYTHON_API_URL` env var). The Like-Day feature requires both services running.

### Frontend Structure
- `frontend/app/` — App Router pages and API routes
- `frontend/components/power/` — Main data visualization components (tables, charts, dashboards)
- `frontend/lib/db.ts` — PostgreSQL connection pool (used by API routes for direct DB queries)
- `frontend/auth.ts` — NextAuth.js v5 with Microsoft Entra ID

### Backend Structure
- `backend/src/api.py` — FastAPI app with `/like-day` endpoint
- `backend/src/pjm_like_day/` — Like-day pipeline: `pipeline.py` orchestrates data pull → feature scaling → KNN similarity ranking
- `backend/src/pjm_like_day/data/` — One module per data source (currently `lmps.py` only)

### Data Flow Patterns
1. **Direct DB queries:** Most frontend API routes (`/api/pjm-lmps-hourly`, `/api/dashboard`, `/api/pjm-load-*`) query Azure PostgreSQL directly using the `pg` pool from `lib/db.ts`.
2. **Python proxy:** `/api/pjm-like-day` forwards to the FastAPI backend which runs the ML pipeline and returns results.

### Adding a New Data View
1. Create API route in `frontend/app/api/<name>/route.ts`
2. Create component in `frontend/components/power/<Name>.tsx`
3. Add section to `SECTION_META` in `HomePageClient.tsx`
4. Add nav item to `Sidebar.tsx` `NAV_SECTIONS`

## Key Conventions

- All components use `"use client"` — interactive client-side rendering
- Dark theme throughout: backgrounds `#0f1117`, `#0b0d14`, `#12141d`; borders `gray-800`; text `gray-100`/`gray-500`
- Tables use sticky headers + left columns, heatmap background gradients, alternating row colors
- Filter pattern: controlled inputs → "Apply" button → triggers fetch with `AbortController` cleanup
- Database schema is pinned by name (`dbt_pjm_v1_2026_feb_19`) — a new dbt build produces a new schema name
- The `.skills/` directory contains architecture decisions and feature specs (data_sources.md, plan.md, pjm_like_day.md)

## Environment Variables

Frontend (`frontend/.env.local`): `AZURE_POSTGRESQL_DB_HOST`, `AZURE_POSTGRESQL_DB_PORT`, `AZURE_POSTGRESQL_DB_NAME`, `AZURE_POSTGRESQL_DB_USER`, `AZURE_POSTGRESQL_DB_PASSWORD`, `PYTHON_API_URL`, NextAuth vars (`AUTH_MICROSOFT_ENTRA_ID_*`, `ALLOWED_EMAILS`)

Backend (`backend/src/.env`): Same Azure PostgreSQL credentials
