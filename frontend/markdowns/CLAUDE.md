# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Spark Spread Visualization — a Next.js 15 (App Router) energy trading dashboard for PJM spark spreads, power prices, and market analytics. Connects to Azure PostgreSQL for ICE futures settlement data. Authentication via Microsoft Entra ID (Azure AD) with email allowlist.

## Commands

- `npm run dev` — Start development server (http://localhost:3000)
- `npm run build` — Production build
- `npm start` — Run production server
- `npm run lint` — ESLint check
- `npm run lint:fix` — Auto-fix lint issues

No test framework is configured.

## Architecture

### Data Flow

API routes (`app/api/`) query Azure PostgreSQL → transform data in `lib/` → return JSON with 5-minute cache headers → client components fetch and render with Recharts.

### View Routing

`?view=` URL parameter controls which dashboard renders. `HomePageClient.tsx` manages view state and URL sync. Views are dynamically imported with Suspense for code splitting.

### Key Directories

- `app/api/` — API routes (spark-spreads, contract-evolution, power-calendar, power-outright, spark-analytics, da-lmps, synmax/production, historical-settlements)
- `components/` — Chart and UI components (Recharts-based)
- `components/power/` — Analytics dashboard, historical settlements, DA LMPs
- `components/synmax/` — SynMax gas production dashboard
- `lib/` — Database pool (`db.ts`), calculation logic, symbol builders, data transforms
- `types/` — TypeScript interfaces for API responses

### Domain Concepts

- **Spark Spread:** `PMI - (HNG + TMT) × 7.0` (heat rate 7.0 MMBtu/MWh)
- **Calendar Spread:** Near leg PMI minus far leg PMI
- **Gas Composite:** HNG + TMT (Tetco M3 basis)
- **ICE Symbol Format:** `{PRODUCT} {MONTH_CODE}{YEAR}-IUS` (e.g., `PMI H26-IUS`)
- **Composite Strips:** Q1–Q4 (quarters), JF (Jan-Feb), JA (Jul-Aug), expanded into component month codes

### Database

PostgreSQL via `pg` library with global connection pool (max 5, idle 30s). Two main tables:
- `ice_python.future_contracts_v1_2025_dec_16` — ICE futures settlements
- `pjm_v1_2026_feb_19.staging_v1_pjm_lmps_hourly` — PJM hourly LMPs

The `pg` package is listed in `next.config.ts` as a `serverExternalPackage`.

### Authentication

NextAuth v5 beta with Microsoft Entra ID. `ALLOWED_EMAILS` env var controls access. Middleware protects all routes except `/api/auth/*`, `/login`, `/_next/*`, `/favicon.ico`.

### Styling

Tailwind CSS with a dark theme. Background: `#0f1117`, chart backgrounds: `#1a1d27`/`#0c0e16`, borders: `#374151`.

## Environment Variables

Required: `AZURE_POSTGRESQL_DB_HOST`, `AZURE_POSTGRESQL_DB_USER`, `AZURE_POSTGRESQL_DB_PASSWORD`, `AZURE_POSTGRESQL_DB_PORT` (default 5432), `AUTH_MICROSOFT_ENTRA_ID_ID`, `AUTH_MICROSOFT_ENTRA_ID_SECRET`, `AUTH_MICROSOFT_ENTRA_ID_TENANT_ID`, `NEXTAUTH_URL`, `NEXTAUTH_SECRET`, `ALLOWED_EMAILS`.

Optional: `SYNMAX_API_KEY`, `VERCEL_URL`.
