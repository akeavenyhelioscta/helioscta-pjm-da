# Helioscta API Scrapes Claude

Scraper and orchestration repository for PJM energy market datasets.

This repository is focused on:
- Scrape modules under `helioscta_api_scrapes/helioscta_api_scrapes/gridstatus/pjm` and `helioscta_api_scrapes/helioscta_api_scrapes/pjm`
- Prefect-based scheduling and execution
- PostgreSQL upsert and query utilities

## Local Setup

1. Create a Python 3.11 environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
pip install -e . --no-deps
```

3. Configure environment files used by scrapers:
- `helioscta_api_scrapes/helioscta_api_scrapes/gridstatus/.env`
- `helioscta_api_scrapes/helioscta_api_scrapes/utils/.env`

## Run Scrapers Manually

Examples:

```bash
python -m helioscta_api_scrapes.helioscta_api_scrapes.gridstatus.pjm.pjm_da_lmp
python -m helioscta_api_scrapes.helioscta_api_scrapes.pjm.da_hrl_lmps
```

## Run Prefect Stack

```bash
docker compose up --build
```

Services:
- `postgres`
- `prefect-server`
- `prefect-worker`

## Environment Variables

Common variables used across scrapers/utilities:
- `AZURE_POSTGRESQL_DB_HOST`
- `AZURE_POSTGRESQL_DB_PORT`
- `AZURE_POSTGRESQL_DB_NAME`
- `AZURE_POSTGRESQL_DB_USER`
- `AZURE_POSTGRESQL_DB_PASSWORD`
- `GRIDSTATUS_API_KEY`
- `SLACK_WEBHOOK_URL`
- `SLACK_CHANNEL_NAME`
- `SLACK_GROUP_ID`

## Documentation

- `docs/README.md`
- `docs/architecture/overview.md`
- `docs/architecture/storage-routing.md`
- `docs/pjm/overview.md`
- `docs/pjm/scrape-registry.md`
