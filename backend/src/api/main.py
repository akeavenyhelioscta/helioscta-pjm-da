"""FastAPI entry point — run with: uvicorn src.api.main:app --reload

Serves view model endpoints for agent and frontend consumption.
"""
import logging
from pathlib import Path

import src.settings  # noqa: F401 — load env vars

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi_mcp import FastApiMCP

from src.like_day_forecast import configs
from src.like_day_forecast.pipelines.forecast import run as run_forecast
from src.data import outages_actual_daily
from src.utils.cache_utils import pull_with_cache
from src.views.forecast_results import build_view_model as forecast_view_model
from src.views.outage_term_bible import build_view_model as outage_view_model

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="PJM DA Forecast API",
    description="Structured view models for like-day forecast data",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

CACHE_KWARGS = dict(
    cache_dir=configs.CACHE_DIR,
    cache_enabled=configs.CACHE_ENABLED,
    ttl_hours=configs.CACHE_TTL_HOURS,
    force_refresh=configs.FORCE_CACHE_REFRESH,
)


@app.get("/health")
def health():
    cache_dir = configs.CACHE_DIR
    parquet_count = len(list(Path(cache_dir).glob("*.parquet"))) if cache_dir and cache_dir.exists() else 0
    return {"status": "ok", "cache_datasets": parquet_count}


@app.get("/views/forecast_results")
def get_forecast_results(
    forecast_date: str | None = Query(None, description="YYYY-MM-DD, defaults to tomorrow"),
):
    """Run the like-day forecast and return the structured view model."""
    result = run_forecast(
        forecast_date=forecast_date,
        config=configs.ScenarioConfig(forecast_date=forecast_date),
        cache_dir=configs.CACHE_DIR,
        cache_enabled=configs.CACHE_ENABLED,
        cache_ttl_hours=configs.CACHE_TTL_HOURS,
        force_refresh=configs.FORCE_CACHE_REFRESH,
    )
    return forecast_view_model(result)


@app.get("/views/outage_term_bible")
def get_outage_term_bible():
    """Return the outage term bible view model with historical context."""
    df = pull_with_cache(
        source_name="outages_actual_daily_history",
        pull_fn=outages_actual_daily.pull,
        pull_kwargs={"sql_overrides": {"start_date": "2023-01-01"}},
        **CACHE_KWARGS,
    )
    return outage_view_model(df)


# ── MCP integration — exposes all endpoints as agent tools ──────────
mcp = FastApiMCP(app)
mcp.mount()
