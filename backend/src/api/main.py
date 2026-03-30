"""FastAPI entry point — run with: uvicorn src.api.main:app --reload

Serves view model endpoints for agent and frontend consumption.
"""
import logging
from enum import Enum
from pathlib import Path

import src.settings  # noqa: F401 — load env vars

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from fastapi_mcp import FastApiMCP

from src.like_day_forecast import configs
from src.like_day_forecast.pipelines.forecast import run as run_forecast
from src.data import outages_actual_daily, outages_forecast_daily, load_forecast_vintages, solar_forecast_vintages, wind_forecast_vintages, lmps_hourly, fuel_mix_hourly, transmission_outages
from src.utils.cache_utils import pull_with_cache
from src.views.like_day_forecast_results import build_view_model as forecast_view_model
from src.views.like_day_strip_forecast_results import build_view_model as strip_view_model
from src.views.outage_term_bible import build_view_model as outage_view_model
from src.views.load_forecast_vintages import build_view_model as load_forecast_view_model
from src.views.generation_forecast_vintages import build_view_model as generation_view_model
from src.views.lmp_7_day_lookback_western_hub import build_view_model as lmp_7day_view_model
from src.views.fuel_mix_7_day_lookback import build_view_model as fuel_mix_view_model
from src.views.transmission_outages import build_view_model as tx_outage_view_model
from src.views.outages_forecast_vintages import build_view_model as outage_fcst_view_model
from src.views.markdown_formatters import (
    format_lmp_7day,
    format_like_day_forecast_results,
    format_like_day_strip_forecast_results,
    format_load_forecast_vintages,
    format_generation_forecast_vintages,
    format_outage_term_bible,
    format_fuel_mix_7day,
    format_transmission_outages,
    format_outages_forecast_vintages,
)


class OutputFormat(str, Enum):
    md = "md"
    json = "json"

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


@app.get("/views/like_day_forecast_results")
def get_like_day_forecast_results(
    forecast_date: str | None = Query(None, description="YYYY-MM-DD, defaults to tomorrow"),
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Run the like-day forecast and return the structured view model with analog days."""
    result = run_forecast(
        forecast_date=forecast_date,
        config=configs.ScenarioConfig(forecast_date=forecast_date),
        cache_dir=configs.CACHE_DIR,
        cache_enabled=configs.CACHE_ENABLED,
        cache_ttl_hours=configs.CACHE_TTL_HOURS,
        force_refresh=configs.FORCE_CACHE_REFRESH,
    )
    vm = forecast_view_model(result)
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(content=format_like_day_forecast_results(vm), media_type="text/markdown")


@app.get("/views/like_day_strip_forecast_results")
def get_like_day_strip_forecast_results(
    horizon: int = Query(3, description="Number of days ahead to forecast (1-7)"),
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Run the like-day strip forecast (D+1 through D+N) and return the structured view model."""
    from src.like_day_forecast.pipelines.strip_forecast import run_strip

    result = run_strip(
        horizon=min(horizon, 7),
        config=configs.ScenarioConfig(),
    )
    vm = strip_view_model(result)
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(content=format_like_day_strip_forecast_results(vm), media_type="text/markdown")


@app.get("/views/outage_term_bible")
def get_outage_term_bible(
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Return the outage term bible view model with historical context."""
    df = pull_with_cache(
        source_name="outages_actual_daily",
        pull_fn=outages_actual_daily.pull,
        pull_kwargs={"schema": configs.SCHEMA},
        **CACHE_KWARGS,
    )
    vm = outage_view_model(df)
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(content=format_outage_term_bible(vm), media_type="text/markdown")


@app.get("/views/load_forecast_vintages")
def get_load_forecast_vintages(
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Return load forecast vintage evolution across all regions for PJM and Meteologica."""
    df_pjm = pull_with_cache(
        source_name="forecast_vintage_pjm",
        pull_fn=load_forecast_vintages.pull_combined_vintages,
        pull_kwargs={"source": "pjm"},
        **CACHE_KWARGS,
    )
    df_meteo = pull_with_cache(
        source_name="forecast_vintage_meteologica",
        pull_fn=load_forecast_vintages.pull_combined_vintages,
        pull_kwargs={"source": "meteologica"},
        **CACHE_KWARGS,
    )
    vm = load_forecast_view_model(df_pjm, df_meteo)
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(content=format_load_forecast_vintages(vm), media_type="text/markdown")


@app.get("/views/solar_forecast_vintages")
def get_solar_forecast_vintages(
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Return solar forecast vintage evolution across all regions for PJM and Meteologica."""
    df = pull_with_cache(
        source_name="solar_vintage_combined",
        pull_fn=solar_forecast_vintages.pull_combined_vintages,
        pull_kwargs={},
        **CACHE_KWARGS,
    )
    vm = generation_view_model(df, forecast_type="solar")
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(content=format_generation_forecast_vintages(vm), media_type="text/markdown")


@app.get("/views/wind_forecast_vintages")
def get_wind_forecast_vintages(
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Return wind forecast vintage evolution across all regions for PJM and Meteologica."""
    df = pull_with_cache(
        source_name="wind_vintage_combined",
        pull_fn=wind_forecast_vintages.pull_combined_vintages,
        pull_kwargs={},
        **CACHE_KWARGS,
    )
    vm = generation_view_model(df, forecast_type="wind")
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(content=format_generation_forecast_vintages(vm), media_type="text/markdown")


@app.get("/views/lmp_7_day_lookback_western_hub")
def get_lmp_7_day_lookback_western_hub(
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Return DA / RT / DART LMP history for Western Hub (last 7 days)."""
    from datetime import date, timedelta

    start = str(date.today() - timedelta(days=7))
    end = str(date.today())
    hub = "WESTERN HUB"
    df_da = pull_with_cache(
        source_name="lmps_hourly_da_western_hub_7d",
        pull_fn=lmps_hourly.pull,
        pull_kwargs={"market": "da", "hub": hub, "sql_overrides": {"start_date": start, "end_date": end}},
        **CACHE_KWARGS,
    )
    df_rt = pull_with_cache(
        source_name="lmps_hourly_rt_western_hub_7d",
        pull_fn=lmps_hourly.pull,
        pull_kwargs={"market": "rt", "hub": hub, "sql_overrides": {"start_date": start, "end_date": end}},
        **CACHE_KWARGS,
    )
    vm = lmp_7day_view_model(df_da, df_rt)
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(content=format_lmp_7day(vm), media_type="text/markdown")


@app.get("/views/fuel_mix_7_day_lookback")
def get_fuel_mix_7_day_lookback(
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Return hourly generation by fuel type for the last 7 days."""
    from datetime import date, timedelta

    start = str(date.today() - timedelta(days=7))
    end = str(date.today())
    df = pull_with_cache(
        source_name="fuel_mix_7d",
        pull_fn=fuel_mix_hourly.pull,
        pull_kwargs={"sql_overrides": {"start_date": start, "end_date": end}},
        **CACHE_KWARGS,
    )
    vm = fuel_mix_view_model(df)
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(content=format_fuel_mix_7day(vm), media_type="text/markdown")


@app.get("/views/outages_forecast_vintages")
def get_outages_forecast_vintages(
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Return generation outage forecast vintages — how the 7-day outage forecast has evolved."""
    df = pull_with_cache(
        source_name="outages_forecast_daily",
        pull_fn=outages_forecast_daily.pull,
        pull_kwargs={"lookback_days": 14},
        **CACHE_KWARGS,
    )
    vm = outage_fcst_view_model(df, region="RTO")
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(content=format_outages_forecast_vintages(vm), media_type="text/markdown")


@app.get("/views/transmission_outages")
def get_transmission_outages(
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Return active transmission outages: regional summary + notable individual outages."""
    df = pull_with_cache(
        source_name="transmission_outages_active",
        pull_fn=transmission_outages.pull,
        pull_kwargs={},
        **CACHE_KWARGS,
    )
    vm = tx_outage_view_model(df)
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(content=format_transmission_outages(vm), media_type="text/markdown")


# ── MCP integration — exposes all endpoints as agent tools ──────────
mcp = FastApiMCP(app)
mcp.mount_http()
