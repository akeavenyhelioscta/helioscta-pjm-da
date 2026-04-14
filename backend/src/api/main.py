"""FastAPI entry point — run with: uvicorn src.api.main:app --reload

Serves view model endpoints for agent and frontend consumption.
"""
import logging
from enum import Enum
from pathlib import Path

import pandas as pd

import src.settings  # noqa: F401 — load env vars

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from fastapi_mcp import FastApiMCP

from src.like_day_forecast import configs
from src.like_day_forecast.pipelines.forecast import run as run_forecast
from src.supply_stack_model.configs import SupplyStackConfig
from src.supply_stack_model.pipelines.forecast import run as run_supply_stack_forecast
from src.data import outages_actual_daily, outages_forecast_daily, load_forecast_vintages, solar_forecast_vintages, wind_forecast_vintages, lmps_hourly, fuel_mix_hourly, transmission_outages, ice_power_intraday, meteologica_da_price_forecast, gas_prices
from src.utils.cache_utils import pull_with_cache
from src.views.like_day_forecast_results import build_view_model as forecast_view_model
from src.views.supply_stack_forecast_results import build_view_model as supply_stack_view_model
from src.views.supply_stack_validation_results import build_validation_view_model as supply_stack_validation_vm
from src.views.like_day_strip_forecast_results import build_view_model as strip_view_model
from src.views.outage_term_bible import build_view_model as outage_view_model
from src.views.load_forecast_vintages import build_view_model as load_forecast_view_model
from src.views.generation_forecast_vintages import build_view_model as generation_view_model
from src.views.lmp_7_day_lookback_western_hub import build_view_model as lmp_7day_view_model
from src.views.fuel_mix_7_day_lookback import build_view_model as fuel_mix_view_model
from src.views.transmission_outages import build_view_model as tx_outage_view_model
from src.views.outages_forecast_vintages import build_view_model as outage_fcst_view_model
from src.views.ice_power_intraday import build_view_model as ice_power_view_model
from src.views.meteologica_da_forecast import build_view_model as meteo_da_view_model
from src.views.regional_congestion import build_view_model as regional_congestion_view_model
from src.views.gas_prices import build_view_model as gas_prices_view_model
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
    format_ice_power_intraday,
    format_meteologica_da_forecast,
    format_regional_congestion,
    format_lasso_qr_forecast_results,
    format_lasso_qr_strip_forecast_results,
    format_lgbm_qr_forecast_results,
    format_lgbm_qr_strip_forecast_results,
    format_supply_stack_forecast_results,
    format_supply_stack_validation_results,
    format_gas_prices,
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
    exclude_dates: str | None = Query(None, description="Comma-separated YYYY-MM-DD dates to exclude from analog pool"),
    exclude_holidays: bool = Query(True, description="Exclude NERC holidays from analog pool when target is not a holiday"),
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Run the like-day forecast and return the structured view model with analog days."""
    parsed_exclude = [d.strip() for d in exclude_dates.split(",") if d.strip()] if exclude_dates else []
    result = run_forecast(
        forecast_date=forecast_date,
        config=configs.ScenarioConfig(
            forecast_date=forecast_date,
            exclude_dates=parsed_exclude,
            exclude_holidays=exclude_holidays,
        ),
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


@app.get("/views/supply_stack_forecast_results")
def get_supply_stack_forecast_results(
    forecast_date: str | None = Query(None, description="YYYY-MM-DD, defaults to tomorrow"),
    region: str = Query("RTO", description="PJM region (RTO, WEST, MIDATL, SOUTH, etc.)"),
    region_preset: str | None = Query(
        None,
        description="Optional preset scope: rto, south, dominion",
    ),
    gas_hub_col: str | None = Query(
        None,
        description="Optional gas hub override (e.g. gas_m3, gas_dom_south, gas_tz6)",
    ),
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Run supply stack forecast and return a structured view model."""
    config = SupplyStackConfig(
        forecast_date=forecast_date,
        region=region,
        region_preset=region_preset,
        gas_hub_col=gas_hub_col,
    )
    result = run_supply_stack_forecast(config=config)
    vm = supply_stack_view_model(result)
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(
        content=format_supply_stack_forecast_results(vm),
        media_type="text/markdown",
    )


@app.get("/views/supply_stack_validation_results")
def get_supply_stack_validation_results(
    forecast_date: str | None = Query(None, description="YYYY-MM-DD, defaults to tomorrow"),
    region: str = Query("RTO", description="PJM region"),
    region_preset: str | None = Query(None, description="Optional preset: rto, south, dominion"),
    gas_hub_col: str | None = Query(None, description="Optional gas hub override"),
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md or json"),
):
    """Run supply stack validation checks and return diagnostics."""
    config = SupplyStackConfig(
        forecast_date=forecast_date,
        region=region,
        region_preset=region_preset,
        gas_hub_col=gas_hub_col,
    )
    vm = supply_stack_validation_vm(config=config)
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(
        content=format_supply_stack_validation_results(vm),
        media_type="text/markdown",
    )


@app.get("/views/outage_term_bible")
def get_outage_term_bible(
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Return the outage term bible view model with historical context."""
    df = pull_with_cache(
        source_name="pjm_outages_actual_daily",
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
        source_name="pjm_load_forecast_vintages",
        pull_fn=load_forecast_vintages.pull_combined_vintages,
        pull_kwargs={"source": "pjm"},
        **CACHE_KWARGS,
    )
    df_meteo = pull_with_cache(
        source_name="meteologica_load_forecast_vintages",
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
    df_pjm = pull_with_cache(
        source_name="pjm_solar_forecast_vintages",
        pull_fn=solar_forecast_vintages.pull_pjm_vintages,
        pull_kwargs={},
        **CACHE_KWARGS,
    )
    df_meteo = pull_with_cache(
        source_name="meteologica_solar_forecast_vintages",
        pull_fn=solar_forecast_vintages.pull_meteologica_vintages,
        pull_kwargs={},
        **CACHE_KWARGS,
    )
    df = pd.concat([df_pjm, df_meteo], ignore_index=True)
    vm = generation_view_model(df, forecast_type="solar")
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(content=format_generation_forecast_vintages(vm), media_type="text/markdown")


@app.get("/views/wind_forecast_vintages")
def get_wind_forecast_vintages(
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Return wind forecast vintage evolution across all regions for PJM and Meteologica."""
    df_pjm = pull_with_cache(
        source_name="pjm_wind_forecast_vintages",
        pull_fn=wind_forecast_vintages.pull_pjm_vintages,
        pull_kwargs={},
        **CACHE_KWARGS,
    )
    df_meteo = pull_with_cache(
        source_name="meteologica_wind_forecast_vintages",
        pull_fn=wind_forecast_vintages.pull_meteologica_vintages,
        pull_kwargs={},
        **CACHE_KWARGS,
    )
    df = pd.concat([df_pjm, df_meteo], ignore_index=True)
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

    start = date.today() - timedelta(days=7)
    hub = "WESTERN HUB"
    df_da = pull_with_cache(
        source_name="pjm_lmps_hourly_da",
        pull_fn=lmps_hourly.pull,
        pull_kwargs={"schema": configs.SCHEMA, "market": "da"},
        **CACHE_KWARGS,
    )
    df_da = df_da[(df_da["hub"] == hub) & (pd.to_datetime(df_da["date"]).dt.date >= start)]
    df_rt = pull_with_cache(
        source_name="pjm_lmps_hourly_rt",
        pull_fn=lmps_hourly.pull,
        pull_kwargs={"schema": configs.SCHEMA, "market": "rt"},
        **CACHE_KWARGS,
    )
    df_rt = df_rt[(df_rt["hub"] == hub) & (pd.to_datetime(df_rt["date"]).dt.date >= start)]
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

    start = date.today() - timedelta(days=7)
    df = pull_with_cache(
        source_name="pjm_fuel_mix_hourly",
        pull_fn=fuel_mix_hourly.pull,
        pull_kwargs={},
        **CACHE_KWARGS,
    )
    df = df[pd.to_datetime(df["date"]).dt.date >= start]
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
        source_name="pjm_outages_forecast_daily",
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
        source_name="pjm_transmission_outages",
        pull_fn=transmission_outages.pull,
        pull_kwargs={},
        **CACHE_KWARGS,
    )
    vm = tx_outage_view_model(df)
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(content=format_transmission_outages(vm), media_type="text/markdown")


@app.get("/views/regional_congestion")
def get_regional_congestion(
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Return DA/RT congestion pricing across PJM regional hubs (last 7 days)."""
    from datetime import date, timedelta

    start = date.today() - timedelta(days=7)
    df_da = pull_with_cache(
        source_name="pjm_lmps_hourly_da",
        pull_fn=lmps_hourly.pull,
        pull_kwargs={"schema": configs.SCHEMA, "market": "da"},
        **CACHE_KWARGS,
    )
    df_da = df_da[pd.to_datetime(df_da["date"]).dt.date >= start]
    df_rt = pull_with_cache(
        source_name="pjm_lmps_hourly_rt",
        pull_fn=lmps_hourly.pull,
        pull_kwargs={"schema": configs.SCHEMA, "market": "rt"},
        **CACHE_KWARGS,
    )
    df_rt = df_rt[pd.to_datetime(df_rt["date"]).dt.date >= start]
    vm = regional_congestion_view_model(df_da, df_rt)
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(content=format_regional_congestion(vm), media_type="text/markdown")


@app.get("/views/ice_power_intraday")
def get_ice_power_intraday(
    settle_lookback_days: int = Query(30, description="Lookback days for settlement history"),
    intraday_lookback_days: int = Query(3, description="Lookback days for intraday tape"),
    products: str | None = Query(None, description="Comma-separated product filter (e.g. 'NxtDay DA,NxtDay RT')"),
    delivery_date: str | None = Query(None, description="Filter to a single delivery date (YYYY-MM-DD)"),
    include_snapshots: bool = Query(False, description="Include full (compressed) intraday snapshot tape"),
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Return ICE PJM power settlements and intraday snapshot tape."""
    from datetime import date as _date

    df_settles = pull_with_cache(
        source_name="ice_power_settles",
        pull_fn=ice_power_intraday.pull_settles,
        pull_kwargs={"lookback_days": settle_lookback_days},
        **CACHE_KWARGS,
    )
    df_intraday = pull_with_cache(
        source_name="ice_power_intraday",
        pull_fn=ice_power_intraday.pull_intraday,
        pull_kwargs={"lookback_days": intraday_lookback_days},
        **CACHE_KWARGS,
    )

    product_list = [p.strip() for p in products.split(",")] if products else None
    dd = _date.fromisoformat(delivery_date) if delivery_date else None

    vm = ice_power_view_model(
        df_settles, df_intraday,
        products=product_list,
        delivery_date=dd,
        include_snapshots=include_snapshots,
    )
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(content=format_ice_power_intraday(vm), media_type="text/markdown")


@app.get("/views/meteologica_da_forecast")
def get_meteologica_da_forecast(
    forecast_date: str | None = Query(None, description="YYYY-MM-DD, defaults to tomorrow"),
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Return Meteologica DA price forecast for a single date in like-day output format."""
    from datetime import date, timedelta

    if forecast_date:
        target = pd.to_datetime(forecast_date).date()
    else:
        target = date.today() + timedelta(days=1)

    df = pull_with_cache(
        source_name="meteologica_da_price_forecast",
        pull_fn=meteologica_da_price_forecast.pull,
        pull_kwargs={},
        **CACHE_KWARGS,
    )
    vm = meteo_da_view_model(df, forecast_date=target)
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(content=format_meteologica_da_forecast(vm), media_type="text/markdown")


@app.get("/views/lasso_qr_forecast_results")
def get_lasso_qr_forecast_results(
    forecast_date: str | None = Query(None, description="YYYY-MM-DD, defaults to tomorrow"),
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Run LASSO Quantile Regression forecast and return structured view model."""
    from src.lasso_quantile_regression.pipelines.forecast import run as run_lasso_qr
    from src.lasso_quantile_regression.configs import LassoQRConfig
    from src.views.lasso_qr_forecast_results import build_view_model as lasso_qr_view

    config = LassoQRConfig(forecast_date=forecast_date)
    result = run_lasso_qr(config=config)
    vm = lasso_qr_view(result)
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(
        content=format_lasso_qr_forecast_results(vm), media_type="text/markdown",
    )


@app.get("/views/lasso_qr_strip_forecast_results")
def get_lasso_qr_strip_forecast_results(
    horizon: int = Query(3, description="Number of days ahead to forecast (1-7)"),
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Run LASSO QR strip forecast (D+1 through D+N) and return structured view model."""
    from src.lasso_quantile_regression.pipelines.strip_forecast import run_strip as run_lasso_strip
    from src.lasso_quantile_regression.configs import LassoQRConfig
    from src.views.lasso_qr_strip_forecast_results import build_view_model as lasso_strip_view

    config = LassoQRConfig()
    result = run_lasso_strip(horizon=min(horizon, 7), config=config)
    vm = lasso_strip_view(result)
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(
        content=format_lasso_qr_strip_forecast_results(vm), media_type="text/markdown",
    )


@app.get("/views/lgbm_qr_forecast_results")
def get_lgbm_qr_forecast_results(
    forecast_date: str | None = Query(None, description="YYYY-MM-DD, defaults to tomorrow"),
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Run LightGBM Quantile Regression forecast and return structured view model."""
    from src.lightgbm_quantile.pipelines.forecast import run as run_lgbm_qr
    from src.lightgbm_quantile.configs import LGBMQRConfig
    from src.views.lgbm_qr_forecast_results import build_view_model as lgbm_qr_view

    config = LGBMQRConfig(forecast_date=forecast_date)
    result = run_lgbm_qr(config=config)
    vm = lgbm_qr_view(result)
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(
        content=format_lgbm_qr_forecast_results(vm),
        media_type="text/markdown",
    )


@app.get("/views/lgbm_qr_strip_forecast_results")
def get_lgbm_qr_strip_forecast_results(
    horizon: int = Query(3, description="Number of days ahead to forecast (1-7)"),
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Run LightGBM QR strip forecast (D+1 through D+N) and return structured view model."""
    from src.lightgbm_quantile.pipelines.strip_forecast import run_strip as run_lgbm_strip
    from src.lightgbm_quantile.configs import LGBMQRConfig
    from src.views.lgbm_qr_strip_forecast_results import build_view_model as lgbm_strip_view

    config = LGBMQRConfig()
    result = run_lgbm_strip(horizon=min(horizon, 7), config=config)
    vm = lgbm_strip_view(result)
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(
        content=format_lgbm_qr_strip_forecast_results(vm),
        media_type="text/markdown",
    )


@app.get("/views/gas_prices")
def get_gas_prices(
    lookback_days: int = Query(7, description="Number of recent trading days to include"),
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Return ICE next-day cash gas prices for PJM-relevant hubs (M3, HH, Z5S, AGT)."""
    df = pull_with_cache(
        source_name="ice_gas_prices",
        pull_fn=gas_prices.pull,
        pull_kwargs={},
        **CACHE_KWARGS,
    )
    vm = gas_prices_view_model(df, lookback_days=lookback_days)
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(content=format_gas_prices(vm), media_type="text/markdown")


# ── MCP integration — exposes all endpoints as agent tools ──────────
mcp = FastApiMCP(app)
mcp.mount_http()
