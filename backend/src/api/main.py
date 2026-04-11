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
from src.like_day_forecast.pipelines.market_adjusted_forecast import run as run_adjusted_forecast
from src.like_day_forecast.pipelines.regression_adjusted_forecast import run as run_regression_forecast
from src.data import outages_actual_daily, outages_forecast_daily, load_forecast_vintages, solar_forecast_vintages, wind_forecast_vintages, lmps_hourly, fuel_mix_hourly, transmission_outages, ice_power_intraday, meteologica_da_price_forecast
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
from src.views.ice_power_intraday import build_view_model as ice_power_view_model
from src.views.meteologica_da_forecast import build_view_model as meteo_da_view_model
from src.views.regional_congestion import build_view_model as regional_congestion_view_model
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


@app.get("/views/market_adjusted_forecast")
def get_market_adjusted_forecast(
    market_onpeak: float = Query(..., description="ICE or broker on-peak price anchor ($/MWh)"),
    market_offpeak: float | None = Query(None, description="Off-peak anchor ($/MWh). If omitted, shifted by same delta as on-peak."),
    forecast_date: str | None = Query(None, description="YYYY-MM-DD, defaults to tomorrow"),
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Rescale the like-day forecast hourly shape to a market-observed price level."""
    result = run_adjusted_forecast(
        market_onpeak=market_onpeak,
        market_offpeak=market_offpeak,
        forecast_date=forecast_date,
        config=configs.ScenarioConfig(forecast_date=forecast_date),
        cache_dir=configs.CACHE_DIR,
        cache_enabled=configs.CACHE_ENABLED,
        cache_ttl_hours=configs.CACHE_TTL_HOURS,
        force_refresh=configs.FORCE_CACHE_REFRESH,
    )
    vm = forecast_view_model(result)
    if format == OutputFormat.json:
        return {**vm, "adjustment": result.get("adjustment")}
    md = format_like_day_forecast_results(vm)
    adj = result.get("adjustment", {})
    header = (
        f"# Market-Adjusted Forecast\n\n"
        f"**Anchor:** On-Peak ${adj.get('market_onpeak', 0):.2f} | "
        f"Off-Peak ${adj.get('market_offpeak', 0):.2f}\n"
        f"**Model base:** On-Peak ${adj.get('base_onpeak', 0):.2f} | "
        f"Off-Peak ${adj.get('base_offpeak', 0):.2f}\n"
        f"**Delta:** On-Peak {adj.get('onpeak_delta', 0):+.2f} | "
        f"Off-Peak {adj.get('offpeak_delta', 0):+.2f}\n\n"
    )
    return PlainTextResponse(content=header + md, media_type="text/markdown")


@app.get("/views/regression_adjusted_forecast")
def get_regression_adjusted_forecast(
    forecast_date: str | None = Query(None, description="YYYY-MM-DD, defaults to tomorrow"),
    exclude_dates: str | None = Query(None, description="Comma-separated YYYY-MM-DD dates to exclude from analog pool"),
    exclude_holidays: bool = Query(True, description="Exclude NERC holidays from analog pool when target is not a holiday"),
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Apply regression correction based on fundamental deltas vs analog-weighted averages."""
    parsed_exclude = [d.strip() for d in exclude_dates.split(",") if d.strip()] if exclude_dates else []
    result = run_regression_forecast(
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
        adj = result.get("adjustment", {})
        deltas_json = [
            {
                "factor": d.label, "unit": d.unit,
                "today": d.today_value, "analog_avg": d.analog_avg,
                "delta": d.delta,
                "adj_onpeak": d.adj_onpeak, "adj_offpeak": d.adj_offpeak,
            }
            for d in result.get("deltas", [])
        ]
        return {**vm, "adjustment": adj, "deltas": deltas_json}
    # Markdown
    adj = result.get("adjustment", {})
    deltas = result.get("deltas", [])
    header = (
        f"# Regression-Adjusted Forecast\n\n"
        f"**Total adjustment:** On-Peak {adj.get('total_onpeak', 0):+.2f} | "
        f"Off-Peak {adj.get('total_offpeak', 0):+.2f}\n"
        f"**Model:** On-Peak ${adj.get('base_onpeak', 0):.2f} | "
        f"Off-Peak ${adj.get('base_offpeak', 0):.2f}\n"
        f"**Adjusted:** On-Peak ${adj.get('adj_onpeak', 0):.2f} | "
        f"Off-Peak ${adj.get('adj_offpeak', 0):.2f}\n\n"
        f"## Fundamental Deltas\n"
        f"| Factor | Today | Analog Avg | Delta | Adj OnPk | Adj OffPk |\n"
        f"|--------|-------|-----------|-------|----------|----------|\n"
    )
    for d in deltas:
        if d.unit == "MW":
            header += (f"| {d.label} | {d.today_value:,.0f} MW | {d.analog_avg:,.0f} MW | "
                       f"{d.delta:+,.0f} | {d.adj_onpeak:+.2f} | {d.adj_offpeak:+.2f} |\n")
        else:
            header += (f"| {d.label} | ${d.today_value:.2f} | ${d.analog_avg:.2f} | "
                       f"{d.delta:+.2f} | {d.adj_onpeak:+.2f} | {d.adj_offpeak:+.2f} |\n")
    header += f"| **Total** | | | | **{adj.get('total_onpeak', 0):+.2f}** | **{adj.get('total_offpeak', 0):+.2f}** |\n\n"
    md = format_like_day_forecast_results(vm)
    return PlainTextResponse(content=header + md, media_type="text/markdown")


@app.get("/views/scenario_forecast")
def get_scenario_forecast(
    scenarios: str = Query(
        "holiday,high_outage_stress,low_wind",
        description="Comma-separated scenario preset names (e.g., 'holiday,low_wind,bull,bear')",
    ),
    forecast_date: str | None = Query(None, description="YYYY-MM-DD, defaults to tomorrow"),
    exclude_dates: str | None = Query(None, description="Comma-separated YYYY-MM-DD dates to exclude from analog pool"),
    exclude_holidays: bool = Query(True, description="Exclude NERC holidays from analog pool when target is not a holiday"),
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Run multiple what-if scenarios and return a comparison table."""
    from src.like_day_forecast.pipelines.scenario_forecast import (
        format_comparison_markdown,
        run as run_scenarios,
    )

    scenario_list = [s.strip() for s in scenarios.split(",") if s.strip()]
    parsed_exclude = [d.strip() for d in exclude_dates.split(",") if d.strip()] if exclude_dates else []
    result = run_scenarios(
        scenarios=scenario_list,
        forecast_date=forecast_date,
        base_config=configs.ScenarioConfig(
            forecast_date=forecast_date,
            exclude_dates=parsed_exclude,
            exclude_holidays=exclude_holidays,
        ),
        cache_dir=configs.CACHE_DIR,
        cache_enabled=configs.CACHE_ENABLED,
        cache_ttl_hours=configs.CACHE_TTL_HOURS,
        force_refresh=configs.FORCE_CACHE_REFRESH,
    )

    if format == OutputFormat.json:
        comparison = result["comparison_table"].to_dict(orient="records")
        per_scenario_summary = {}
        for name, res in result["per_scenario"].items():
            adj = res.get("adjustment", {})
            per_scenario_summary[name] = {
                "model_onpeak": adj.get("base_onpeak"),
                "model_offpeak": adj.get("base_offpeak"),
                "onpeak": adj.get("adj_onpeak"),
                "offpeak": adj.get("adj_offpeak"),
                "total_adj_onpeak": adj.get("total_onpeak"),
                "total_adj_offpeak": adj.get("total_offpeak"),
                "n_analogs": res.get("n_analogs_used"),
                "deltas": [
                    {
                        "factor": d.label,
                        "delta": d.delta,
                        "unit": d.unit,
                        "adj_onpeak": d.adj_onpeak,
                        "adj_offpeak": d.adj_offpeak,
                    }
                    for d in res.get("deltas", [])
                ],
                "overrides": res.get("fundamental_overrides_applied", {}),
            }
        return {
            "forecast_date": result["forecast_date"],
            "scenarios_run": result["scenarios_run"],
            "comparison": comparison,
            "per_scenario": per_scenario_summary,
        }

    md = format_comparison_markdown(result)
    return PlainTextResponse(content=md, media_type="text/markdown")


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
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Return ICE PJM power settlements and intraday snapshot tape."""
    df_settles = pull_with_cache(
        source_name="ice_power_settles",
        pull_fn=ice_power_intraday.pull_settles,
        pull_kwargs={"lookback_days": 30},
        **CACHE_KWARGS,
    )
    df_intraday = pull_with_cache(
        source_name="ice_power_intraday",
        pull_fn=ice_power_intraday.pull_intraday,
        pull_kwargs={"lookback_days": 3},
        **CACHE_KWARGS,
    )
    vm = ice_power_view_model(df_settles, df_intraday)
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


# ── MCP integration — exposes all endpoints as agent tools ──────────
mcp = FastApiMCP(app)
mcp.mount_http()
