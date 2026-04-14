"""Like-Day ICE Level Adjustment forecast report fragment.

Runs the ICE level-adjusted pipeline and reuses the base like-day report
sections, adding ICE-specific context cards (settle price, scale factor,
pre/post on-peak average).

Sections:
  1. ICE Adjustment Context — cards showing ICE session info and scaling
  2. Analog Days            — table of analog days (reused from base fragment)
  3. Quantile Bands         — pivoted band table with Override (reused)
  4. Quantile Band Chart    — Plotly chart (reused)
"""
import logging
from pathlib import Path
from typing import Any

from src.like_day_forecast import configs
from src.like_day_forecast.pipelines.forecast import run as run_forecast
from src.reporting.fragments.like_day_forecast_results import (
    _analog_days_table_html,
    _quartile_bands_table_html,
    _quartile_bands_plot_html,
    _error_html,
)

logger = logging.getLogger(__name__)

Section = tuple[str, Any, str | None]


# ── Public entry point ───────────────────────────────────────────────


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list[Section]:
    """Run ICE level-adjusted forecast and return report sections."""
    logger.info("Building ICE level-adjusted forecast report...")

    result = run_forecast(
        forecast_date=None,
        config=configs.ScenarioConfig(
            name="ice_level",
            include_ice_forward=False,
            ice_level_adjustment=True,
            schema=schema,
        ),
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        cache_ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    if "error" in result:
        return [("Forecast Error", _error_html(result["error"]), None)]

    output_table = result["output_table"]
    quantiles_table = result["quantiles_table"]
    df_forecast = result["df_forecast"]
    has_actuals = result["has_actuals"]
    forecast_date = result["forecast_date"]
    reference_date = result["reference_date"]
    analogs_df = result.get("analogs")
    ice_info = result.get("ice_info")

    sections: list[Section] = []

    # 1. ICE adjustment context cards
    sections.append((
        f"ICE Level Adjustment — {forecast_date}",
        _ice_context_html(ice_info, forecast_date),
        None,
    ))

    # 2. Analog days table
    if analogs_df is not None and len(analogs_df) > 0:
        sections.append((
            f"Analog Days — {forecast_date}",
            _analog_days_table_html(analogs_df, forecast_date, reference_date),
            None,
        ))

    # 3. Quantile bands table (with editable Override + Ovr-Fcst diff)
    sections.append((
        f"Quantile Bands (ICE Adjusted) — {forecast_date}",
        _quartile_bands_table_html(
            quantiles_table, output_table, has_actuals,
            prefix="qb-ice", plot_id="qb-ice-plot",
        ),
        None,
    ))

    # 4. Quantile bands plot
    sections.append((
        "Quantile Band Chart (ICE Adjusted)",
        _quartile_bands_plot_html(
            df_forecast, output_table, has_actuals,
            plot_id="qb-ice-plot",
        ),
        None,
    ))

    return sections


# ── ICE context cards ────────────────────────────────────────────────


def _ice_context_html(ice_info: dict | None, forecast_date: str) -> str:
    """Render ICE session info and scaling context as summary cards."""
    card = (
        "background:#111d31;border:1px solid #253b59;border-radius:10px;"
        "padding:12px 16px;min-width:140px;flex:1;"
    )
    label_s = (
        "font-size:10px;font-weight:600;color:#6f8db1;"
        "text-transform:uppercase;letter-spacing:0.5px;"
    )
    val_s = (
        "font-size:18px;font-weight:700;color:#dbe7ff;"
        "font-family:'Space Grotesk',monospace;margin-top:2px;"
    )

    html = '<div style="padding:12px 8px 4px 8px;">'
    html += '<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:12px;">'

    if ice_info is None:
        html += (
            f'<div style="{card}">'
            f'<div style="{label_s}">Status</div>'
            f'<div style="{val_s}color:#e74c3c;">No ICE data available</div>'
            f'</div>'
        )
        html += '</div></div>'
        return html

    def _fmt_price(v):
        return f"${v:.2f}" if v is not None else "n/a"

    cards = [
        ("Forecast Date", str(forecast_date)),
        ("ICE On-Peak Price", _fmt_price(ice_info.get("ice_onpeak_price"))),
        ("Scale Factor", f"{ice_info.get('scale_factor', 'n/a')}"),
        ("Pre-Adj On-Peak Avg", _fmt_price(ice_info.get("onpeak_before_adjustment"))),
        ("ICE Settle", _fmt_price(ice_info.get("ice_settle"))),
        ("ICE High", _fmt_price(ice_info.get("ice_high"))),
        ("ICE Low", _fmt_price(ice_info.get("ice_low"))),
        ("ICE WAP", _fmt_price(ice_info.get("ice_wap"))),
    ]

    for lbl, val in cards:
        html += (
            f'<div style="{card}">'
            f'<div style="{label_s}">{lbl}</div>'
            f'<div style="{val_s}">{val}</div>'
            f'</div>'
        )

    html += '</div></div>'
    return html
