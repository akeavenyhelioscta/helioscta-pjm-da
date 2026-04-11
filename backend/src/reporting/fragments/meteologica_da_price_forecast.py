"""Meteologica DA price forecast dashboard — all available forecast dates.

Sections:
  1. Price Summary  — one row per available date, HE1-24 + OnPeak/OffPeak/Flat
  2. Price Profile   — overlaid hourly lines, one trace per date
"""
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go

from src.like_day_forecast import configs
from src.data import meteologica_da_price_forecast
from src.utils.cache_utils import pull_with_cache

logger = logging.getLogger(__name__)

PLOTLY_TEMPLATE = "plotly_dark"
ONPEAK_HOURS = list(range(8, 24))       # HE 8-23
OFFPEAK_HOURS = list(range(1, 8)) + [24]  # HE 1-7, 24

DAY_COLORS = [
    "#60a5fa",  # blue
    "#f87171",  # red
    "#34d399",  # green
    "#fbbf24",  # amber
    "#a78bfa",  # purple
    "#fb923c",  # orange
    "#e879f9",  # pink
    "#2dd4bf",  # teal
    "#f472b6",  # rose
    "#38bdf8",  # sky
]

Section = tuple[str, Any, str | None]


# ── Public entry point ───────────────────────────────────────────────


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list:
    """Pull Meteologica DA price forecast and return report sections for all available days."""
    logger.info("Building Meteologica DA price forecast report fragments...")

    cache_kwargs = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    df = pull_with_cache(
        source_name="meteologica_da_price_forecast",
        pull_fn=meteologica_da_price_forecast.pull,
        pull_kwargs={},
        **cache_kwargs,
    )

    if df.empty:
        return [("Meteologica DA Price Forecast", _empty("No forecast data available."), None)]

    df = df.sort_values(["forecast_date", "hour_ending"])
    dates = sorted(df["forecast_date"].unique())

    if not dates:
        return [("Meteologica DA Price Forecast", _empty("No forecast dates available."), None)]

    date_min = dates[0]
    date_max = dates[-1]
    range_label = f"{date_min} to {date_max}" if len(dates) > 1 else str(date_min)

    # Execution timestamps per date
    exec_info = _extract_exec_info(df)

    sections: list = []

    sections.append((
        f"Price Forecast — {range_label}",
        _build_summary_table(df, dates, exec_info),
        None,
    ))

    sections.append((
        "Price Profile — All Dates",
        _build_price_chart(df, dates, range_label, exec_info),
        None,
    ))

    return sections


# ── Execution info extraction ──────────────────────────────────────


def _extract_exec_info(df: pd.DataFrame) -> dict:
    """Extract per-date execution timestamps and a global latest.

    Returns:
        Dict with 'per_date' mapping forecast_date → exec timestamp string,
        and 'latest' with the overall latest execution timestamp.
    """
    info: dict = {"per_date": {}, "latest": None}
    if "forecast_execution_datetime" not in df.columns:
        return info

    for dt in df["forecast_date"].unique():
        day_df = df[df["forecast_date"] == dt]
        latest_exec = pd.to_datetime(day_df["forecast_execution_datetime"]).max()
        if pd.notna(latest_exec):
            info["per_date"][dt] = latest_exec.strftime("%a %b %d, %H:%M")

    all_exec = pd.to_datetime(df["forecast_execution_datetime"]).max()
    if pd.notna(all_exec):
        info["latest"] = all_exec.strftime("%a %b %d, %Y %H:%M EPT")

    return info


# ── Summary Table ────────────────────────────────────────────────────


def _build_summary_table(df: pd.DataFrame, dates: list, exec_info: dict) -> str:
    """One row per date, HE1-24 + OnPeak/OffPeak/Flat, with execution timestamp column."""
    html = '<div style="overflow-x:auto;padding:8px 0;">\n'

    # Global execution badge
    if exec_info.get("latest"):
        html += (
            f'<div style="padding:4px 8px;margin-bottom:8px;">'
            f'<span style="font-size:11px;color:#6f8db1;font-weight:600;">Latest Execution:</span> '
            f'<span style="font-size:12px;font-family:monospace;color:#dbe7ff;">{exec_info["latest"]}</span>'
            f'</div>\n'
        )

    html += (
        '<table style="width:100%;border-collapse:collapse;font-size:11px;'
        "font-family:'IBM Plex Sans',monospace;\">\n"
    )

    # Header
    html += '<tr style="border-bottom:1px solid #2a3f60;">'
    html += _th("Date", align="left")
    html += _th("Run At", align="left")
    for he in range(1, 25):
        html += _th(f"HE{he}")
    html += _th("OnPeak")
    html += _th("OffPeak")
    html += _th("Flat")
    html += "</tr>\n"

    # One row per date
    for idx, dt in enumerate(dates):
        day_df = df[df["forecast_date"] == dt]
        prices = day_df.set_index("hour_ending")["forecast_da_price"]
        color = DAY_COLORS[idx % len(DAY_COLORS)]
        dt_label = pd.Timestamp(dt).strftime("%a %m/%d")
        exec_ts = exec_info["per_date"].get(dt, "—")

        bg = f"rgba({_hex_to_rgb(color)}, 0.06)" if idx % 2 == 0 else "transparent"
        html += f'<tr style="border-bottom:1px solid #1a2a42;background:{bg};">'
        html += (
            f'<td style="padding:4px 8px;font-weight:600;color:{color};'
            f'white-space:nowrap;">{dt_label}</td>'
        )
        html += (
            f'<td style="padding:4px 8px;color:#6f8db1;font-size:10px;'
            f'white-space:nowrap;">{exec_ts}</td>'
        )

        vals = []
        for he in range(1, 25):
            val = prices.get(he, None)
            if val is not None and not pd.isna(val):
                cell_color = _price_color(val)
                html += (
                    f'<td style="padding:4px 6px;text-align:right;color:{cell_color};">'
                    f"${val:.2f}</td>"
                )
                vals.append(val)
            else:
                html += '<td style="padding:4px 6px;text-align:right;color:#3a4a60;">&mdash;</td>'

        onpeak = [prices.get(h) for h in ONPEAK_HOURS
                  if h in prices.index and not pd.isna(prices.get(h))]
        offpeak = [prices.get(h) for h in OFFPEAK_HOURS
                   if h in prices.index and not pd.isna(prices.get(h))]

        for agg in [onpeak, offpeak, vals]:
            if agg:
                avg = sum(agg) / len(agg)
                cell_color = _price_color(avg)
                html += (
                    f'<td style="padding:4px 8px;text-align:right;font-weight:600;'
                    f'color:{cell_color};">${avg:.2f}</td>'
                )
            else:
                html += (
                    '<td style="padding:4px 8px;text-align:right;'
                    'color:#3a4a60;">&mdash;</td>'
                )

        html += "</tr>\n"

    html += "</table>\n</div>\n"
    return html


# ── Price Profile Chart ──────────────────────────────────────────────


def _build_price_chart(df: pd.DataFrame, dates: list, range_label: str, exec_info: dict) -> str:
    """Overlaid hourly price profiles — one trace per date, with execution time in legend."""
    fig = go.Figure()

    for idx, dt in enumerate(dates):
        day_df = df[df["forecast_date"] == dt].sort_values("hour_ending")
        color = DAY_COLORS[idx % len(DAY_COLORS)]
        dt_label = pd.Timestamp(dt).strftime("%a %m/%d")

        exec_ts = exec_info["per_date"].get(dt)
        legend_label = f"{dt_label} (run {exec_ts})" if exec_ts else dt_label

        fig.add_trace(go.Scatter(
            x=day_df["hour_ending"].values,
            y=day_df["forecast_da_price"].values,
            mode="lines+markers",
            name=legend_label,
            line=dict(color=color, width=2.5),
            marker=dict(size=5),
            hovertemplate=f"{dt_label}<br>HE %{{x}}<br>${{y:.2f}}/MWh<extra></extra>",
        ))

    # On-peak shading
    fig.add_vrect(
        x0=7.5, x1=23.5,
        fillcolor="rgba(251, 191, 36, 0.06)",
        line_width=0,
        annotation_text="On-Peak",
        annotation_position="top left",
        annotation_font=dict(size=10, color="#6f8db1"),
    )

    fig.update_layout(
        title=f"Meteologica DA Price Forecast — {range_label}",
        xaxis_title="Hour Ending",
        yaxis_title="$/MWh",
        height=500,
        template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="top", y=-0.10, xanchor="left", x=0),
        margin=dict(l=60, r=40, t=40, b=70),
        hovermode="x unified",
    )
    fig.update_xaxes(dtick=1, range=[0.5, 24.5])

    return fig.to_html(include_plotlyjs="cdn", full_html=False, div_id="meteo-da-price")


# ── Helpers ──────────────────────────────────────────────────────────


def _th(text: str, align: str = "right") -> str:
    return (
        f'<th style="padding:4px 6px;text-align:{align};color:#f0b429;'
        f'font-weight:600;white-space:nowrap;">{text}</th>'
    )


def _price_color(val: float) -> str:
    if val < 0:
        return "#ef4444"
    if val > 100:
        return "#f59e0b"
    return "#dbe7ff"


def _hex_to_rgb(hex_color: str) -> str:
    """Convert '#60a5fa' → '96, 165, 250'."""
    h = hex_color.lstrip("#")
    return f"{int(h[0:2], 16)}, {int(h[2:4], 16)}, {int(h[4:6], 16)}"


def _empty(text: str) -> str:
    return f'<div style="padding:16px;color:#e74c3c;font-size:14px;">{text}</div>'
