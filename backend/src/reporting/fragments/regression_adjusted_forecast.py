"""Regression-adjusted forecast report — fundamental delta corrections.

Sections:
  1. Fundamental Deltas  — Table of each factor, today vs analog avg, sensitivity, adjustment
  2. Adjusted Quantile Bands  — Shifted band table with Model + Regression Adj rows
  3. Comparison Chart — Plotly: model bands + regression-adjusted overlay
"""
import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from src.like_day_forecast import configs
from src.like_day_forecast.pipelines.regression_adjusted_forecast import run as run_regression

logger = logging.getLogger(__name__)

PLOTLY_TEMPLATE = "plotly_dark"
HE_COLS = [f"HE{h}" for h in range(1, 25)]
SUMMARY_COLS = ["OnPeak", "OffPeak", "Flat"]
ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = list(range(1, 8)) + [24]
Section = tuple[str, Any, str | None]


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list[Section]:
    """Run regression-adjusted forecast and return report sections."""
    logger.info("Building regression-adjusted forecast report...")

    result = run_regression(
        forecast_date=None,
        config=configs.ScenarioConfig(schema=schema),
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        cache_ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    if "error" in result:
        return [("Regression Adj Error", _error_html(result["error"]), None)]

    adj = result["adjustment"]
    deltas = result["deltas"]
    forecast_date = result["forecast_date"]
    sections: list[Section] = []

    # 1. Fundamental deltas waterfall
    sections.append((
        f"Fundamental Deltas — {forecast_date}",
        _deltas_table_html(deltas, adj),
        None,
    ))

    # 2. Adjusted quantile bands table
    sections.append((
        f"Regression-Adjusted Bands — {forecast_date}",
        _adjusted_bands_table_html(result),
        None,
    ))

    # 3. Comparison chart
    sections.append((
        "Model vs Regression-Adjusted Chart",
        _comparison_chart_html(result),
        None,
    ))

    return sections


# ── Section 1: Fundamental Deltas Table ──────────────────────────────


def _deltas_table_html(deltas: list, adj: dict) -> str:
    """Waterfall-style table showing each fundamental's contribution."""
    html = '<div style="padding:12px;">'

    # Summary card
    total_on = adj["total_onpeak"]
    total_off = adj["total_offpeak"]
    total_flat = (total_on * 16 + total_off * 8) / 24
    sign_color = "#EF553B" if total_on > 0 else "#00CC96"

    html += (
        f'<div style="display:flex;gap:24px;margin-bottom:16px;flex-wrap:wrap;">'
        f'<div style="background:#16263d;padding:12px 20px;border-radius:6px;'
        f'border-left:4px solid {sign_color};">'
        f'<div style="color:#8ea8c4;font-size:11px;">Total On-Peak Adj</div>'
        f'<div style="color:{sign_color};font-size:22px;font-weight:700;font-family:monospace;">'
        f'{total_on:+.2f} $/MWh</div></div>'
        f'<div style="background:#16263d;padding:12px 20px;border-radius:6px;'
        f'border-left:4px solid {sign_color};">'
        f'<div style="color:#8ea8c4;font-size:11px;">Total Off-Peak Adj</div>'
        f'<div style="color:{sign_color};font-size:22px;font-weight:700;font-family:monospace;">'
        f'{total_off:+.2f} $/MWh</div></div>'
        f'<div style="background:#16263d;padding:12px 20px;border-radius:6px;'
        f'border-left:4px solid {sign_color};">'
        f'<div style="color:#8ea8c4;font-size:11px;">Adj On-Peak Price</div>'
        f'<div style="color:#FFA15A;font-size:22px;font-weight:700;font-family:monospace;">'
        f'${adj["adj_onpeak"]:.2f}</div></div>'
        f'</div>'
    )

    # Detail table
    headers = [
        "Factor", "Today", "Analog Avg", "Delta", "Unit",
        "Sens (OnPk)", "Sens (OffPk)", "Adj OnPk", "Adj OffPk",
    ]

    html += (
        '<table style="border-collapse:collapse;font-size:12px;font-family:monospace;'
        'min-width:800px;width:100%;">'
        '<thead><tr>'
    )
    for h in headers:
        align = "text-align:left;" if h == "Factor" else "text-align:right;"
        html += (
            f'<th style="padding:8px 10px;background:#16263d;color:#e6efff;'
            f'font-size:11px;{align}">{h}</th>'
        )
    html += '</tr></thead><tbody>'

    for d in deltas:
        delta_color = "#EF553B" if d.adj_onpeak > 0.1 else "#00CC96" if d.adj_onpeak < -0.1 else "#8ea8c4"

        # Format based on unit type
        if d.unit == "MW":
            today_fmt = f"{d.today_value:,.0f}"
            analog_fmt = f"{d.analog_avg:,.0f}"
            delta_fmt = f"{d.delta:+,.0f}"
        else:
            today_fmt = f"${d.today_value:.2f}"
            analog_fmt = f"${d.analog_avg:.2f}"
            delta_fmt = f"{d.delta:+.2f}"

        html += (
            f'<tr style="border-bottom:1px solid #1e3350;">'
            f'<td style="padding:6px 10px;color:#dbe7ff;font-weight:600;">{d.label}</td>'
            f'<td style="padding:6px 10px;color:#8ea8c4;text-align:right;">{today_fmt}</td>'
            f'<td style="padding:6px 10px;color:#8ea8c4;text-align:right;">{analog_fmt}</td>'
            f'<td style="padding:6px 10px;color:{delta_color};text-align:right;font-weight:600;">'
            f'{delta_fmt}</td>'
            f'<td style="padding:6px 10px;color:#556;text-align:right;">{d.unit}</td>'
            f'<td style="padding:6px 10px;color:#556;text-align:right;">'
            f'{d.sensitivity_onpeak:+.2f}</td>'
            f'<td style="padding:6px 10px;color:#556;text-align:right;">'
            f'{d.sensitivity_offpeak:+.2f}</td>'
            f'<td style="padding:6px 10px;color:{delta_color};text-align:right;font-weight:700;">'
            f'{d.adj_onpeak:+.2f}</td>'
            f'<td style="padding:6px 10px;color:{delta_color};text-align:right;font-weight:700;">'
            f'{d.adj_offpeak:+.2f}</td>'
            f'</tr>'
        )

    # Total row
    html += (
        f'<tr style="border-top:2px solid #3a5070;background:rgba(14,25,42,0.6);">'
        f'<td style="padding:8px 10px;color:#FFA15A;font-weight:700;" colspan="7">Total Adjustment</td>'
        f'<td style="padding:8px 10px;color:#FFA15A;text-align:right;font-weight:700;">'
        f'{total_on:+.2f}</td>'
        f'<td style="padding:8px 10px;color:#FFA15A;text-align:right;font-weight:700;">'
        f'{total_off:+.2f}</td>'
        f'</tr>'
    )

    html += '</tbody></table></div>'
    return html


# ── Section 2: Adjusted Bands Table ──────────────────────────────────


def _adjusted_bands_table_html(result: dict) -> str:
    """Regression-adjusted band table with Model + Adj rows."""
    adj_quantiles = result["quantiles_table"]
    output_table = result["output_table"]
    base_output = result["base_output_table"]
    adj_info = result["adjustment"]

    cols = ["Date", "Band"] + HE_COLS + SUMMARY_COLS
    tid = "regadj"

    display_rows = []
    for _, row in adj_quantiles.iterrows():
        r = {"Date": row["Date"], "Band": row["Type"]}
        for c in HE_COLS + SUMMARY_COLS:
            r[c] = row[c]
        display_rows.append(r)

    # Model row
    fcst_row = base_output[base_output["Type"] == "Forecast"].iloc[0]
    r = {"Date": fcst_row["Date"], "Band": "Model"}
    for c in HE_COLS + SUMMARY_COLS:
        r[c] = fcst_row[c]
    display_rows.append(r)

    # Regression Adj row
    adj_rows = output_table[output_table["Type"] == "Regression Adj"]
    if len(adj_rows) > 0:
        adj_row = adj_rows.iloc[0]
        r = {"Date": adj_row["Date"], "Band": "Regression Adj"}
        for c in HE_COLS + SUMMARY_COLS:
            r[c] = adj_row[c]
        display_rows.append(r)

    has_actuals = result.get("has_actuals", False)
    if has_actuals:
        actual_rows = output_table[output_table["Type"] == "Actual"]
        if len(actual_rows) > 0:
            actual_row = actual_rows.iloc[0]
            r = {"Date": actual_row["Date"], "Band": "Actual"}
            for c in HE_COLS + SUMMARY_COLS:
                r[c] = actual_row[c]
            display_rows.append(r)

    band_row_colors = {
        "Model": "#8ea8c4",
        "Regression Adj": "#FFA15A",
        "Actual": "#4cc9f0",
    }
    band_row_bg = {
        "Model": "rgba(142, 168, 196, 0.08)",
        "Regression Adj": "rgba(255, 161, 90, 0.10)",
        "Actual": "rgba(76, 201, 240, 0.08)",
    }

    html = '<div style="overflow-x:auto;padding:8px;">'
    html += (
        f'<table id="{tid}" style="width:100%;border-collapse:collapse;'
        f'font-size:12px;font-family:monospace;">'
    )

    html += "<thead><tr>"
    for col in cols:
        align = "text-align:left;" if col in ("Date", "Band") else ""
        html += (
            f'<th style="padding:6px 8px;background:#16263d;color:#e6efff;'
            f'text-align:right;font-size:11px;position:sticky;top:0;{align}">{col}</th>'
        )
    html += "</tr></thead><tbody>"

    quantile_idx = 0
    for row in display_rows:
        band = row["Band"]
        label_color = band_row_colors.get(band, "#a6bad6")
        is_special = band in band_row_colors

        tr_parts = []
        if band == "Model":
            tr_parts.append("border-top:3px solid #3a5070")
        if band in band_row_bg:
            tr_parts.append(f"background:{band_row_bg[band]}")
        elif band not in band_row_colors:
            bg = "rgba(22, 38, 61, 0.5)" if quantile_idx % 2 == 0 else "transparent"
            tr_parts.append(f"background:{bg}")
            quantile_idx += 1
        tr_style = ";".join(tr_parts) + ";" if tr_parts else ""

        html += f'<tr style="{tr_style}">'
        for col in cols:
            val = row[col]
            style = "padding:5px 8px;border-bottom:1px solid #1e3350;text-align:right;"
            if col in ("Date", "Band"):
                style += f"text-align:left;font-weight:600;color:{label_color};"
                html += f'<td style="{style}">{val}</td>'
            elif pd.notna(val):
                cell_color = label_color if is_special else "#dbe7ff"
                html += f'<td style="{style}color:{cell_color};">{val:.1f}</td>'
            else:
                html += f'<td style="{style}color:#556;">\u2014</td>'
        html += "</tr>"

    # Delta row
    total_on = adj_info["total_onpeak"]
    total_off = adj_info["total_offpeak"]
    total_flat = (total_on * 16 + total_off * 8) / 24

    html += (
        f'<tr><td colspan="{len(cols)}" style="padding:0;height:3px;'
        f'background:linear-gradient(90deg,#4a6a8a 0%,#4a6a8a60 100%);'
        f'border:none;"></td></tr>'
    )
    html += f'<tr style="background:rgba(14, 25, 42, 0.6);">'
    for col in cols:
        style = "padding:4px 8px;border-bottom:1px solid #1a2d48;text-align:right;font-size:11px;"
        if col == "Date":
            style += "text-align:left;font-weight:600;color:#7a92b0;"
            html += f'<td style="{style}">{fcst_row["Date"]}</td>'
        elif col == "Band":
            style += "text-align:left;font-weight:600;color:#7a92b0;font-style:italic;"
            html += f'<td style="{style}">Adj\u2013Model</td>'
        elif col.startswith("HE") and col[2:].isdigit():
            h = int(col[2:])
            d = total_on if h in ONPEAK_HOURS else total_off
            html += f'<td style="{style}color:#8ea8c4;">{d:+.1f}</td>'
        elif col == "OnPeak":
            html += f'<td style="{style}color:#8ea8c4;">{total_on:+.1f}</td>'
        elif col == "OffPeak":
            html += f'<td style="{style}color:#8ea8c4;">{total_off:+.1f}</td>'
        elif col == "Flat":
            html += f'<td style="{style}color:#8ea8c4;">{total_flat:+.1f}</td>'
    html += "</tr>"

    html += "</tbody></table></div>"
    return html


# ── Section 3: Comparison Chart ──────────────────────────────────────


def _comparison_chart_html(result: dict) -> str:
    """Plotly chart: regression-adjusted bands + model + adjusted forecast."""
    adj_df = result["df_forecast"]
    base_df = result["base_df_forecast"]
    output_table = result["output_table"]
    has_actuals = result.get("has_actuals", False)

    hours = adj_df["hour_ending"].values
    fig = go.Figure()

    # Adjusted P10-P90
    if "q_0.10" in adj_df.columns and "q_0.90" in adj_df.columns:
        fig.add_trace(go.Scatter(
            x=hours, y=adj_df["q_0.90"],
            mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip",
        ))
        fig.add_trace(go.Scatter(
            x=hours, y=adj_df["q_0.10"],
            mode="lines", line=dict(width=0),
            fill="tonexty", fillcolor="rgba(255, 161, 90, 0.10)",
            name="Adj P10\u2013P90", hoverinfo="skip",
        ))

    # Adjusted P25-P75
    if "q_0.25" in adj_df.columns and "q_0.75" in adj_df.columns:
        fig.add_trace(go.Scatter(
            x=hours, y=adj_df["q_0.75"],
            mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip",
        ))
        fig.add_trace(go.Scatter(
            x=hours, y=adj_df["q_0.25"],
            mode="lines", line=dict(width=0),
            fill="tonexty", fillcolor="rgba(255, 161, 90, 0.25)",
            name="Adj P25\u2013P75", hoverinfo="skip",
        ))

    # Adjusted P50
    if "q_0.50" in adj_df.columns:
        fig.add_trace(go.Scatter(
            x=hours, y=adj_df["q_0.50"],
            mode="lines", name="Adj P50",
            line=dict(color="#FF6692", width=2, dash="dash"),
            hovertemplate="HE %{x}<br>Adj Median: $%{y:.1f}/MWh<extra></extra>",
        ))

    # Model forecast (dotted gray)
    fig.add_trace(go.Scatter(
        x=hours, y=base_df["point_forecast"],
        mode="lines", name="Model",
        line=dict(color="#8ea8c4", width=1.5, dash="dot"),
        hovertemplate="HE %{x}<br>Model: $%{y:.1f}/MWh<extra></extra>",
    ))

    # Regression-adjusted forecast (bold orange)
    fig.add_trace(go.Scatter(
        x=hours, y=adj_df["point_forecast"],
        mode="lines+markers", name="Regression Adj",
        line=dict(color="#FFA15A", width=3),
        marker=dict(size=5),
        hovertemplate="HE %{x}<br>Reg Adj: $%{y:.1f}/MWh<extra></extra>",
    ))

    # Actuals
    if has_actuals:
        actual_rows = output_table[output_table["Type"] == "Actual"]
        if len(actual_rows) > 0:
            actual_row = actual_rows.iloc[0]
            actual_vals = [actual_row[f"HE{h}"] for h in range(1, 25)]
            fig.add_trace(go.Scatter(
                x=list(range(1, 25)), y=actual_vals,
                mode="lines+markers", name="Actual",
                line=dict(color="#4cc9f0", width=2),
                marker=dict(size=5),
                hovertemplate="HE %{x}<br>Actual: $%{y:.1f}/MWh<extra></extra>",
            ))

    fig.update_layout(
        title="Regression-Adjusted Forecast vs Model",
        xaxis_title="Hour Ending",
        yaxis_title="$/MWh",
        height=500,
        template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="top", y=-0.12, xanchor="left", x=0),
        margin=dict(l=60, r=40, t=40, b=70),
        hovermode="x unified",
    )
    fig.update_xaxes(dtick=1, range=[0.5, 24.5])

    return fig.to_html(include_plotlyjs="cdn", full_html=False, div_id="regadj-plot")


def _error_html(msg: str) -> str:
    return f'<div style="padding:16px;color:#e74c3c;font-size:14px;">{msg}</div>'
