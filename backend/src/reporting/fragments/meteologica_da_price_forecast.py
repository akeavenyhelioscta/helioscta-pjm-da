"""Meteologica DA price forecast dashboard — weekly hourly price forecast.

Sections:
  1. Price Summary  — one row per date (today → Friday), HE1-24 + OnPeak/OffPeak/Flat
  2. Price Profile   — overlaid hourly lines, one trace per date
"""
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go

from src.like_day_forecast import configs
from src.like_day_forecast.pipelines.forecast import run as run_forecast
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
]

Section = tuple[str, Any, str | None]


def _week_range(today: date) -> tuple[date, date]:
    """Return (today, Friday) of the current week. If today is Sat/Sun, use next Mon-Fri."""
    weekday = today.weekday()  # Mon=0 … Sun=6
    if weekday <= 4:  # Mon–Fri
        friday = today + timedelta(days=(4 - weekday))
    else:
        # Weekend → next Monday through next Friday
        monday = today + timedelta(days=(7 - weekday))
        friday = monday + timedelta(days=4)
        today = monday
    return today, friday


# ── Public entry point ───────────────────────────────────────────────


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list:
    """Pull Meteologica DA price forecast and return report sections for the week."""
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

    # Date range: today through Friday
    start, end = _week_range(date.today())
    df_week = df[(df["forecast_date"] >= start) & (df["forecast_date"] <= end)].copy()

    if df_week.empty:
        return [("Meteologica DA Price Forecast", _empty("No forecast data for this week."), None)]

    df_week = df_week.sort_values(["forecast_date", "hour_ending"])
    dates = sorted(df_week["forecast_date"].unique())
    range_label = f"{start} to {end}"

    # Execution timestamp
    exec_ts = ""
    if "forecast_execution_datetime" in df_week.columns:
        latest_exec = pd.to_datetime(df_week["forecast_execution_datetime"]).max()
        if pd.notna(latest_exec):
            exec_ts = latest_exec.strftime("%a %b %d, %H:%M")

    sections: list = []

    sections.append((
        f"Price Forecast — {range_label}",
        _build_summary_table(df_week, dates, exec_ts),
        None,
    ))

    sections.append((
        "Weekly Price Profile",
        _build_price_chart(df_week, dates, range_label),
        None,
    ))

    # ── Like-Day vs Meteologica comparison for tomorrow ────────────
    tomorrow = date.today() + timedelta(days=1)
    df_tomorrow = df[df["forecast_date"] == tomorrow].copy()

    if not df_tomorrow.empty:
        try:
            ld_result = run_forecast(
                forecast_date=None,
                config=configs.ScenarioConfig(schema=schema),
                cache_dir=cache_dir,
                cache_enabled=cache_enabled,
                cache_ttl_hours=cache_ttl_hours,
                force_refresh=force_refresh,
            )
            if "error" not in ld_result:
                sections.append((
                    f"Like-Day vs Meteologica — {tomorrow}",
                    _build_comparison_table(ld_result, df_tomorrow, tomorrow),
                    None,
                ))
                sections.append((
                    "Shape Comparison Chart",
                    _build_comparison_chart(ld_result, df_tomorrow, tomorrow),
                    None,
                ))
        except Exception as e:
            logger.warning(f"Could not build like-day comparison: {e}")

    return sections


# ── Summary Table ────────────────────────────────────────────────────


def _build_summary_table(df: pd.DataFrame, dates: list, exec_ts: str) -> str:
    """One row per date, HE1-24 + OnPeak/OffPeak/Flat."""
    html = '<div style="overflow-x:auto;padding:8px 0;">\n'

    if exec_ts:
        html += (
            f'<div style="padding:4px 8px;margin-bottom:8px;">'
            f'<span style="font-size:11px;color:#6f8db1;font-weight:600;">Latest Execution:</span> '
            f'<span style="font-size:12px;font-family:monospace;color:#dbe7ff;">{exec_ts} EPT</span>'
            f'</div>\n'
        )

    html += (
        '<table style="width:100%;border-collapse:collapse;font-size:11px;'
        "font-family:'IBM Plex Sans',monospace;\">\n"
    )

    # Header
    html += '<tr style="border-bottom:1px solid #2a3f60;">'
    html += _th("Date", align="left")
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

        bg = f"rgba({_hex_to_rgb(color)}, 0.06)" if idx % 2 == 0 else "transparent"
        html += f'<tr style="border-bottom:1px solid #1a2a42;background:{bg};">'
        html += (
            f'<td style="padding:4px 8px;font-weight:600;color:{color};'
            f'white-space:nowrap;">{dt_label}</td>'
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


def _build_price_chart(df: pd.DataFrame, dates: list, range_label: str) -> str:
    """Overlaid hourly price profiles — one trace per date."""
    fig = go.Figure()

    for idx, dt in enumerate(dates):
        day_df = df[df["forecast_date"] == dt].sort_values("hour_ending")
        color = DAY_COLORS[idx % len(DAY_COLORS)]
        dt_label = pd.Timestamp(dt).strftime("%a %m/%d")

        fig.add_trace(go.Scatter(
            x=day_df["hour_ending"].values,
            y=day_df["forecast_da_price"].values,
            mode="lines+markers",
            name=dt_label,
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


# ── Like-Day vs Meteologica Comparison ─────────────────────────────────


def _build_comparison_table(ld_result: dict, df_meteo: pd.DataFrame, tomorrow: date) -> str:
    """Side-by-side table: Like-Day quantiles + forecast vs Meteologica hourly."""
    quantiles_table = ld_result["quantiles_table"]
    output_table = ld_result["output_table"]
    df_forecast = ld_result["df_forecast"]

    meteo_prices = df_meteo.set_index("hour_ending")["forecast_da_price"]

    cols = ["Source"] + [f"HE{h}" for h in range(1, 25)] + ["OnPeak", "OffPeak", "Flat"]

    display_rows = []

    # Quantile bands
    for _, row in quantiles_table.iterrows():
        r = {"Source": row["Type"]}
        for h in range(1, 25):
            r[f"HE{h}"] = row[f"HE{h}"]
        for s in ["OnPeak", "OffPeak", "Flat"]:
            r[s] = row[s]
        display_rows.append(r)

    # Like-Day forecast
    fcst_row = output_table[output_table["Type"] == "Forecast"].iloc[0]
    r = {"Source": "Like-Day"}
    for h in range(1, 25):
        r[f"HE{h}"] = fcst_row[f"HE{h}"]
    for s in ["OnPeak", "OffPeak", "Flat"]:
        r[s] = fcst_row[s]
    display_rows.append(r)

    # Meteologica
    meteo_vals = {}
    r = {"Source": "Meteologica"}
    for h in range(1, 25):
        val = meteo_prices.get(h)
        r[f"HE{h}"] = float(val) if pd.notna(val) else None
        if pd.notna(val):
            meteo_vals[h] = float(val)
    onpk = [meteo_vals[h] for h in ONPEAK_HOURS if h in meteo_vals]
    offpk = [meteo_vals[h] for h in OFFPEAK_HOURS if h in meteo_vals]
    allh = list(meteo_vals.values())
    import numpy as np
    r["OnPeak"] = np.mean(onpk) if onpk else None
    r["OffPeak"] = np.mean(offpk) if offpk else None
    r["Flat"] = np.mean(allh) if allh else None
    display_rows.append(r)

    # Diff row: Like-Day − Meteologica
    r_diff = {"Source": "LD \u2013 Meteo"}
    for h in range(1, 25):
        ld_val = fcst_row[f"HE{h}"]
        m_val = meteo_vals.get(h)
        r_diff[f"HE{h}"] = (ld_val - m_val) if (pd.notna(ld_val) and m_val is not None) else None
    for s in ["OnPeak", "OffPeak", "Flat"]:
        ld_s = display_rows[-2].get(s)  # Like-Day
        m_s = display_rows[-1].get(s)   # Meteologica
        r_diff[s] = (ld_s - m_s) if (ld_s is not None and m_s is not None) else None
    display_rows.append(r_diff)

    # Render
    row_colors = {
        "Like-Day": "#FFA15A",
        "Meteologica": "#60a5fa",
        "LD \u2013 Meteo": "#7a92b0",
    }
    row_bg = {
        "Like-Day": "rgba(255, 161, 90, 0.08)",
        "Meteologica": "rgba(96, 165, 250, 0.08)",
        "LD \u2013 Meteo": "rgba(14, 25, 42, 0.6)",
    }

    html = '<div style="overflow-x:auto;padding:8px;">'
    html += (
        '<table style="width:100%;border-collapse:collapse;'
        'font-size:11px;font-family:monospace;">'
    )

    # Header
    html += "<thead><tr>"
    for col in cols:
        align = "text-align:left;" if col == "Source" else ""
        html += (
            f'<th style="padding:5px 6px;background:#16263d;color:#e6efff;'
            f'text-align:right;font-size:10px;position:sticky;top:0;{align}">{col}</th>'
        )
    html += "</tr></thead><tbody>"

    q_idx = 0
    for row in display_rows:
        src = row["Source"]
        label_color = row_colors.get(src, "#a6bad6")
        is_special = src in row_colors
        is_diff = src == "LD \u2013 Meteo"

        tr_parts = []
        if src == "Like-Day":
            tr_parts.append("border-top:3px solid #3a5070")
        if src in row_bg:
            tr_parts.append(f"background:{row_bg[src]}")
        elif not is_special:
            bg = "rgba(22, 38, 61, 0.5)" if q_idx % 2 == 0 else "transparent"
            tr_parts.append(f"background:{bg}")
            q_idx += 1
        tr_style = ";".join(tr_parts) + ";" if tr_parts else ""

        html += f'<tr style="{tr_style}">'
        for col in cols:
            val = row[col]
            style = "padding:4px 6px;border-bottom:1px solid #1e3350;text-align:right;"

            if col == "Source":
                style += f"text-align:left;font-weight:600;color:{label_color};"
                html += f'<td style="{style}">{val}</td>'
            elif val is not None and pd.notna(val):
                if is_diff:
                    fmt = f"{val:+.1f}"
                    html += f'<td style="{style}color:#8ea8c4;font-size:10px;">{fmt}</td>'
                else:
                    cell_color = label_color if is_special else "#dbe7ff"
                    html += f'<td style="{style}color:{cell_color};">{val:.1f}</td>'
            else:
                html += f'<td style="{style}color:#556;">\u2014</td>'
        html += "</tr>"

    html += "</tbody></table></div>"
    return html


def _build_comparison_chart(ld_result: dict, df_meteo: pd.DataFrame, tomorrow: date) -> str:
    """Plotly chart overlaying Like-Day bands + forecast vs Meteologica hourly."""
    df_forecast = ld_result["df_forecast"]
    output_table = ld_result["output_table"]
    hours = df_forecast["hour_ending"].values

    meteo_prices = df_meteo.sort_values("hour_ending")
    meteo_hours = meteo_prices["hour_ending"].values
    meteo_vals = meteo_prices["forecast_da_price"].values

    fig = go.Figure()

    # Like-Day P10-P90 band
    if "q_0.10" in df_forecast.columns and "q_0.90" in df_forecast.columns:
        fig.add_trace(go.Scatter(
            x=hours, y=df_forecast["q_0.90"],
            mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip",
        ))
        fig.add_trace(go.Scatter(
            x=hours, y=df_forecast["q_0.10"],
            mode="lines", line=dict(width=0),
            fill="tonexty", fillcolor="rgba(255, 161, 90, 0.10)",
            name="LD P10\u2013P90", hoverinfo="skip",
        ))

    # Like-Day P25-P75 band
    if "q_0.25" in df_forecast.columns and "q_0.75" in df_forecast.columns:
        fig.add_trace(go.Scatter(
            x=hours, y=df_forecast["q_0.75"],
            mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip",
        ))
        fig.add_trace(go.Scatter(
            x=hours, y=df_forecast["q_0.25"],
            mode="lines", line=dict(width=0),
            fill="tonexty", fillcolor="rgba(255, 161, 90, 0.25)",
            name="LD P25\u2013P75", hoverinfo="skip",
        ))

    # Like-Day P50
    if "q_0.50" in df_forecast.columns:
        fig.add_trace(go.Scatter(
            x=hours, y=df_forecast["q_0.50"],
            mode="lines", name="LD P50",
            line=dict(color="#FF6692", width=1.5, dash="dash"),
            hovertemplate="HE %{x}<br>LD Median: $%{y:.1f}<extra></extra>",
        ))

    # Like-Day forecast
    fig.add_trace(go.Scatter(
        x=hours, y=df_forecast["point_forecast"],
        mode="lines+markers", name="Like-Day",
        line=dict(color="#FFA15A", width=2.5),
        marker=dict(size=5),
        hovertemplate="HE %{x}<br>Like-Day: $%{y:.1f}<extra></extra>",
    ))

    # Meteologica forecast
    fig.add_trace(go.Scatter(
        x=meteo_hours, y=meteo_vals,
        mode="lines+markers", name="Meteologica",
        line=dict(color="#60a5fa", width=2.5),
        marker=dict(size=5, symbol="diamond"),
        hovertemplate="HE %{x}<br>Meteo: $%{y:.1f}<extra></extra>",
    ))

    # On-peak shading
    fig.add_vrect(
        x0=7.5, x1=23.5,
        fillcolor="rgba(251, 191, 36, 0.04)",
        line_width=0,
    )

    fig.update_layout(
        title=f"Like-Day vs Meteologica — {tomorrow}",
        xaxis_title="Hour Ending",
        yaxis_title="$/MWh",
        height=500,
        template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="top", y=-0.12, xanchor="left", x=0),
        margin=dict(l=60, r=40, t=40, b=70),
        hovermode="x unified",
    )
    fig.update_xaxes(dtick=1, range=[0.5, 24.5])

    return fig.to_html(include_plotlyjs="cdn", full_html=False, div_id="ld-vs-meteo")


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
