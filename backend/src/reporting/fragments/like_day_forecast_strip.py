"""Strip forecast report — multi-day DA LMP predictions with quantile bands.

Each forecast day (D+1 through Friday) gets its own section pair:
  1. Quantile Bands Table — P01–P99, Forecast, Actual (if available)
  2. Quantile Band Chart  — shaded bands, median, forecast, actuals, outliers
"""
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from src.like_day_forecast import configs
from src.like_day_forecast.pipelines.strip_forecast import run_strip

logger = logging.getLogger(__name__)

PLOTLY_TEMPLATE = "plotly_dark"
HE_COLS = [f"HE{h}" for h in range(1, 25)]
SUMMARY_COLS = ["OnPeak", "OffPeak", "Flat"]
DAY_ABBR = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}

Section = tuple[str, Any, str | None]


def _horizon_to_friday() -> int:
    """Number of days from today+1 through Friday (inclusive)."""
    today = date.today()
    wd = today.weekday()  # Mon=0 … Sun=6
    if wd <= 3:  # Mon–Thu: forecast through Friday
        return 4 - wd  # Mon→4, Tue→3, Wed→2, Thu→1
    if wd == 4:  # Fri: at least D+1 (Sat is not useful, show next Mon–Fri)
        return 5
    # Weekend: next Mon–Fri
    return 5 + (4 - (7 - wd) % 7)


# ── Public entry point ───────────────────────────────────────────────


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list:
    """Run the strip forecast and return per-day quantile sections."""
    logger.info("Building strip forecast report fragments...")

    horizon = _horizon_to_friday()
    result = run_strip(
        horizon=horizon,
        config=configs.ScenarioConfig(schema=schema),
    )

    if "error" in result:
        return [("Strip Forecast Error", _error_html(result["error"]), None)]

    strip_table = result["strip_table"]
    quantiles_table = result["quantiles_table"]
    per_day = result["per_day"]
    ref_date = result["reference_date"]
    forecast_dates = [pd.to_datetime(d).date() for d in result["forecast_dates"]]

    sections: list = []

    for i, fd in enumerate(forecast_dates):
        day_key = str(fd)
        if day_key not in per_day:
            continue

        day_info = per_day[day_key]
        df_forecast = day_info["df_forecast"]
        has_actuals = day_info["has_actuals"]
        offset = day_info["offset"]
        day_label = f"{fd} ({DAY_ABBR[fd.weekday()]}) — D+{offset}"

        # Filter tables to this date
        day_quantiles = quantiles_table[quantiles_table["Date"] == fd]
        day_strip = strip_table[strip_table["Date"] == fd]

        if i > 0:
            sections.append(f"D+{offset}")

        # Unique id prefix per day to avoid DOM collisions
        tid = f"sf{offset}"

        sections.append((
            f"Quantile Bands — {day_label}",
            _quantile_table_html(day_quantiles, day_strip, df_forecast, has_actuals, fd, tid),
            None,
        ))
        sections.append((
            f"Quantile Chart — {day_label}",
            _quantile_chart_html(df_forecast, day_strip, has_actuals, day_label, tid),
            None,
        ))

    return sections


# ── Quantile Bands Table ─────────────────────────────────────────────


def _quantile_table_html(
    quantiles_df: pd.DataFrame,
    strip_df: pd.DataFrame,
    df_forecast: pd.DataFrame,
    has_actuals: bool,
    forecast_date: date,
    tid: str,
) -> str:
    """Quantile bands table for a single day — mirrors forecast_results layout."""
    cols = ["Date", "Band"] + HE_COLS + SUMMARY_COLS

    display_rows = []

    # Quantile rows (P01, P05, … P99)
    for _, row in quantiles_df.iterrows():
        r = {"Date": row["Date"], "Band": row["Type"]}
        for c in HE_COLS + SUMMARY_COLS:
            r[c] = row[c]
        display_rows.append(r)

    # Forecast row
    fc_rows = strip_df[strip_df["Type"] == "Forecast"]
    if len(fc_rows):
        fc = fc_rows.iloc[0]
        r = {"Date": fc["Date"], "Band": "Forecast"}
        for c in HE_COLS + SUMMARY_COLS:
            r[c] = fc[c]
        display_rows.append(r)

    # Actual row
    if has_actuals:
        act_rows = strip_df[strip_df["Type"] == "Actual"]
        if len(act_rows):
            act = act_rows.iloc[0]
            r = {"Date": act["Date"], "Band": "Actual"}
            for c in HE_COLS + SUMMARY_COLS:
                r[c] = act[c]
            display_rows.append(r)

    # Styling
    band_row_colors = {
        "Forecast": "#FFA15A",
        "Actual": "#4cc9f0",
    }
    band_row_bg = {
        "Forecast": "rgba(255, 161, 90, 0.08)",
        "Actual": "rgba(76, 201, 240, 0.08)",
    }
    quantile_even_bg = "rgba(22, 38, 61, 0.5)"

    html = f'<div style="overflow-x:auto;padding:8px;">'
    html += (
        f'<table id="{tid}" style="width:100%;border-collapse:collapse;'
        f'font-size:12px;font-family:monospace;">'
    )

    # Header
    html += "<thead><tr>"
    for col in cols:
        align = "left" if col in ("Date", "Band") else "right"
        html += (
            f'<th style="padding:6px 8px;background:#16263d;color:#e6efff;'
            f'text-align:{align};font-size:11px;position:sticky;top:0;">{col}</th>'
        )
    html += "</tr></thead><tbody>"

    q_idx = 0
    for row in display_rows:
        band = row["Band"]
        label_color = band_row_colors.get(band, "#a6bad6")
        is_special = band in band_row_colors

        tr_parts: list[str] = []
        if band == "Forecast":
            tr_parts.append("border-top:3px solid #3a5070")
        if band in band_row_bg:
            tr_parts.append(f"background:{band_row_bg[band]}")
        elif band not in band_row_colors:
            tr_parts.append(
                f"background:{quantile_even_bg if q_idx % 2 == 0 else 'transparent'}"
            )
            q_idx += 1
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

    # Diff row: Forecast − Actual
    if has_actuals and len(fc_rows) and len(act_rows):
        fc = fc_rows.iloc[0]
        act = act_rows.iloc[0]
        ncols = len(cols)
        html += (
            f'<tr><td colspan="{ncols}" style="padding:0;height:3px;'
            f'background:linear-gradient(90deg,#4a6a8a 0%,#4a6a8a60 100%);'
            f'border:none;"></td></tr>'
        )
        html += '<tr style="background:rgba(14, 25, 42, 0.6);">'
        for col in cols:
            style = (
                "padding:4px 8px;border-bottom:1px solid #1a2d48;"
                "text-align:right;font-size:11px;"
            )
            if col == "Date":
                html += f'<td style="{style}text-align:left;font-weight:600;color:#7a92b0;">{forecast_date}</td>'
            elif col == "Band":
                html += f'<td style="{style}text-align:left;font-weight:600;color:#7a92b0;font-style:italic;">Fcst\u2013Act</td>'
            else:
                fv = fc[col]
                av = act[col]
                if pd.notna(fv) and pd.notna(av):
                    d = fv - av
                    fmt = f"{d:+.1f}"
                    bg = _err_bg(d)
                    html += f'<td style="{style}color:#dbe7ff;background:{bg};">{fmt}</td>'
                else:
                    html += f'<td style="{style}color:#556;">\u2014</td>'
        html += "</tr>"

    html += "</tbody></table></div>"
    return html


# ── Quantile Bands Chart ────────────────────────────────────────────


def _quantile_chart_html(
    df_forecast: pd.DataFrame,
    strip_df: pd.DataFrame,
    has_actuals: bool,
    day_label: str,
    tid: str,
) -> str:
    """Plotly chart with quantile bands, median, forecast, actuals, outlier markers."""
    hours = df_forecast["hour_ending"].values
    fig = go.Figure()

    # P10–P90 outer band
    if "q_0.10" in df_forecast.columns and "q_0.90" in df_forecast.columns:
        fig.add_trace(go.Scatter(
            x=hours, y=df_forecast["q_0.90"],
            mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip",
        ))
        fig.add_trace(go.Scatter(
            x=hours, y=df_forecast["q_0.10"],
            mode="lines", line=dict(width=0),
            fill="tonexty", fillcolor="rgba(99, 110, 250, 0.10)",
            name="P10\u2013P90", hoverinfo="skip",
        ))

    # P25–P75 inner band
    if "q_0.25" in df_forecast.columns and "q_0.75" in df_forecast.columns:
        fig.add_trace(go.Scatter(
            x=hours, y=df_forecast["q_0.75"],
            mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip",
        ))
        fig.add_trace(go.Scatter(
            x=hours, y=df_forecast["q_0.25"],
            mode="lines", line=dict(width=0),
            fill="tonexty", fillcolor="rgba(99, 110, 250, 0.25)",
            name="P25\u2013P75", hoverinfo="skip",
        ))

    # P50 median line
    if "q_0.50" in df_forecast.columns:
        fig.add_trace(go.Scatter(
            x=hours, y=df_forecast["q_0.50"],
            mode="lines", name="P50 (Median)",
            line=dict(color="#AB63FA", width=2, dash="dash"),
            hovertemplate="HE %{x}<br>Median: $%{y:.1f}/MWh<extra></extra>",
        ))

    # Forecast line
    fig.add_trace(go.Scatter(
        x=hours, y=df_forecast["point_forecast"],
        mode="lines+markers", name="Forecast",
        line=dict(color="#FFA15A", width=2.5),
        marker=dict(size=5),
        hovertemplate="HE %{x}<br>Forecast: $%{y:.1f}/MWh<extra></extra>",
    ))

    # Actual line + outlier markers
    if has_actuals:
        act_rows = strip_df[strip_df["Type"] == "Actual"]
        if len(act_rows):
            act = act_rows.iloc[0]
            actual_vals = [act[f"HE{h}"] for h in range(1, 25)]

            fig.add_trace(go.Scatter(
                x=list(range(1, 25)), y=actual_vals,
                mode="lines+markers", name="Actual",
                line=dict(color="#4cc9f0", width=2),
                marker=dict(size=5),
                hovertemplate="HE %{x}<br>Actual: $%{y:.1f}/MWh<extra></extra>",
            ))

            # Outlier markers (outside P25–P75)
            if "q_0.25" in df_forecast.columns and "q_0.75" in df_forecast.columns:
                q25 = dict(zip(df_forecast["hour_ending"].astype(int), df_forecast["q_0.25"]))
                q75 = dict(zip(df_forecast["hour_ending"].astype(int), df_forecast["q_0.75"]))
                out_h, out_v = [], []
                for h in range(1, 25):
                    a = actual_vals[h - 1]
                    lo, hi = q25.get(h), q75.get(h)
                    if pd.notna(a) and lo is not None and hi is not None and (a < lo or a > hi):
                        out_h.append(h)
                        out_v.append(a)
                if out_h:
                    fig.add_trace(go.Scatter(
                        x=out_h, y=out_v,
                        mode="markers", name="Outside P25\u2013P75",
                        marker=dict(
                            color="#EF553B", size=10, symbol="diamond",
                            line=dict(width=1.5, color="#EF553B"),
                        ),
                        hovertemplate=(
                            "HE %{x}<br>Actual: $%{y:.1f}/MWh<br>"
                            "<b>Outside IQR</b><extra></extra>"
                        ),
                    ))

    fig.update_layout(
        title=f"Quantile Bands — {day_label}",
        xaxis_title="Hour Ending",
        yaxis_title="$/MWh",
        height=500,
        template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="top", y=-0.12, xanchor="left", x=0),
        margin=dict(l=60, r=40, t=40, b=70),
        hovermode="x unified",
    )
    fig.update_xaxes(dtick=1, range=[0.5, 24.5])

    plot_id = f"{tid}-plot"
    return fig.to_html(include_plotlyjs="cdn", full_html=False, div_id=plot_id)


# ── Helpers ──────────────────────────────────────────────────────────


def _err_bg(v: float) -> str:
    a = abs(v)
    if a < 0.05:
        return ""
    if a < 5:
        return "rgba(0,204,150,0.20)"
    if a < 15:
        return "rgba(255,161,90,0.25)"
    return "rgba(239,85,59,0.30)"


def _error_html(msg: str) -> str:
    return f'<div style="padding:16px;color:#e74c3c;font-size:14px;">{msg}</div>'
