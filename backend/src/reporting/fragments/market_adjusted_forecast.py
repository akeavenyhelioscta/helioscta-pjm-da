"""Market-adjusted forecast report — rescaled like-day shape anchored to ICE tape.

Sections:
  1. Adjustment Summary  — Model vs Market delta table
  2. Model Quantile Bands  — Original (unadjusted) band table + model forecast
  3. Adjusted Quantile Bands  — Shifted band table + adjusted forecast + delta row
  4. Combined Chart — Plotly chart: model bands (blue-gray) + adjusted bands (orange)
"""
import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from src.like_day_forecast import configs
from src.like_day_forecast.pipelines.market_adjusted_forecast import run as run_adjusted
from src.data import meteologica_da_price_forecast
from src.utils.cache_utils import pull_with_cache

logger = logging.getLogger(__name__)

PLOTLY_TEMPLATE = "plotly_dark"
HE_COLS = [f"HE{h}" for h in range(1, 25)]
SUMMARY_COLS = ["OnPeak", "OffPeak", "Flat"]
ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = list(range(1, 8)) + [24]
Section = tuple[str, Any, str | None]

# Default ICE anchor — can be overridden via build_fragments(market_onpeak=...)
DEFAULT_MARKET_ONPEAK = 60.80


def build_fragments(
    schema: str = configs.SCHEMA,
    market_onpeak: float = DEFAULT_MARKET_ONPEAK,
    market_offpeak: float | None = None,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list[Section]:
    """Run market-adjusted forecast and return report sections."""
    logger.info(f"Building market-adjusted forecast report (anchor: ${market_onpeak:.2f} on-peak)...")

    result = run_adjusted(
        market_onpeak=market_onpeak,
        market_offpeak=market_offpeak,
        forecast_date=None,
        config=configs.ScenarioConfig(schema=schema),
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        cache_ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    if "error" in result:
        return [("Market-Adjusted Forecast Error", _error_html(result["error"]), None)]

    adj = result["adjustment"]
    forecast_date = result["forecast_date"]

    # Pull Meteologica DA price forecast for the same date
    meteo_hourly = None
    try:
        from datetime import date, timedelta
        target = pd.to_datetime(forecast_date).date() if isinstance(forecast_date, str) else forecast_date
        df_meteo = pull_with_cache(
            source_name="meteologica_da_price_forecast",
            pull_fn=meteologica_da_price_forecast.pull,
            pull_kwargs={},
            cache_dir=cache_dir,
            cache_enabled=cache_enabled,
            ttl_hours=cache_ttl_hours,
            force_refresh=force_refresh,
        )
        df_meteo_day = df_meteo[df_meteo["forecast_date"] == target].sort_values("hour_ending")
        if not df_meteo_day.empty:
            meteo_hourly = dict(zip(
                df_meteo_day["hour_ending"].astype(int),
                df_meteo_day["forecast_da_price"],
            ))
            logger.info(f"Meteologica DA forecast loaded for {target}: "
                        f"{len(meteo_hourly)} hours")
    except Exception as e:
        logger.warning(f"Could not pull Meteologica DA forecast: {e}")

    sections: list[Section] = []

    # 1. Adjustment summary card
    sections.append((
        f"Market Adjustment — {forecast_date}",
        _adjustment_summary_html(adj, meteo_hourly),
        None,
    ))

    # 2. Model quantile bands table (original, unadjusted)
    sections.append((
        f"Model Quantile Bands — {forecast_date}",
        _model_bands_table_html(result),
        None,
    ))

    # 3. Adjusted quantile bands table (with Meteologica)
    sections.append((
        f"Adjusted Quantile Bands — {forecast_date}",
        _adjusted_bands_table_html(result, meteo_hourly),
        None,
    ))

    # 4. Combined chart — model + adjusted + Meteologica
    sections.append((
        "Model vs Market-Adjusted Chart",
        _comparison_chart_html(result, meteo_hourly),
        None,
    ))

    return sections


# ── Section 1: Adjustment Summary ────────────────────────────────────


def _adjustment_summary_html(adj: dict, meteo_hourly: dict | None = None) -> str:
    """Compact card showing model → market adjustment, with Meteologica comparison."""
    base_on = adj["base_onpeak"]
    base_off = adj["base_offpeak"]
    mkt_on = adj["market_onpeak"]
    mkt_off = adj["market_offpeak"]
    delta_on = adj["onpeak_delta"]
    delta_off = adj["offpeak_delta"]
    base_flat = (base_on * 16 + base_off * 8) / 24
    mkt_flat = (mkt_on * 16 + mkt_off * 8) / 24

    # Compute Meteologica period averages
    meteo_on = meteo_off = meteo_flat = None
    if meteo_hourly:
        on_vals = [meteo_hourly[h] for h in ONPEAK_HOURS if h in meteo_hourly and pd.notna(meteo_hourly[h])]
        off_vals = [meteo_hourly[h] for h in OFFPEAK_HOURS if h in meteo_hourly and pd.notna(meteo_hourly[h])]
        all_vals = [v for v in meteo_hourly.values() if pd.notna(v)]
        meteo_on = np.mean(on_vals) if on_vals else None
        meteo_off = np.mean(off_vals) if off_vals else None
        meteo_flat = np.mean(all_vals) if all_vals else None

    has_meteo = meteo_on is not None

    html = '<div style="padding:12px;">'
    html += (
        '<table style="border-collapse:collapse;font-size:13px;font-family:monospace;'
        'min-width:500px;">'
    )
    html += '<thead><tr>'
    html += '<th style="padding:8px 16px;background:#16263d;color:#e6efff;text-align:left;">Period</th>'
    html += '<th style="padding:8px 16px;background:#16263d;color:#e6efff;text-align:right;">Like-Day</th>'
    if has_meteo:
        html += '<th style="padding:8px 16px;background:#16263d;color:#e6efff;text-align:right;">Meteologica</th>'
    html += '<th style="padding:8px 16px;background:#16263d;color:#e6efff;text-align:right;">Market</th>'
    html += '<th style="padding:8px 16px;background:#16263d;color:#e6efff;text-align:right;">LD Delta</th>'
    if has_meteo:
        html += '<th style="padding:8px 16px;background:#16263d;color:#e6efff;text-align:right;">Meteo Delta</th>'
    html += '</tr></thead><tbody>'

    rows_data = [
        ("On-Peak", base_on, meteo_on, mkt_on, delta_on),
        ("Off-Peak", base_off, meteo_off, mkt_off, delta_off),
        ("Flat", base_flat, meteo_flat, mkt_flat, mkt_flat - base_flat),
    ]

    for label, base, meteo, mkt, delta in rows_data:
        delta_color = "#EF553B" if delta > 0 else "#00CC96" if delta < 0 else "#8ea8c4"
        meteo_delta = (mkt - meteo) if meteo is not None else None
        meteo_delta_color = "#EF553B" if meteo_delta and meteo_delta > 0 else "#00CC96" if meteo_delta and meteo_delta < 0 else "#8ea8c4"

        html += f'<tr style="border-bottom:1px solid #1e3350;">'
        html += f'<td style="padding:8px 16px;color:#dbe7ff;font-weight:600;">{label}</td>'
        html += f'<td style="padding:8px 16px;color:#8ea8c4;text-align:right;">${base:.2f}</td>'
        if has_meteo:
            html += f'<td style="padding:8px 16px;color:#60a5fa;text-align:right;">${meteo:.2f}</td>'
        html += (f'<td style="padding:8px 16px;color:#FFA15A;text-align:right;font-weight:600;">'
                 f'${mkt:.2f}</td>')
        html += (f'<td style="padding:8px 16px;color:{delta_color};text-align:right;font-weight:600;">'
                 f'{delta:+.2f}</td>')
        if has_meteo and meteo_delta is not None:
            html += (f'<td style="padding:8px 16px;color:{meteo_delta_color};text-align:right;font-weight:600;">'
                     f'{meteo_delta:+.2f}</td>')
        html += '</tr>'

    html += '</tbody></table></div>'
    return html


# ── Section 2: Model Bands Table ────────────────────────────────────


def _model_bands_table_html(result: dict) -> str:
    """Original (unadjusted) quantile bands + model forecast."""
    base_quantiles = result["base_quantiles_table"]
    base_output = result["base_output_table"]

    cols = ["Date", "Band"] + HE_COLS + SUMMARY_COLS
    tid = "model-bands"

    display_rows = []
    for _, row in base_quantiles.iterrows():
        r = {"Date": row["Date"], "Band": row["Type"]}
        for c in HE_COLS + SUMMARY_COLS:
            r[c] = row[c]
        display_rows.append(r)

    # Model forecast row
    fcst_row = base_output[base_output["Type"] == "Forecast"].iloc[0]
    r = {"Date": fcst_row["Date"], "Band": "Model"}
    for c in HE_COLS + SUMMARY_COLS:
        r[c] = fcst_row[c]
    display_rows.append(r)

    band_row_colors = {"Model": "#8ea8c4"}
    band_row_bg = {"Model": "rgba(142, 168, 196, 0.08)"}

    return _render_bands_table(cols, tid, display_rows, band_row_colors, band_row_bg)


# ── Section 3: Adjusted Bands Table ──────────────────────────────────


def _adjusted_bands_table_html(result: dict, meteo_hourly: dict | None = None) -> str:
    """Shifted quantile bands + adjusted forecast + Meteologica."""
    adj_quantiles = result["quantiles_table"]
    output_table = result["output_table"]
    base_output = result["base_output_table"]
    adj_info = result["adjustment"]
    delta_on = adj_info["onpeak_delta"]
    delta_off = adj_info["offpeak_delta"]

    cols = ["Date", "Band"] + HE_COLS + SUMMARY_COLS
    tid = "adj-bands"

    display_rows = []
    for _, row in adj_quantiles.iterrows():
        r = {"Date": row["Date"], "Band": row["Type"]}
        for c in HE_COLS + SUMMARY_COLS:
            r[c] = row[c]
        display_rows.append(r)

    # Adjusted forecast
    adj_row = output_table[output_table["Type"] == "Adjusted"].iloc[0]
    r = {"Date": adj_row["Date"], "Band": "Adjusted"}
    for c in HE_COLS + SUMMARY_COLS:
        r[c] = adj_row[c]
    display_rows.append(r)

    # Meteologica raw
    if meteo_hourly:
        r = {"Date": adj_row["Date"], "Band": "Meteologica"}
        for h in range(1, 25):
            r[f"HE{h}"] = meteo_hourly.get(h)
        on_vals = [meteo_hourly[h] for h in ONPEAK_HOURS if h in meteo_hourly and pd.notna(meteo_hourly[h])]
        off_vals = [meteo_hourly[h] for h in OFFPEAK_HOURS if h in meteo_hourly and pd.notna(meteo_hourly[h])]
        all_vals = [v for v in meteo_hourly.values() if pd.notna(v)]
        r["OnPeak"] = np.mean(on_vals) if on_vals else np.nan
        r["OffPeak"] = np.mean(off_vals) if off_vals else np.nan
        r["Flat"] = np.mean(all_vals) if all_vals else np.nan
        display_rows.append(r)

        # Meteologica shifted (same delta as like-day)
        r_shifted = {"Date": adj_row["Date"], "Band": "Meteo Adj"}
        for h in range(1, 25):
            base_val = meteo_hourly.get(h)
            if base_val is not None and pd.notna(base_val):
                delta = delta_on if h in ONPEAK_HOURS else delta_off
                r_shifted[f"HE{h}"] = float(base_val) + delta
            else:
                r_shifted[f"HE{h}"] = None
        on_adj = [r_shifted[f"HE{h}"] for h in ONPEAK_HOURS if r_shifted.get(f"HE{h}") is not None]
        off_adj = [r_shifted[f"HE{h}"] for h in OFFPEAK_HOURS if r_shifted.get(f"HE{h}") is not None]
        all_adj = [r_shifted[f"HE{h}"] for h in range(1, 25) if r_shifted.get(f"HE{h}") is not None]
        r_shifted["OnPeak"] = np.mean(on_adj) if on_adj else np.nan
        r_shifted["OffPeak"] = np.mean(off_adj) if off_adj else np.nan
        r_shifted["Flat"] = np.mean(all_adj) if all_adj else np.nan
        display_rows.append(r_shifted)

    # Actuals (if present)
    has_actuals = result.get("has_actuals", False)
    if has_actuals:
        actual_row = output_table[output_table["Type"] == "Actual"]
        if len(actual_row) > 0:
            actual_row = actual_row.iloc[0]
            r = {"Date": actual_row["Date"], "Band": "Actual"}
            for c in HE_COLS + SUMMARY_COLS:
                r[c] = actual_row[c]
            display_rows.append(r)

    band_row_colors = {
        "Adjusted": "#FFA15A",
        "Meteologica": "#60a5fa",
        "Meteo Adj": "#34d399",
        "Actual": "#4cc9f0",
    }
    band_row_bg = {
        "Adjusted": "rgba(255, 161, 90, 0.10)",
        "Meteologica": "rgba(96, 165, 250, 0.08)",
        "Meteo Adj": "rgba(52, 211, 153, 0.08)",
        "Actual": "rgba(76, 201, 240, 0.08)",
    }

    html = _render_bands_table(cols, tid, display_rows, band_row_colors, band_row_bg)

    # Append delta row
    fcst_row = base_output[base_output["Type"] == "Forecast"].iloc[0]
    delta_on = adj_info["onpeak_delta"]
    delta_off = adj_info["offpeak_delta"]
    delta_flat = (delta_on * 16 + delta_off * 8) / 24

    delta_html = (
        f'<tr><td colspan="{len(cols)}" style="padding:0;height:3px;'
        f'background:linear-gradient(90deg,#4a6a8a 0%,#4a6a8a60 100%);'
        f'border:none;"></td></tr>'
    )
    delta_html += f'<tr style="background:rgba(14, 25, 42, 0.6);">'
    for col in cols:
        style = "padding:4px 8px;border-bottom:1px solid #1a2d48;text-align:right;font-size:11px;"
        if col == "Date":
            style += "text-align:left;font-weight:600;color:#7a92b0;"
            delta_html += f'<td style="{style}">{fcst_row["Date"]}</td>'
        elif col == "Band":
            style += "text-align:left;font-weight:600;color:#7a92b0;font-style:italic;"
            delta_html += f'<td style="{style}">Adj\u2013Model</td>'
        elif col.startswith("HE") and col[2:].isdigit():
            h = int(col[2:])
            d = delta_on if h in ONPEAK_HOURS else delta_off
            delta_html += f'<td style="{style}color:#8ea8c4;">{d:+.1f}</td>'
        elif col == "OnPeak":
            delta_html += f'<td style="{style}color:#8ea8c4;">{delta_on:+.1f}</td>'
        elif col == "OffPeak":
            delta_html += f'<td style="{style}color:#8ea8c4;">{delta_off:+.1f}</td>'
        elif col == "Flat":
            delta_html += f'<td style="{style}color:#8ea8c4;">{delta_flat:+.1f}</td>'
    delta_html += "</tr>"

    # Inject delta rows before closing </tbody></table>
    html = html.replace("</tbody></table>", delta_html + "</tbody></table>")
    return html


def _render_bands_table(
    cols: list[str],
    tid: str,
    display_rows: list[dict],
    band_row_colors: dict[str, str],
    band_row_bg: dict[str, str],
) -> str:
    """Shared table renderer for model and adjusted band tables."""
    html = '<div style="overflow-x:auto;padding:8px;">'
    html += (
        f'<table id="{tid}" style="width:100%;border-collapse:collapse;'
        f'font-size:12px;font-family:monospace;">'
    )

    # Header
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
        if is_special:
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

    html += "</tbody></table></div>"
    return html


# ── Section 3: Comparison Chart ──────────────────────────────────────


def _comparison_chart_html(result: dict, meteo_hourly: dict | None = None) -> str:
    """Plotly chart: model bands + adjusted bands + both forecasts + Meteologica."""
    adj_df = result["df_forecast"]
    base_df = result["base_df_forecast"]
    output_table = result["output_table"]
    adj_info = result["adjustment"]
    has_actuals = result.get("has_actuals", False)

    hours = adj_df["hour_ending"].values
    fig = go.Figure()

    # ── Model (original) bands ──────────────────────────────────────
    # Model P10-P90 band (blue-gray)
    if "q_0.10" in base_df.columns and "q_0.90" in base_df.columns:
        fig.add_trace(go.Scatter(
            x=hours, y=base_df["q_0.90"],
            mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip",
        ))
        fig.add_trace(go.Scatter(
            x=hours, y=base_df["q_0.10"],
            mode="lines", line=dict(width=0),
            fill="tonexty", fillcolor="rgba(142, 168, 196, 0.10)",
            name="Model P10\u2013P90", hoverinfo="skip",
        ))

    # Model P25-P75 band (blue-gray, darker)
    if "q_0.25" in base_df.columns and "q_0.75" in base_df.columns:
        fig.add_trace(go.Scatter(
            x=hours, y=base_df["q_0.75"],
            mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip",
        ))
        fig.add_trace(go.Scatter(
            x=hours, y=base_df["q_0.25"],
            mode="lines", line=dict(width=0),
            fill="tonexty", fillcolor="rgba(142, 168, 196, 0.22)",
            name="Model P25\u2013P75", hoverinfo="skip",
        ))

    # ── Adjusted bands ──────────────────────────────────────────────
    # Adjusted P10-P90 band (orange)
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

    # Adjusted P25-P75 band (orange, darker)
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

    # ── Median lines ────────────────────────────────────────────────
    # Model P50
    if "q_0.50" in base_df.columns:
        fig.add_trace(go.Scatter(
            x=hours, y=base_df["q_0.50"],
            mode="lines", name="Model P50",
            line=dict(color="#8ea8c4", width=2, dash="dash"),
            hovertemplate="HE %{x}<br>Model Median: $%{y:.1f}/MWh<extra></extra>",
        ))

    # Adjusted P50
    if "q_0.50" in adj_df.columns:
        fig.add_trace(go.Scatter(
            x=hours, y=adj_df["q_0.50"],
            mode="lines", name="Adj P50",
            line=dict(color="#FF6692", width=2, dash="dash"),
            hovertemplate="HE %{x}<br>Adj Median: $%{y:.1f}/MWh<extra></extra>",
        ))

    # ── Point forecasts ─────────────────────────────────────────────
    # Model forecast (thin gray dotted)
    fig.add_trace(go.Scatter(
        x=hours, y=base_df["point_forecast"],
        mode="lines", name="Model",
        line=dict(color="#8ea8c4", width=1.5, dash="dot"),
        hovertemplate="HE %{x}<br>Model: $%{y:.1f}/MWh<extra></extra>",
    ))

    # Adjusted forecast (bold orange)
    fig.add_trace(go.Scatter(
        x=hours, y=adj_df["point_forecast"],
        mode="lines+markers", name="Adjusted",
        line=dict(color="#FFA15A", width=3),
        marker=dict(size=5),
        hovertemplate="HE %{x}<br>Adjusted: $%{y:.1f}/MWh<extra></extra>",
    ))

    # Meteologica raw + adjusted
    if meteo_hourly:
        meteo_hours = sorted(meteo_hourly.keys())
        meteo_vals = [meteo_hourly[h] for h in meteo_hours]

        # Meteologica raw (blue, thin dashed)
        fig.add_trace(go.Scatter(
            x=meteo_hours, y=meteo_vals,
            mode="lines", name="Meteologica",
            line=dict(color="#60a5fa", width=1.5, dash="dash"),
            hovertemplate="HE %{x}<br>Meteo: $%{y:.1f}/MWh<extra></extra>",
        ))

        # Meteologica shifted (green, solid)
        delta_on = adj_info["onpeak_delta"]
        delta_off = adj_info["offpeak_delta"]
        meteo_adj_vals = []
        for h in meteo_hours:
            v = meteo_hourly[h]
            if pd.notna(v):
                delta = delta_on if h in ONPEAK_HOURS else delta_off
                meteo_adj_vals.append(float(v) + delta)
            else:
                meteo_adj_vals.append(None)

        fig.add_trace(go.Scatter(
            x=meteo_hours, y=meteo_adj_vals,
            mode="lines+markers", name="Meteo Adj",
            line=dict(color="#34d399", width=2.5),
            marker=dict(size=4, symbol="diamond"),
            hovertemplate="HE %{x}<br>Meteo Adj: $%{y:.1f}/MWh<extra></extra>",
        ))

    # Actuals if available
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
        title="Model vs Market-Adjusted Forecast",
        xaxis_title="Hour Ending",
        yaxis_title="$/MWh",
        height=550,
        template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="top", y=-0.12, xanchor="left", x=0),
        margin=dict(l=60, r=40, t=40, b=80),
        hovermode="x unified",
    )
    fig.update_xaxes(dtick=1, range=[0.5, 24.5])

    return fig.to_html(include_plotlyjs="cdn", full_html=False, div_id="mkt-plot")


def _error_html(msg: str) -> str:
    return f'<div style="padding:16px;color:#e74c3c;font-size:14px;">{msg}</div>'
