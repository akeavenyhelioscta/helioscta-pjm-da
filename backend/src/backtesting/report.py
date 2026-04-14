"""HTML report generation for backtest results.

Produces a per-day quantile-bands report matching the existing forecast
dashboard style: for each (model, date) combination, shows a pivoted
bands table and a Plotly chart with shaded quantile regions.
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from src.backtesting.engine import BacktestResult
from src.reporting.html_dashboard import HTMLDashboardBuilder

logger = logging.getLogger(__name__)

PLOTLY_TEMPLATE = "plotly_dark"
ONPEAK_HOURS = list(range(8, 24))   # HE8-HE23
OFFPEAK_HOURS = list(range(1, 8)) + [24]  # HE1-HE7, HE24
HE_COLS = [f"HE{h}" for h in range(1, 25)]
SUMMARY_COLS = ["OnPeak", "OffPeak", "Flat"]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_backtest_report(
    result: BacktestResult,
    title: str = "Backtest Report",
) -> str:
    """Build a self-contained HTML dashboard from backtest results."""
    builder = HTMLDashboardBuilder(title=title, theme="dark")

    # Summary header
    builder.add_content("Summary", _build_summary_html(result))

    # Per-model, per-date sections
    if len(result.hourly_predictions) > 0:
        df_h = result.hourly_predictions
        models = sorted(df_h["model"].unique())

        for model in models:
            builder.add_divider(model)
            df_model = df_h[df_h["model"] == model]
            dates = sorted(df_model["forecast_date"].unique(), reverse=True)

            for fdate in dates:
                df_day = df_model[df_model["forecast_date"] == fdate].copy()
                df_day = df_day.sort_values("hour_ending")

                # Extract reference_date for walk-forward verification
                ref_date = None
                if "reference_date" in df_day.columns:
                    ref_vals = df_day["reference_date"].dropna().unique()
                    if len(ref_vals) > 0:
                        ref_date = ref_vals[0]

                label = f"{model} — {fdate}"

                # Combined table + chart as raw HTML
                section_html = _build_day_header_html(fdate, ref_date, model)
                section_html += _build_day_table_html(df_day, fdate)
                section_html += _build_day_chart_html(df_day, fdate)
                builder.add_content(label, section_html)

    # Aggregate metrics at the bottom (on-peak focus)
    if len(result.aggregate_metrics) > 0:
        builder.add_divider("Aggregate Metrics")
        df_agg = result.aggregate_metrics.copy()
        if "period" in df_agg.columns:
            df_agg_onpeak = df_agg[df_agg["period"] == "on_peak"]
            if len(df_agg_onpeak) > 0:
                builder.add_content("Aggregate Metrics (On-Peak)", df_agg_onpeak)
            else:
                builder.add_content("Aggregate Metrics", df_agg)
        else:
            builder.add_content("Aggregate Metrics", df_agg)

    return builder.build()


def write_backtest_report(
    result: BacktestResult,
    path: Path,
    title: str = "Backtest Report",
) -> Path:
    """Build HTML report and write to file."""
    html = build_backtest_report(result, title=title)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------


def _build_summary_html(result: BacktestResult) -> str:
    if len(result.run_metadata) == 0:
        return "<p>No run metadata available.</p>"

    rm = result.run_metadata
    start = str(rm["forecast_date"].min())
    end = str(rm["forecast_date"].max())
    models = ", ".join(sorted(rm["model"].unique()))
    ok = int((rm["status"] == "ok").sum())
    total = len(rm)
    n_days = rm["forecast_date"].nunique()

    card_style = (
        "display:inline-block; background:#132138; border-radius:8px; "
        "padding:14px 22px; margin:6px 10px 6px 0; min-width:140px; "
        "text-align:center;"
    )
    label_style = "font-size:0.8em; color:#a6bad6; margin-bottom:4px;"
    value_style = "font-size:1.4em; color:#dbe7ff; font-weight:600;"

    def _card(label: str, value: str) -> str:
        return (
            f'<div style="{card_style}">'
            f'<div style="{label_style}">{label}</div>'
            f'<div style="{value_style}">{value}</div>'
            f"</div>"
        )

    cards = "".join([
        _card("Date Range", f"{start} &rarr; {end}"),
        _card("Models", models),
        _card("Success Rate", f"{ok}/{total}"),
        _card("Forecast Days", str(n_days)),
    ])
    return f'<div style="display:flex; flex-wrap:wrap;">{cards}</div>'


# ---------------------------------------------------------------------------
# Per-day header with reference date
# ---------------------------------------------------------------------------


def _build_day_header_html(fdate, ref_date, model: str) -> str:
    """Render forecast/reference date header for walk-forward verification."""
    ref_str = str(ref_date) if ref_date is not None else "unknown"
    return (
        f'<div style="margin-bottom:8px;font-family:monospace;font-size:13px;">'
        f'<span style="color:#e6efff;font-weight:600;">Forecast: {fdate}</span>'
        f'<span style="color:#a6bad6;margin-left:16px;">Reference: {ref_str}</span>'
        f'<span style="color:#6f8db1;margin-left:16px;font-size:11px;">'
        f'(model used data through {ref_str} only)</span>'
        f'</div>'
    )


# ---------------------------------------------------------------------------
# Per-day quantile bands table
# ---------------------------------------------------------------------------


def _fmt(v: float | None, decimals: int = 1) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "-"
    return f"{v:.{decimals}f}"


def _err_bg(v: float | None) -> str:
    """Background color based on absolute error magnitude."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "transparent"
    a = abs(v)
    if a < 5:
        return "rgba(0, 204, 150, 0.12)"
    if a < 15:
        return "rgba(255, 161, 90, 0.15)"
    return "rgba(239, 85, 59, 0.18)"


def _period_avg(vals: dict[int, float], hours: list[int]) -> float | None:
    """Average value over a set of hours."""
    selected = [vals[h] for h in hours if h in vals and vals[h] is not None
                and not (isinstance(vals[h], float) and np.isnan(vals[h]))]
    return float(np.mean(selected)) if selected else None


def _build_day_table_html(df_day: pd.DataFrame, fdate) -> str:
    """Pivoted quantile bands + forecast/actual/error table."""
    # Detect available quantile columns
    q_cols = sorted([c for c in df_day.columns if c.startswith("q_")])
    q_labels = {c: f"P{int(float(c[2:]) * 100)}" for c in q_cols}

    # Build hour-keyed dicts
    def _he_dict(col: str) -> dict[int, float]:
        return {int(r["hour_ending"]): r[col] for _, r in df_day.iterrows()
                if pd.notna(r.get(col))}

    forecast_by_he = _he_dict("forecast")
    actual_by_he = _he_dict("actual")
    error_by_he = _he_dict("error")
    q_by_he: dict[str, dict[int, float]] = {qc: _he_dict(qc) for qc in q_cols}

    # Collect rows: each is (label, color, bg, {HE1..HE24, OnPeak, OffPeak, Flat})
    rows: list[tuple[str, str, str, dict[str, float | None]]] = []

    # Quantile band rows
    for idx, qc in enumerate(q_cols):
        label = q_labels[qc]
        vals = q_by_he[qc]
        row_vals = {f"HE{h}": vals.get(h) for h in range(1, 25)}
        row_vals["OnPeak"] = _period_avg(vals, ONPEAK_HOURS)
        row_vals["OffPeak"] = _period_avg(vals, OFFPEAK_HOURS)
        row_vals["Flat"] = _period_avg(vals, list(range(1, 25)))
        bg = "rgba(22, 38, 61, 0.5)" if idx % 2 == 0 else "transparent"
        rows.append((label, "#a6bad6", bg, row_vals))

    # Forecast row
    fc_vals = {f"HE{h}": forecast_by_he.get(h) for h in range(1, 25)}
    fc_vals["OnPeak"] = _period_avg(forecast_by_he, ONPEAK_HOURS)
    fc_vals["OffPeak"] = _period_avg(forecast_by_he, OFFPEAK_HOURS)
    fc_vals["Flat"] = _period_avg(forecast_by_he, list(range(1, 25)))
    rows.append(("Forecast", "#FFA15A", "rgba(255, 161, 90, 0.08)", fc_vals))

    # Actual row
    has_actuals = len(actual_by_he) > 0
    if has_actuals:
        act_vals = {f"HE{h}": actual_by_he.get(h) for h in range(1, 25)}
        act_vals["OnPeak"] = _period_avg(actual_by_he, ONPEAK_HOURS)
        act_vals["OffPeak"] = _period_avg(actual_by_he, OFFPEAK_HOURS)
        act_vals["Flat"] = _period_avg(actual_by_he, list(range(1, 25)))
        rows.append(("Actual", "#4cc9f0", "rgba(76, 201, 240, 0.08)", act_vals))

    # Error row
    if has_actuals:
        err_vals = {f"HE{h}": error_by_he.get(h) for h in range(1, 25)}
        err_vals["OnPeak"] = _period_avg(error_by_he, ONPEAK_HOURS)
        err_vals["OffPeak"] = _period_avg(error_by_he, OFFPEAK_HOURS)
        err_vals["Flat"] = _period_avg(error_by_he, list(range(1, 25)))
        rows.append(("Error", "#e6efff", "rgba(14, 25, 42, 0.6)", err_vals))

    # Build HTML table
    cols = ["Date", "Band"] + HE_COLS + SUMMARY_COLS
    html = '<div style="overflow-x:auto;padding:8px;">'
    html += (
        '<table style="width:100%;border-collapse:collapse;'
        'font-size:12px;font-family:monospace;">'
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

    # Data rows
    for label, color, bg, vals in rows:
        tr_style = f"background:{bg};"
        if label == "Forecast":
            tr_style += "border-top:3px solid #3a5070;"
        if label == "Error":
            tr_style += "border-top:1px dashed #3a5070;"
        html += f'<tr style="{tr_style}">'

        # Date cell
        html += (
            f'<td style="padding:5px 8px;border-bottom:1px solid #1e3350;'
            f'text-align:left;color:#a6bad6;">{fdate}</td>'
        )
        # Band cell
        html += (
            f'<td style="padding:5px 8px;border-bottom:1px solid #1e3350;'
            f'text-align:left;color:{color};font-weight:600;">{label}</td>'
        )
        # Value cells
        for col in HE_COLS + SUMMARY_COLS:
            v = vals.get(col)
            cell_bg = _err_bg(v) if label == "Error" else "transparent"
            sign = ""
            if label == "Error" and v is not None and not np.isnan(v):
                sign = "+" if v > 0 else ""
            html += (
                f'<td style="padding:5px 8px;border-bottom:1px solid #1e3350;'
                f'text-align:right;color:{color};background:{cell_bg};">'
                f'{sign}{_fmt(v)}</td>'
            )
        html += "</tr>"

    html += "</tbody></table>"

    # Metrics summary line (on-peak focus: HE8-HE23)
    if has_actuals:
        onpk_errors = [error_by_he[h] for h in ONPEAK_HOURS
                       if h in error_by_he and error_by_he[h] is not None]
        onpk_actuals = [actual_by_he[h] for h in ONPEAK_HOURS
                        if h in actual_by_he and actual_by_he[h] is not None]
        if onpk_errors and onpk_actuals:
            arr = np.array(onpk_errors, dtype=float)
            act = np.array(onpk_actuals, dtype=float)
            mae = float(np.mean(np.abs(arr)))
            rmse = float(np.sqrt(np.mean(arr ** 2)))
            bias = float(np.mean(arr))
            mape = float(np.mean(np.abs(arr / act) * 100))
            bias_sign = "+" if bias > 0 else ""
            html += (
                f'<div style="margin-top:6px;font-size:12px;font-family:monospace;color:#a6bad6;">'
                f'  <span style="color:#6f8db1;">[On-Peak HE8\u201323]</span>'
                f'  MAE: <b style="color:#e6efff;">${mae:.2f}/MWh</b>'
                f'  |  RMSE: <b style="color:#e6efff;">${rmse:.2f}/MWh</b>'
                f'  |  MAPE: <b style="color:#e6efff;">{mape:.1f}%</b>'
                f'  |  Bias: <b style="color:{"#00CC96" if abs(bias) < 5 else "#FFA15A" if abs(bias) < 15 else "#EF553B"};">'
                f'{bias_sign}{bias:.2f}</b>'
                f'</div>'
            )

    html += "</div>"
    return html


# ---------------------------------------------------------------------------
# Per-day quantile bands chart
# ---------------------------------------------------------------------------


def _build_day_chart_html(df_day: pd.DataFrame, fdate) -> str:
    """Plotly chart with shaded quantile bands, forecast, and actual."""
    hours = df_day["hour_ending"].values
    fig = go.Figure()

    # P10-P90 outer band
    if "q_0.10" in df_day.columns and "q_0.90" in df_day.columns:
        fig.add_trace(go.Scatter(
            x=hours, y=df_day["q_0.90"],
            mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip",
        ))
        fig.add_trace(go.Scatter(
            x=hours, y=df_day["q_0.10"],
            mode="lines", line=dict(width=0),
            fill="tonexty", fillcolor="rgba(99, 110, 250, 0.10)",
            name="P10\u2013P90", hoverinfo="skip",
        ))

    # P25-P75 inner band
    if "q_0.25" in df_day.columns and "q_0.75" in df_day.columns:
        fig.add_trace(go.Scatter(
            x=hours, y=df_day["q_0.75"],
            mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip",
        ))
        fig.add_trace(go.Scatter(
            x=hours, y=df_day["q_0.25"],
            mode="lines", line=dict(width=0),
            fill="tonexty", fillcolor="rgba(99, 110, 250, 0.25)",
            name="P25\u2013P75", hoverinfo="skip",
        ))

    # P50 median
    if "q_0.50" in df_day.columns:
        fig.add_trace(go.Scatter(
            x=hours, y=df_day["q_0.50"],
            mode="lines", name="P50 (Median)",
            line=dict(color="#AB63FA", width=2, dash="dash"),
            hovertemplate="HE %{x}<br>Median: $%{y:.1f}/MWh<extra></extra>",
        ))

    # Forecast line
    fig.add_trace(go.Scatter(
        x=hours, y=df_day["forecast"],
        mode="lines+markers", name="Forecast",
        line=dict(color="#FFA15A", width=2.5),
        marker=dict(size=5),
        hovertemplate="HE %{x}<br>Forecast: $%{y:.1f}/MWh<extra></extra>",
    ))

    # Actual line + outlier markers
    if df_day["actual"].notna().any():
        fig.add_trace(go.Scatter(
            x=hours, y=df_day["actual"],
            mode="lines+markers", name="Actual",
            line=dict(color="#4cc9f0", width=2),
            marker=dict(size=5),
            hovertemplate="HE %{x}<br>Actual: $%{y:.1f}/MWh<extra></extra>",
        ))

        # Outlier markers: actual outside P25-P75
        if "q_0.25" in df_day.columns and "q_0.75" in df_day.columns:
            outlier_h, outlier_v = [], []
            for _, r in df_day.iterrows():
                a, lo, hi = r.get("actual"), r.get("q_0.25"), r.get("q_0.75")
                if pd.notna(a) and pd.notna(lo) and pd.notna(hi) and (a < lo or a > hi):
                    outlier_h.append(r["hour_ending"])
                    outlier_v.append(a)
            if outlier_h:
                fig.add_trace(go.Scatter(
                    x=outlier_h, y=outlier_v,
                    mode="markers", name="Outside P25\u2013P75",
                    marker=dict(
                        color="#EF553B", size=10, symbol="diamond",
                        line=dict(width=1.5, color="#EF553B"),
                    ),
                    hovertemplate="HE %{x}<br>Actual: $%{y:.1f}/MWh<br><b>Outside IQR</b><extra></extra>",
                ))

    fig.update_layout(
        title=f"Quantile Bands vs Actual & Forecast — {fdate}",
        xaxis_title="Hour Ending",
        yaxis_title="$/MWh",
        height=500,
        template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="top", y=-0.12, xanchor="left", x=0),
        margin=dict(l=60, r=40, t=40, b=70),
        hovermode="x unified",
    )
    fig.update_xaxes(dtick=1, range=[0.5, 24.5])

    return fig.to_html(include_plotlyjs="cdn", full_html=False)
