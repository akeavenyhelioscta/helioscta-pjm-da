"""Regional spark spread dashboard — hourly heat rate profiles overlaid by day.

Sections:
  1. Latest Day Summary — all hubs, on-peak heat rate + spark spread
  2. DA Heat Rate Hourly Profile — each day overlaid, one chart per focus hub
  3. RT Heat Rate Hourly Profile — same layout for real-time
  4. DA Spark Spread Hourly Profile — overlaid daily spark spreads
  5. RT Spark Spread Hourly Profile — same for RT
"""
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from src.like_day_forecast import configs
from src.data import lmps_hourly, gas_prices_hourly
from src.utils.cache_utils import pull_with_cache
from src.views.regional_spark_spreads import build_view_model

logger = logging.getLogger(__name__)

PLOTLY_TEMPLATE = "plotly_dark"
Section = tuple[str, Any, str | None]

DAY_COLORS = [
    "#636efa", "#EF553B", "#00cc96", "#ab63fa", "#FFA15A",
    "#19d3f3", "#FF6692", "#B6E880", "#FF97FF", "#FECB52",
    "#4cc9f0", "#f87171", "#34d399", "#fbbf24", "#a78bfa",
    "#fb923c", "#e879f9", "#6ee7b7", "#94a3b8", "#c084fc",
    "#60a5fa", "#f472b6", "#2dd4bf", "#facc15", "#818cf8",
    "#fb7185", "#a3e635", "#38bdf8", "#c4b5fd", "#fca5a5",
]

# Per PJM Market Monitor (State of the Market, Section 7 — Net Revenue)
# Reference heat rate: 7,000 Btu/kWh (7.0 MMBtu/MWh) combined cycle
FOCUS_HUBS = ["Western Hub", "Dominion", "Eastern Hub", "AEP Gen"]


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list[Section]:
    """Build regional spark spread report sections."""
    logger.info("Building regional spark spread report...")

    cache_kwargs = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    df_da = pull_with_cache(
        source_name="pjm_lmps_hourly_da",
        pull_fn=lmps_hourly.pull,
        pull_kwargs={"schema": schema, "market": "da"},
        **cache_kwargs,
    )
    df_rt = pull_with_cache(
        source_name="pjm_lmps_hourly_rt",
        pull_fn=lmps_hourly.pull,
        pull_kwargs={"schema": schema, "market": "rt"},
        **cache_kwargs,
    )
    df_gas = pull_with_cache(
        source_name="ice_gas_prices_hourly",
        pull_fn=gas_prices_hourly.pull,
        pull_kwargs={},
        **cache_kwargs,
    )

    vm = build_view_model(df_da, df_rt, df_gas, lookback_days=30)
    df_daily = vm["daily"]
    df_hourly = vm["hourly"]

    if df_daily.empty:
        return [("Regional Spark Spreads", _error("No data available."), None)]

    sections: list[Section] = []

    # Hourly overlaid profiles per hub
    for market in ["DA", "RT"]:
        for metric, label, unit, div_suffix in [
            ("heat_rate", "Implied Heat Rate", "MMBtu/MWh", "hr"),
            ("spark_7", "Spark Spread (7 HR CC)", "$/MWh", "spark"),
        ]:
            charts_html = _build_hub_charts(df_hourly, market, metric, label, unit, div_suffix)
            if charts_html:
                sections.append((
                    f"{market} {label} — Hourly Profiles",
                    charts_html,
                    None,
                ))

    return sections


def _summary_table_html(df: pd.DataFrame, target_date) -> str:
    """Latest day: one row per hub."""
    day = df[df["date"] == target_date].sort_values("lmp_onpeak", ascending=False)

    HUB_COLORS_MAP = {
        "Western Hub": "#FFA15A",
        "AEP Gen": "#60a5fa",
        "Dominion": "#fbbf24",
        "Eastern Hub": "#4cc9f0",
    }

    cols = ["Power Hub", "Gas Hub", "LMP OnPk", "LMP OffPk", "Gas OnPk", "Gas OffPk",
            "HR OnPk", "HR OffPk", "Spark OnPk", "Spark OffPk"]

    html = '<div style="overflow-x:auto;padding:8px;">'
    html += '<table style="width:100%;border-collapse:collapse;font-size:12px;font-family:monospace;">'
    html += "<thead><tr>"
    for col in cols:
        align = "text-align:left;" if "Hub" in col else ""
        html += (f'<th style="padding:6px 8px;background:#16263d;color:#e6efff;'
                 f'text-align:right;font-size:11px;{align}">{col}</th>')
    html += "</tr></thead><tbody>"

    for _, row in day.iterrows():
        hub = row["power_hub_display"]
        color = HUB_COLORS_MAP.get(hub, "#dbe7ff")
        is_western = hub == "Western Hub"
        weight = "font-weight:700;" if is_western else ""
        bg = "background:rgba(255,161,90,0.08);" if is_western else ""

        hr_onpk = row["heat_rate_onpeak"]
        hr_color = "#EF553B" if pd.notna(hr_onpk) and hr_onpk > 15 else "#00CC96" if pd.notna(hr_onpk) and hr_onpk < 10 else "#dbe7ff"
        spark_onpk = row["spark_7_onpeak"]
        spark_color = "#00CC96" if pd.notna(spark_onpk) and spark_onpk > 20 else "#EF553B" if pd.notna(spark_onpk) and spark_onpk < 0 else "#dbe7ff"

        html += f'<tr style="border-bottom:1px solid #1e3350;{bg}">'
        html += f'<td style="padding:5px 8px;color:{color};{weight}">{hub}</td>'
        html += f'<td style="padding:5px 8px;color:#8ea8c4;">{row["gas_hub_display"]}</td>'
        for val in [row["lmp_onpeak"], row["lmp_offpeak"]]:
            html += f'<td style="padding:5px 8px;text-align:right;color:#dbe7ff;{weight}">'
            html += f'${val:.2f}</td>' if pd.notna(val) else '\u2014</td>'
        for val in [row["gas_onpeak"], row["gas_offpeak"]]:
            html += f'<td style="padding:5px 8px;text-align:right;color:#8ea8c4;">'
            html += f'${val:.3f}</td>' if pd.notna(val) else '\u2014</td>'
        html += f'<td style="padding:5px 8px;text-align:right;color:{hr_color};{weight}">'
        html += f'{hr_onpk:.1f}</td>' if pd.notna(hr_onpk) else '\u2014</td>'
        hr_off = row["heat_rate_offpeak"]
        html += f'<td style="padding:5px 8px;text-align:right;color:#8ea8c4;">'
        html += f'{hr_off:.1f}</td>' if pd.notna(hr_off) else '\u2014</td>'
        html += f'<td style="padding:5px 8px;text-align:right;color:{spark_color};{weight}">'
        html += f'${spark_onpk:.2f}</td>' if pd.notna(spark_onpk) else '\u2014</td>'
        spark_off = row["spark_7_offpeak"]
        html += f'<td style="padding:5px 8px;text-align:right;color:#8ea8c4;">'
        html += f'${spark_off:.2f}</td>' if pd.notna(spark_off) else '\u2014</td>'
        html += "</tr>"

    html += "</tbody></table></div>"
    return html


def _build_hub_charts(
    df_hourly: pd.DataFrame, market: str, metric: str, label: str,
    unit: str, div_suffix: str,
) -> str:
    """Build a single subplot figure with 6 hub panels sharing one legend."""
    from plotly.subplots import make_subplots

    df = df_hourly[df_hourly["market"] == market].copy()
    if df.empty:
        return ""

    dates = sorted(df["date"].unique())
    last_3 = set(dates[-3:]) if len(dates) >= 3 else set(dates)

    # 2 rows x 3 cols
    active_hubs = [h for h in FOCUS_HUBS if h in df["power_hub_display"].unique()]
    n_hubs = len(active_hubs)
    if n_hubs == 0:
        return ""
    n_cols = min(3, n_hubs)
    n_rows = (n_hubs + n_cols - 1) // n_cols

    subtitles = []
    for hub_display in active_hubs:
        hub_data = df[df["power_hub_display"] == hub_display]
        gas_hub = hub_data.iloc[0]["gas_hub_display"] if len(hub_data) > 0 else ""
        subtitles.append(f"{hub_display} / {gas_hub}")

    fig = make_subplots(
        rows=n_rows, cols=n_cols,
        subplot_titles=subtitles,
        horizontal_spacing=0.06,
        vertical_spacing=0.12,
    )

    # Track which dates we've added to legend (only show once)
    legend_added = set()

    for hub_idx, hub_display in enumerate(active_hubs):
        row = hub_idx // n_cols + 1
        col = hub_idx % n_cols + 1

        hub_data = df[df["power_hub_display"] == hub_display]

        for i, d in enumerate(dates):
            day_data = hub_data[hub_data["date"] == d].sort_values("hour_ending")
            if day_data.empty:
                continue

            color = DAY_COLORS[i % len(DAY_COLORS)]
            dt_label = pd.Timestamp(d).strftime("%a %m/%d")
            is_last_3 = d in last_3
            is_latest = d == dates[-1]
            show_legend = dt_label not in legend_added

            fig.add_trace(
                go.Scatter(
                    x=day_data["hour_ending"],
                    y=day_data[metric],
                    mode="lines+markers" if is_latest else "lines",
                    name=dt_label,
                    legendgroup=dt_label,
                    showlegend=show_legend,
                    visible=True if is_last_3 else "legendonly",
                    line=dict(color=color, width=2.5 if is_latest else 1.5),
                    marker=dict(size=4) if is_latest else dict(size=0),
                    opacity=1.0 if is_last_3 else 0.6,
                    hovertemplate=f"{dt_label}<br>HE %{{x}}<br>{unit}: %{{y:.1f}}<extra></extra>",
                ),
                row=row, col=col,
            )
            if show_legend:
                legend_added.add(dt_label)

    # Update all axes
    for i in range(1, n_hubs + 1):
        fig.update_xaxes(dtick=4, range=[0.5, 24.5], row=(i - 1) // n_cols + 1, col=(i - 1) % n_cols + 1)

    div_id = f"{div_suffix}-{market.lower()}"
    fig.update_layout(
        height=350 * n_rows,
        template=PLOTLY_TEMPLATE,
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.05,
            xanchor="left",
            x=0,
            font=dict(size=10),
        ),
        margin=dict(l=50, r=20, t=30, b=80),
        hovermode="x unified",
    )

    return fig.to_html(include_plotlyjs="cdn", full_html=False, div_id=div_id)


def _error(msg: str) -> str:
    return f'<div style="padding:16px;color:#e74c3c;font-size:14px;">{msg}</div>'
