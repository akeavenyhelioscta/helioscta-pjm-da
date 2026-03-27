"""Outages Term Bible — Year-Month heatmap, stats, and daily values.

For each outage type (Total, Forced, Planned, Maint), renders:
  1. Year-Month heatmap table with Yearly Stats (Avg/Max/Min)
  2. Monthly Stats row (Avg/Max/Min)
  3. Daily values table for current month (one column per year)
  4. Daily values Plotly line chart for current month (one line per year)
"""
import logging
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go

from src.like_day_forecast import configs
from src.data import outages_actual_daily
from src.utils.cache_utils import pull_with_cache
from src.views.outage_term_bible import build_view_model

logger = logging.getLogger(__name__)

PLOTLY_TEMPLATE = "plotly_dark"
HISTORY_START = "2023-01-01"

Section = tuple[str, Any, str | None]

_OUTAGE_TYPES = [
    ("Total Outages", "total_outages_mw"),
    ("Forced Outages", "forced_outages_mw"),
    ("Planned Outages", "planned_outages_mw"),
    ("Maint Outages", "maintenance_outages_mw"),
]

_MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

_YEAR_COLORS = [
    "#00cc96", "#636efa", "#ef553b", "#ab63fa", "#ffa15a", "#19d3f3", "#ff6692",
]


# ── Public entry point ───────────────────────────────────────────────


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list:
    """Build Term Bible report — heatmap + daily table/chart per outage type."""
    logger.info("Building Outages Term Bible report...")

    cache_kwargs = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    df = _safe_pull(
        "outages_actual_daily_history",
        outages_actual_daily.pull,
        {"sql_overrides": {"start_date": HISTORY_START}},
        **cache_kwargs,
    )

    if df is None or len(df) == 0:
        return [("Term Bible", _empty("No historical outage data available."), None)]

    # Build structured view model (domain interpretation layer)
    vm = build_view_model(df)
    total = vm.get("outage_types", {}).get("total_outages", {})
    ctx = total.get("month_context", {})
    logger.info(f"View model: {vm['current_month']}, "
                f"total={total.get('current_mw', '?')} MW, "
                f"percentile={ctx.get('percentile', '?')}")

    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day

    sections: list = []
    for type_label, col in _OUTAGE_TYPES:
        sections.append(f"--- {type_label} ---")
        sections.append((f"{type_label} — Year-Month", _render_year_month_heatmap(df, col, type_label), None))
        sections.append((f"{type_label} — Daily Values", _render_daily_table(df, col, type_label), None))
        sections.append((f"{type_label} — Daily Chart", _render_daily_chart(df, col, type_label), None))

    return sections


# ── Helpers ──────────────────────────────────────────────────────────


def _safe_pull(source_name, pull_fn, pull_kwargs, **cache_kwargs):
    try:
        return pull_with_cache(
            source_name=source_name,
            pull_fn=pull_fn,
            pull_kwargs=pull_kwargs,
            **cache_kwargs,
        )
    except Exception as e:
        logger.warning(f"{source_name} pull failed: {e}")
        return None


def _empty(text: str) -> str:
    return f"<div style='padding:16px;color:#e74c3c;'>{text}</div>"


def _heatmap_color(value: float, vmin: float, vmax: float) -> str:
    """Green (high) → Red (low) background color."""
    if pd.isna(value) or vmax == vmin:
        return "transparent"

    t = (value - vmin) / (vmax - vmin)

    if t < 0.5:
        ratio = t / 0.5
        r, g, b = 200, int(60 + 140 * ratio), 60
    else:
        ratio = (t - 0.5) / 0.5
        r, g, b = int(200 - 140 * ratio), 200, int(60 + 40 * ratio)

    return f"rgba({r}, {g}, {b}, 0.55)"


# ── Section 1: Year-Month Heatmap ────────────────────────────────────


def _render_year_month_heatmap(df: pd.DataFrame, col: str, type_label: str) -> str:
    """Year × Month heatmap with monthly stats row and yearly stats columns."""
    # Compute monthly averages
    monthly = df.groupby(["year", "month"])[col].mean().reset_index()
    pivot = monthly.pivot(index="year", columns="month", values=col)

    years = sorted(pivot.index)
    vmin = monthly[col].min()
    vmax = monthly[col].max()

    # Yearly stats
    yearly_stats = df.groupby("year")[col].agg(["mean", "max", "min"])

    # Monthly stats (across all years)
    monthly_stats = monthly.groupby("month")[col].agg(["mean", "max", "min"])

    # Header
    header = '<th class="tb-hdr"></th>'
    for m in range(1, 13):
        header += f'<th class="tb-hdr">{_MONTHS[m - 1][:3]}</th>'
    header += '<th class="tb-hdr">Yearly Avg</th><th class="tb-hdr">Yearly Max</th><th class="tb-hdr">Yearly Min</th>'

    # Data rows
    rows_html = []
    for year in years:
        cells = f'<td class="tb-year">{year}</td>'
        for m in range(1, 13):
            val = pivot.loc[year, m] if m in pivot.columns and year in pivot.index and pd.notna(pivot.loc[year].get(m)) else None
            if val is None:
                cells += '<td class="tb-cell tb-empty"></td>'
            else:
                bg = _heatmap_color(val, vmin, vmax)
                cells += f'<td class="tb-cell" style="background:{bg};">{val:,.0f}</td>'

        # Yearly stats
        if year in yearly_stats.index:
            for stat in ["mean", "max", "min"]:
                v = yearly_stats.loc[year, stat]
                cells += f'<td class="tb-stat">{v:,.0f}</td>'
        else:
            cells += '<td class="tb-stat"></td>' * 3

        rows_html.append(f"<tr>{cells}</tr>")

    # Monthly stats rows
    stats_html = ""
    for stat_label, stat_key in [("Monthly Avg", "mean"), ("Monthly Max", "max"), ("Monthly Min", "min")]:
        cells = f'<td class="tb-year tb-stat-label">{stat_label}</td>'
        for m in range(1, 13):
            if m in monthly_stats.index:
                v = monthly_stats.loc[m, stat_key]
                cells += f'<td class="tb-stat">{v:,.0f}</td>'
            else:
                cells += '<td class="tb-stat"></td>'
        cells += '<td class="tb-stat"></td>' * 3
        stats_html += f"<tr>{cells}</tr>"

    return _TB_STYLE + f"""
<div class="tb-wrap">
<table class="tb-table">
<thead><tr>{header}</tr></thead>
<tbody>
{"".join(rows_html)}
<tr class="tb-stats-divider"><td colspan="{16}"></td></tr>
{stats_html}
</tbody>
</table>
</div>
"""


# ── Section 2: Daily Values Table ────────────────────────────────────


def _render_daily_table(df: pd.DataFrame, col: str, type_label: str) -> str:
    """Daily values table for current month — one column per year."""
    current_month = date.today().month
    month_data = df[df["month"] == current_month].copy()

    if len(month_data) == 0:
        return _empty(f"No data for {_MONTHS[current_month - 1]}.")

    years = sorted(month_data["year"].unique())
    pivot = month_data.pivot_table(index="day", columns="year", values=col, aggfunc="first")

    vmin = month_data[col].min()
    vmax = month_data[col].max()

    # Grand total row
    grand_totals = month_data.groupby("year")[col].mean()

    # Header
    header = '<th class="tb-hdr"></th>'
    for y in years:
        header += f'<th class="tb-hdr">{y}</th>'

    # Grand total row
    gt_cells = '<td class="tb-year tb-stat-label">Grand Total</td>'
    for y in years:
        v = grand_totals.get(y)
        if v is not None and pd.notna(v):
            gt_cells += f'<td class="tb-stat">{v:,.0f}</td>'
        else:
            gt_cells += '<td class="tb-stat"></td>'

    # Day rows
    rows_html = []
    all_days = sorted(pivot.index)
    for day in all_days:
        month_abbr = _MONTHS[current_month - 1][:3]
        cells = f'<td class="tb-year">{month_abbr}-{day:02d}</td>'
        for y in years:
            val = pivot.loc[day].get(y) if y in pivot.columns else None
            if val is None or pd.isna(val):
                cells += '<td class="tb-cell tb-empty"></td>'
            else:
                bg = _heatmap_color(val, vmin, vmax)
                cells += f'<td class="tb-cell" style="background:{bg};">{val:,.0f}</td>'
        rows_html.append(f"<tr>{cells}</tr>")

    return _TB_STYLE + f"""
<div class="tb-section-title">Table for Current Month — {_MONTHS[current_month - 1]}</div>
<div class="tb-wrap">
<table class="tb-table">
<thead><tr>{header}</tr></thead>
<tbody>
<tr>{gt_cells}</tr>
<tr class="tb-stats-divider"><td colspan="{len(years) + 1}"></td></tr>
{"".join(rows_html)}
</tbody>
</table>
</div>
"""


# ── Section 3: Daily Values Chart ────────────────────────────────────


def _render_daily_chart(df: pd.DataFrame, col: str, type_label: str) -> go.Figure:
    """Plotly line chart of daily values for current month, one line per year."""
    current_month = date.today().month
    month_data = df[df["month"] == current_month].copy()

    if len(month_data) == 0:
        fig = go.Figure()
        fig.update_layout(
            title=f"No data for {_MONTHS[current_month - 1]}",
            template=PLOTLY_TEMPLATE,
        )
        return fig

    years = sorted(month_data["year"].unique())

    fig = go.Figure()
    for i, year in enumerate(years):
        yr_data = month_data[month_data["year"] == year].sort_values("day")
        color = _YEAR_COLORS[i % len(_YEAR_COLORS)]
        fig.add_trace(go.Scatter(
            x=yr_data["day"],
            y=yr_data[col],
            mode="lines+markers",
            name=str(year),
            line=dict(color=color, width=2),
            marker=dict(size=4),
        ))

    month_name = _MONTHS[current_month - 1]
    fig.update_layout(
        title=f"{type_label} — {month_name} Daily Comparison",
        xaxis_title=f"Day of {month_name}",
        yaxis_title="MW",
        height=450,
        template=PLOTLY_TEMPLATE,
        legend=dict(font=dict(size=11)),
        xaxis=dict(dtick=1),
    )

    return fig


# ── CSS ──────────────────────────────────────────────────────────────

_TB_STYLE = """
<style>
.tb-section-title {
    font-family: 'IBM Plex Sans', 'Segoe UI', Tahoma, sans-serif;
    font-size: 13px;
    font-weight: 600;
    color: #8db4e0;
    padding: 12px 12px 4px;
}
.tb-wrap {
    overflow-x: auto;
    padding: 0 12px 12px;
}
.tb-table {
    border-collapse: collapse;
    width: 100%;
    font-family: 'IBM Plex Sans', 'Segoe UI', Tahoma, sans-serif;
    font-size: 12px;
}
.tb-table th, .tb-table td {
    padding: 4px 10px;
    text-align: right;
    white-space: nowrap;
}
.tb-hdr {
    background: #141e30;
    color: #8db4e0;
    font-weight: 600;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.4px;
    border-bottom: 2px solid #2a3f60;
    position: sticky;
    top: 0;
    z-index: 2;
}
.tb-year {
    text-align: left;
    color: #9eb4d3;
    font-weight: 600;
    font-size: 12px;
    background: #0e1520;
    position: sticky;
    left: 0;
    z-index: 1;
}
.tb-stat-label {
    font-style: italic;
    color: #7a94b5;
}
.tb-cell {
    color: #e0e0e0;
    font-variant-numeric: tabular-nums;
    font-weight: 500;
    border-radius: 2px;
}
.tb-stat {
    color: #b0c4de;
    font-variant-numeric: tabular-nums;
    font-weight: 500;
    font-size: 11px;
}
.tb-empty {
    color: #4a5568;
}
.tb-table tbody tr {
    border-bottom: 1px solid #1a2a42;
}
.tb-table tbody tr:hover {
    outline: 1px solid #4a6a8f;
}
.tb-stats-divider td {
    border-top: 2px solid #2a3f60;
    padding: 0;
    height: 4px;
}
</style>
"""
