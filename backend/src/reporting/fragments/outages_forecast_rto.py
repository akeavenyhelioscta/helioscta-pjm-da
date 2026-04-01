"""Forecast Outages RTO — 4 heatmap vintage tables.

Renders one HTML heatmap table per outage type (Total, Forced, Planned, Maint).
Rows = forecast execution dates (labeled Current Forecast, 24hrs Ago, etc.).
Columns = forecast dates (next 7 days).
Cells = MW with green-to-red conditional coloring (green = high, red = low).
"""
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from src.like_day_forecast import configs
from src.data import outages_forecast_daily
from src.utils.cache_utils import pull_with_cache

logger = logging.getLogger(__name__)

Section = tuple[str, Any, str | None]

_OUTAGE_TYPES = [
    ("Total Outages", "total_outages_mw"),
    ("Forced Outages", "forced_outages_mw"),
    ("Planned Outages", "planned_outages_mw"),
    ("Maint Outages", "maintenance_outages_mw"),
]


# ── Public entry point ───────────────────────────────────────────────


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list:
    """Build Forecast Outages RTO report — 4 heatmap vintage tables."""
    logger.info("Building Forecast Outages RTO report...")

    cache_kwargs = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    df = _safe_pull(
        "pjm_outages_forecast_daily",
        outages_forecast_daily.pull,
        {"lookback_days": 14},
        **cache_kwargs,
    )

    if df is None or len(df) == 0:
        return [("Forecast Outages RTO", _empty("No outage forecast data available."), None)]

    # Filter to RTO region
    df = df[df["region"] == configs.LOAD_REGION].copy()

    # Keep only the highest-rank (most recent) forecast per execution_date × forecast_date
    df = df.sort_values("forecast_rank", ascending=False)
    df = df.drop_duplicates(subset=["forecast_execution_date", "forecast_date"], keep="first")

    # Get the last 8 execution dates
    exec_dates = sorted(df["forecast_execution_date"].unique(), reverse=True)[:8]
    df = df[df["forecast_execution_date"].isin(exec_dates)].copy()

    if len(df) == 0:
        return [("Forecast Outages RTO", _empty("No recent forecast data."), None)]

    # Label execution dates
    df["exec_label"] = df["forecast_execution_date"].apply(
        lambda d: _label_exec_date(d, exec_dates)
    )

    sections: list = []
    for type_label, col in _OUTAGE_TYPES:
        html = _render_heatmap_table(df, col, type_label, exec_dates)
        sections.append((type_label, html, None))

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


def _label_exec_date(d, exec_dates_sorted: list) -> str:
    """Label execution dates: Current Forecast, 24hrs Ago, or day-of-week date."""
    idx = exec_dates_sorted.index(d)
    if idx == 0:
        return "Current Forecast"
    elif idx == 1:
        return "24hrs Ago"
    else:
        return pd.Timestamp(d).strftime("%a %b-%d")


def _heatmap_color(value: float, vmin: float, vmax: float) -> str:
    """Map value to green (high) → red (low) background color.

    Green = high outages, Red = low outages (matching PDF convention).
    """
    if pd.isna(value) or vmax == vmin:
        return "transparent"

    # Normalize 0 (low/red) to 1 (high/green)
    t = (value - vmin) / (vmax - vmin)

    # Red (low) → Yellow (mid) → Green (high)
    if t < 0.5:
        # Red to Yellow
        ratio = t / 0.5
        r, g, b = 200, int(60 + 140 * ratio), 60
    else:
        # Yellow to Green
        ratio = (t - 0.5) / 0.5
        r, g, b = int(200 - 140 * ratio), 200, int(60 + 40 * ratio)

    return f"rgba({r}, {g}, {b}, 0.55)"


def _render_heatmap_table(
    df: pd.DataFrame,
    value_col: str,
    type_label: str,
    exec_dates_sorted: list,
) -> str:
    """Build an HTML heatmap table for one outage type."""
    # Pivot: rows = execution dates (newest first), columns = forecast dates
    forecast_dates = sorted(df["forecast_date"].unique())
    vmin = df[value_col].min()
    vmax = df[value_col].max()

    # Header row
    header_cells = (
        '<th class="oh-hdr oh-sticky-col">Forecast Exec.</th>'
        '<th class="oh-hdr">Forecast Label</th>'
    )
    for fd in forecast_dates:
        label = pd.Timestamp(fd).strftime("%a %b-%d")
        header_cells += f'<th class="oh-hdr">{label}</th>'

    # Data rows — newest execution date first
    rows_html = []
    for exec_dt in exec_dates_sorted:
        exec_label = _label_exec_date(exec_dt, exec_dates_sorted)
        exec_str = pd.Timestamp(exec_dt).strftime("%a %b-%d")

        cells = f'<td class="oh-dt oh-sticky-col">{exec_str}</td>'
        cells += f'<td class="oh-label">{exec_label}</td>'

        row_data = df[df["forecast_execution_date"] == exec_dt]
        for fd in forecast_dates:
            match = row_data[row_data["forecast_date"] == fd]
            if len(match) == 0:
                cells += '<td class="oh-cell oh-empty"></td>'
            else:
                val = match.iloc[0][value_col]
                if pd.isna(val):
                    cells += '<td class="oh-cell oh-empty"></td>'
                else:
                    bg = _heatmap_color(val, vmin, vmax)
                    cells += (
                        f'<td class="oh-cell" style="background:{bg};">'
                        f'{val:,.0f}</td>'
                    )

        rows_html.append(f"<tr>{cells}</tr>")

    table_html = f"""
<div class="oh-section-title">{type_label}</div>
<div class="oh-wrap">
<table class="oh-table">
<thead><tr>{header_cells}</tr></thead>
<tbody>
<tr><td colspan="{len(forecast_dates) + 2}" class="oh-sub-hdr">Forecast Date (Forecasts)</td></tr>
{"".join(rows_html)}
</tbody>
</table>
</div>
"""
    return _STYLE + table_html


# ── CSS ──────────────────────────────────────────────────────────────

_STYLE = """
<style>
.oh-section-title {
    font-family: 'IBM Plex Sans', 'Segoe UI', Tahoma, sans-serif;
    font-size: 14px;
    font-weight: 700;
    color: #c5d8f2;
    padding: 14px 12px 6px;
    letter-spacing: 0.3px;
}
.oh-wrap {
    overflow-x: auto;
    padding: 0 12px 16px;
}
.oh-table {
    border-collapse: collapse;
    width: 100%;
    font-family: 'IBM Plex Sans', 'Segoe UI', Tahoma, sans-serif;
    font-size: 13px;
}
.oh-table th, .oh-table td {
    padding: 5px 12px;
    text-align: right;
    white-space: nowrap;
}
.oh-hdr {
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
.oh-sub-hdr {
    text-align: center;
    color: #6f8db1;
    font-size: 11px;
    font-weight: 600;
    background: #0e1520;
    border-bottom: 1px solid #1a2a42;
    padding: 4px 0;
}
.oh-dt {
    text-align: left;
    color: #9eb4d3;
    font-weight: 500;
    font-size: 12px;
    background: #0e1520;
}
.oh-sticky-col {
    position: sticky;
    left: 0;
    z-index: 1;
}
.oh-label {
    text-align: left;
    color: #7a94b5;
    font-size: 12px;
    font-style: italic;
}
.oh-cell {
    color: #e0e0e0;
    font-variant-numeric: tabular-nums;
    font-size: 12px;
    font-weight: 500;
    border-radius: 2px;
}
.oh-empty {
    color: #4a5568;
}
.oh-table tbody tr {
    border-bottom: 1px solid #1a2a42;
}
.oh-table tbody tr:hover {
    outline: 1px solid #4a6a8f;
}
.oh-table tbody tr:hover .oh-dt {
    background: #1a2640;
}
</style>
"""
