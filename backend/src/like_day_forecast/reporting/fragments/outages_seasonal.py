"""Seasonal Outages RTO — 4 yearly overlay charts.

For each outage type (Total, Forced, Planned, Maint), renders a Plotly chart
with one line per year overlaid on a day-of-year x-axis. Month boundaries and
WINTER/SUMMER season labels are shown as vertical lines and annotations.
"""
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go

from src.like_day_forecast import configs
from src.like_day_forecast.data import outages_actual_daily
from src.like_day_forecast.utils.cache_utils import pull_with_cache

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

_YEAR_COLORS = [
    "#00cc96", "#636efa", "#ef553b", "#ab63fa", "#ffa15a", "#19d3f3", "#ff6692",
]

_MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

# Month day-of-year boundaries (non-leap year approximation)
_MONTH_STARTS = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]

# Seasons: Nov-Mar = WINTER, Apr-Oct = SUMMER
_SEASON_MAP = {
    1: "WINTER", 2: "WINTER", 3: "WINTER",
    4: "SUMMER", 5: "SUMMER", 6: "SUMMER", 7: "SUMMER",
    8: "SUMMER", 9: "SUMMER", 10: "SUMMER",
    11: "WINTER", 12: "WINTER",
}


# ── Public entry point ───────────────────────────────────────────────


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list:
    """Build Seasonal Outages RTO — 4 yearly overlay charts."""
    logger.info("Building Seasonal Outages RTO report...")

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
        return [("Seasonal Outages RTO", _empty("No historical outage data available."), None)]

    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["day_of_year"] = df["date"].dt.dayofyear

    sections: list = []
    for type_label, col in _OUTAGE_TYPES:
        fig = _build_seasonal_chart(df, col, type_label)
        sections.append((type_label, fig, None))

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


def _build_seasonal_chart(df: pd.DataFrame, col: str, type_label: str) -> go.Figure:
    """Build a single seasonal overlay chart for one outage type."""
    years = sorted(df["year"].unique())

    fig = go.Figure()

    for i, year in enumerate(years):
        yr_data = df[df["year"] == year].sort_values("day_of_year")
        color = _YEAR_COLORS[i % len(_YEAR_COLORS)]
        fig.add_trace(go.Scatter(
            x=yr_data["day_of_year"],
            y=yr_data[col],
            mode="lines",
            name=str(year),
            line=dict(color=color, width=1.5),
        ))

    # Add month boundary lines and labels
    for m_idx, (m_start, m_name) in enumerate(zip(_MONTH_STARTS, _MONTHS)):
        season = _SEASON_MAP[m_idx + 1]

        # Vertical line at month start
        fig.add_vline(
            x=m_start, line_dash="dot",
            line_color="rgba(100, 130, 170, 0.3)", line_width=1,
        )

        # Month name + season label at top
        mid_x = (m_start + (_MONTH_STARTS[m_idx + 1] if m_idx < 11 else 366)) / 2
        fig.add_annotation(
            x=mid_x, y=1.06, yref="paper",
            text=f"<b>{m_name[:3]}</b><br><span style='font-size:9px;color:#7a94b5'>{season}</span>",
            showarrow=False,
            font=dict(size=10, color="#9eb4d3"),
            align="center",
        )

    fig.update_layout(
        title=f"{type_label} RTO — Seasonal Overlay",
        xaxis_title="Day of Year",
        yaxis_title="MW",
        height=500,
        template=PLOTLY_TEMPLATE,
        legend=dict(
            font=dict(size=11),
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        xaxis=dict(
            range=[1, 366],
            dtick=30,
        ),
        margin=dict(t=100),
    )

    return fig
