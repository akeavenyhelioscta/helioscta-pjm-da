"""Tie flow charts — one plot per region showing scheduled & actual MW.

Sections (one per tie flow region):
  - Scheduled & Actual hourly profiles with 30d min/max envelope + last 3 days
"""
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go

from src.like_day_forecast import configs
from src.data import tie_flows_hourly
from src.utils.cache_utils import pull_with_cache

logger = logging.getLogger(__name__)

PLOTLY_TEMPLATE = "plotly_dark"
PROFILE_LOOKBACK_DAYS = 30

Section = tuple[str, Any, str | None]

# Colors for last 3 days (most recent first)
DAY_COLORS = ["#EF553B", "#636EFA", "#00CC96"]

# Colors for scheduled vs actual traces
SCHEDULED_COLOR = "#FF6692"
ACTUAL_COLOR = "#19D3F3"


# ── Public entry point ───────────────────────────────────────────────


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list[Section]:
    """Pull tie flow data and return one chart section per region."""
    logger.info("Building tie flow report fragments...")

    cache_kwargs = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    logger.info("Pulling tie flows hourly...")
    df_raw = pull_with_cache(
        source_name="pjm_tie_flows_hourly",
        pull_fn=tie_flows_hourly.pull,
        pull_kwargs={},
        **cache_kwargs,
    )
    df = df_raw.copy()
    df["date"] = pd.to_datetime(df["date"])

    # Remap hour 0 → HE 24 and filter to last 30 days
    df["hour_ending"] = df["hour_ending"].replace(0, 24)
    df = df[df["hour_ending"].between(1, 24)]
    cutoff = df["date"].max() - pd.Timedelta(days=PROFILE_LOOKBACK_DAYS)
    df = df[df["date"] >= cutoff]

    regions = sorted(df["tie_flow_name"].unique())
    logger.info(f"Found {len(regions)} tie flow regions")

    sections: list[Section] = []
    for region in regions:
        df_region = df[df["tie_flow_name"] == region].copy()
        fig = _region_fig(df_region, region)
        sections.append((region, fig, None))

    return sections


# ── Per-region chart builder ─────────────────────────────────────────


def _region_fig(df: pd.DataFrame, region: str) -> go.Figure:
    """Build a chart for one tie flow region.

    Shows:
      - 30-day min/max envelope for both scheduled and actual
      - Last 3 days of scheduled and actual as individual traces
      - Older days toggled off in legend
    """
    fig = go.Figure()

    all_dates = sorted(df["date"].unique())
    if len(all_dates) == 0:
        fig.update_layout(
            title=f"{region} — No Data",
            template=PLOTLY_TEMPLATE,
            height=450,
        )
        return fig

    last_3 = all_dates[-3:]
    other_dates = all_dates[:-3]
    last_3_rev = list(reversed(last_3))
    other_dates_rev = list(reversed(other_dates))

    # ── 30-day envelopes ─────────────────────────────────────────
    hourly = df.groupby("hour_ending")

    for col, label, color_rgb in [
        ("scheduled_mw", "Scheduled", "255, 102, 146"),
        ("actual_mw", "Actual", "25, 211, 243"),
    ]:
        stats = hourly[col].agg(["min", "max"])

        fig.add_trace(go.Scatter(
            x=stats.index, y=stats["max"],
            mode="lines",
            name=f"30d {label} Range",
            line=dict(color=f"rgba({color_rgb}, 0.3)", width=0),
            showlegend=False,
            legendgroup=f"env_{col}",
            hoverinfo="skip",
        ))
        fig.add_trace(go.Scatter(
            x=stats.index, y=stats["min"],
            mode="lines",
            name=f"30d {label} Range",
            line=dict(color=f"rgba({color_rgb}, 0.3)", width=0),
            fill="tonexty",
            fillcolor=f"rgba({color_rgb}, 0.10)",
            showlegend=True,
            legendgroup=f"env_{col}",
            hoverinfo="skip",
        ))

    # ── Last 3 days — visible ────────────────────────────────────
    for i, dt in enumerate(last_3_rev):
        day = df[df["date"] == dt].sort_values("hour_ending")
        day_label = str(dt.date()) if hasattr(dt, "date") else str(dt)
        color = DAY_COLORS[i % len(DAY_COLORS)]

        # Scheduled — dashed
        fig.add_trace(go.Scatter(
            x=day["hour_ending"], y=day["scheduled_mw"],
            mode="lines",
            name=f"{day_label} Sched",
            line=dict(color=color, width=2, dash="dash"),
            legendgroup=day_label,
            hovertemplate=(
                f"<b>{day_label}</b> HE %{{x}}<br>"
                "Scheduled: %{y:,.0f} MW<extra></extra>"
            ),
        ))

        # Actual — solid with markers
        fig.add_trace(go.Scatter(
            x=day["hour_ending"], y=day["actual_mw"],
            mode="lines+markers",
            name=f"{day_label} Actual",
            line=dict(color=color, width=2),
            marker=dict(size=4),
            legendgroup=day_label,
            hovertemplate=(
                f"<b>{day_label}</b> HE %{{x}}<br>"
                "Actual: %{y:,.0f} MW<extra></extra>"
            ),
        ))

    # ── Older days — toggled off ─────────────────────────────────
    for dt in other_dates_rev:
        day = df[df["date"] == dt].sort_values("hour_ending")
        day_label = str(dt.date()) if hasattr(dt, "date") else str(dt)

        fig.add_trace(go.Scatter(
            x=day["hour_ending"], y=day["scheduled_mw"],
            mode="lines",
            name=f"{day_label} Sched",
            line=dict(color="rgba(160, 174, 200, 0.3)", width=0.8, dash="dash"),
            visible="legendonly",
            legendgroup=day_label,
        ))

        fig.add_trace(go.Scatter(
            x=day["hour_ending"], y=day["actual_mw"],
            mode="lines",
            name=f"{day_label} Actual",
            line=dict(color="rgba(160, 174, 200, 0.4)", width=0.8),
            visible="legendonly",
            legendgroup=day_label,
        ))

    # ── Zero line ────────────────────────────────────────────────
    fig.add_hline(
        y=0, line_dash="dash",
        line_color="rgba(255, 255, 255, 0.3)", line_width=1,
    )

    fig.update_layout(
        title=f"{region} — Scheduled & Actual Tie Flows (Last {PROFILE_LOOKBACK_DAYS} Days)",
        height=450,
        template=PLOTLY_TEMPLATE,
        legend=dict(font=dict(size=10)),
        xaxis=dict(dtick=1, range=[0.5, 24.5], title_text="Hour Ending"),
        yaxis=dict(title_text="MW"),
    )
    return fig
