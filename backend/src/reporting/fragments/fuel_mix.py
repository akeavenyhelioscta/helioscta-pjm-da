"""Fuel mix generation charts — per-fuel-type profile + ramp side-by-side.

One section per fuel type (dispatch stack order), each with hourly generation
profile (left) and hourly ramp (right) as side-by-side subplots.
"""
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.like_day_forecast import configs
from src.data import fuel_mix_hourly
from src.utils.cache_utils import pull_with_cache

logger = logging.getLogger(__name__)

PLOTLY_TEMPLATE = "plotly_dark"
PROFILE_LOOKBACK_DAYS = 30

# Fuel types in dispatch stack order
_FUEL_TYPES = [
    ("nuclear", "Nuclear Gen"),
    ("coal", "Coal Gen"),
    ("gas", "Gas Gen"),
    ("oil", "Oil Gen"),
    ("hydro", "Hydro Gen"),
    ("solar", "Solar Gen"),
    ("wind", "Wind Gen"),
]

Section = tuple[str, Any, str | None]

LAST_N_COLORS = ["#EF553B", "#636EFA", "#00CC96"]


# ── Public entry point ───────────────────────────────────────────────


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list[Section]:
    """Pull fuel mix data, return per-fuel side-by-side profile + ramp sections."""
    logger.info("Building fuel mix report fragments...")

    cache_kwargs = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    logger.info("Pulling fuel mix hourly...")
    df_raw = pull_with_cache(
        source_name="pjm_fuel_mix_hourly",
        pull_fn=fuel_mix_hourly.pull,
        pull_kwargs={},
        **cache_kwargs,
    )
    df = df_raw.copy()
    df["date"] = pd.to_datetime(df["date"])

    recent = _prep_recent(df)

    sections: list[Section] = []

    for col, label in _FUEL_TYPES:
        if col not in recent.columns:
            continue
        if recent[col].abs().sum() == 0:
            continue
        sections.append((label, _profile_and_ramp_fig(recent, col, label), None))

    return sections


# ── Shared data prep ─────────────────────────────────────────────────


def _prep_recent(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to last 30 days and remap hour 0 → HE 24."""
    cutoff = df["date"].max() - pd.Timedelta(days=PROFILE_LOOKBACK_DAYS)
    recent = df[df["date"] >= cutoff].copy()
    recent["hour_ending"] = recent["hour_ending"].replace(0, 24)
    recent = recent[recent["hour_ending"].between(1, 24)]
    return recent


# ── Side-by-side profile + ramp chart ────────────────────────────────


def _profile_and_ramp_fig(recent: pd.DataFrame, col: str, label: str) -> go.Figure:
    """Side-by-side: hourly gen profile (left) + hourly ramp (right)."""
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=(f"{label} — Hourly Profile", f"{label} — Hourly Ramp"),
        horizontal_spacing=0.08,
    )

    # Prep ramp data
    ramp_data = recent.sort_values(["date", "hour_ending"]).copy()
    ramp_col = f"{col}_ramp"
    ramp_data[ramp_col] = ramp_data.groupby("date")[col].diff()
    ramp_data = ramp_data.dropna(subset=[ramp_col])

    all_dates = sorted(recent["date"].unique())
    last_3 = all_dates[-3:]
    other_dates = all_dates[:-3]

    last_3_rev = list(reversed(last_3))
    other_dates_rev = list(reversed(other_dates))

    # ── Left: Profile ────────────────────────────────────────────────
    hourly_stats = recent.groupby("hour_ending")[col].agg(["min", "max"])

    fig.add_trace(go.Scatter(
        x=hourly_stats.index, y=hourly_stats["max"],
        mode="lines", name="30d Max",
        line=dict(color="rgba(99, 110, 250, 0.3)", width=0),
        showlegend=True, legendgroup="envelope",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=hourly_stats.index, y=hourly_stats["min"],
        mode="lines", name="30d Min/Max Range",
        line=dict(color="rgba(99, 110, 250, 0.3)", width=0),
        fill="tonexty", fillcolor="rgba(99, 110, 250, 0.15)",
        showlegend=True, legendgroup="envelope",
    ), row=1, col=1)

    for i, dt in enumerate(last_3_rev):
        day = recent[recent["date"] == dt].sort_values("hour_ending")
        day_label = str(dt.date()) if hasattr(dt, "date") else str(dt)
        fig.add_trace(go.Scatter(
            x=day["hour_ending"], y=day[col],
            mode="lines+markers", name=day_label,
            line=dict(color=LAST_N_COLORS[i % len(LAST_N_COLORS)], width=2),
            marker=dict(size=4),
            showlegend=True, legendgroup=day_label,
        ), row=1, col=1)

    for dt in other_dates_rev:
        day = recent[recent["date"] == dt].sort_values("hour_ending")
        day_label = str(dt.date()) if hasattr(dt, "date") else str(dt)
        fig.add_trace(go.Scatter(
            x=day["hour_ending"], y=day[col],
            mode="lines", name=day_label,
            line=dict(color="rgba(160, 174, 200, 0.4)", width=0.8),
            visible="legendonly",
            showlegend=True, legendgroup=day_label,
        ), row=1, col=1)

    # ── Right: Ramp ──────────────────────────────────────────────────
    ramp_stats = ramp_data.groupby("hour_ending")[ramp_col].agg(["min", "max"])

    fig.add_trace(go.Scatter(
        x=ramp_stats.index, y=ramp_stats["max"],
        mode="lines", name="30d Max",
        line=dict(color="rgba(99, 110, 250, 0.3)", width=0),
        showlegend=False, legendgroup="envelope",
    ), row=1, col=2)
    fig.add_trace(go.Scatter(
        x=ramp_stats.index, y=ramp_stats["min"],
        mode="lines", name="30d Min/Max Range",
        line=dict(color="rgba(99, 110, 250, 0.3)", width=0),
        fill="tonexty", fillcolor="rgba(99, 110, 250, 0.15)",
        showlegend=False, legendgroup="envelope",
    ), row=1, col=2)

    ramp_dates = sorted(ramp_data["date"].unique())
    ramp_last_3 = ramp_dates[-3:]
    ramp_other = ramp_dates[:-3]
    ramp_last_3_rev = list(reversed(ramp_last_3))
    ramp_other_rev = list(reversed(ramp_other))

    for i, dt in enumerate(ramp_last_3_rev):
        day = ramp_data[ramp_data["date"] == dt].sort_values("hour_ending")
        day_label = str(dt.date()) if hasattr(dt, "date") else str(dt)
        fig.add_trace(go.Bar(
            x=day["hour_ending"], y=day[ramp_col],
            name=day_label,
            marker_color=LAST_N_COLORS[i % len(LAST_N_COLORS)],
            opacity=0.8,
            showlegend=False, legendgroup=day_label,
        ), row=1, col=2)

    for dt in ramp_other_rev:
        day = ramp_data[ramp_data["date"] == dt].sort_values("hour_ending")
        day_label = str(dt.date()) if hasattr(dt, "date") else str(dt)
        fig.add_trace(go.Bar(
            x=day["hour_ending"], y=day[ramp_col],
            name=day_label,
            marker_color="rgba(160, 174, 200, 0.5)",
            visible="legendonly",
            showlegend=False, legendgroup=day_label,
        ), row=1, col=2)

    fig.add_hline(y=0, line_dash="dash", line_color="rgba(255, 255, 255, 0.5)",
                  line_width=1, row=1, col=2)

    fig.update_layout(
        title=f"{label} — Last {PROFILE_LOOKBACK_DAYS} Days",
        height=500,
        template=PLOTLY_TEMPLATE,
        legend=dict(font=dict(size=10)),
        barmode="group",
    )
    fig.update_xaxes(dtick=1, range=[0.5, 24.5], title_text="Hour Ending", col=1)
    fig.update_xaxes(dtick=1, range=[1.5, 24.5], title_text="Hour Ending", col=2)
    fig.update_yaxes(title_text="MW", col=1)
    fig.update_yaxes(title_text="MW/hr", col=2)
    return fig
