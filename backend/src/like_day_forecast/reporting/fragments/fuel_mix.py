"""Fuel mix generation charts — side-by-side gas & coal.

Sections:
  1. Hourly Generation Profiles — gas (left) & coal (right), 30d envelope + last 3 days
  2. Hourly Generation Ramps    — gas (left) & coal (right), grouped bars + 30d envelope
  3. Gen vs RT LMP Scatter       — gas (left) & coal (right) vs RT Western Hub, by peak type
"""
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.like_day_forecast import configs
from src.like_day_forecast.data import fuel_mix_hourly, lmps_hourly
from src.like_day_forecast.utils.cache_utils import pull_with_cache

logger = logging.getLogger(__name__)

PLOTLY_TEMPLATE = "plotly_dark"
PROFILE_LOOKBACK_DAYS = 30
ONPEAK_HOURS = list(range(8, 24))       # HE 8–23
OFFPEAK_HOURS = list(range(1, 8)) + [24]  # HE 1–7, 24

Section = tuple[str, Any, str | None]


# ── Public entry point ───────────────────────────────────────────────


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list[Section]:
    """Pull fuel mix + RT LMP data, return 3 side-by-side chart sections."""
    logger.info("Building fuel mix report fragments...")

    cache_kwargs = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    logger.info("Pulling fuel mix hourly...")
    df_raw = pull_with_cache(
        source_name="fuel_mix_hourly",
        pull_fn=fuel_mix_hourly.pull,
        pull_kwargs={},
        **cache_kwargs,
    )
    df = df_raw.copy()
    df["date"] = pd.to_datetime(df["date"])

    logger.info("Pulling RT LMP hourly...")
    df_lmp_rt = pull_with_cache(
        source_name="lmps_hourly_rt",
        pull_fn=lmps_hourly.pull,
        pull_kwargs={"schema": schema, "hub": configs.HUB, "market": "rt"},
        **cache_kwargs,
    )

    return [
        ("Hourly Generation Profiles", _profiles_fig(df), None),
        ("Hourly Generation Ramps", _ramps_fig(df), None),
        ("Gen vs RT Western Hub", _gen_vs_lmp_scatter(df, df_lmp_rt), None),
    ]


# ── Shared data prep ─────────────────────────────────────────────────


def _prep_recent(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to last 30 days and remap hour 0 → HE 24."""
    cutoff = df["date"].max() - pd.Timedelta(days=PROFILE_LOOKBACK_DAYS)
    recent = df[df["date"] >= cutoff].copy()
    recent["hour_ending"] = recent["hour_ending"].replace(0, 24)
    recent = recent[recent["hour_ending"].between(1, 24)]
    return recent


# ── Section 1: Hourly Generation Profiles ────────────────────────────


def _profiles_fig(df: pd.DataFrame) -> go.Figure:
    """Side-by-side gas & coal hourly profiles with 30d envelope + last 3 days."""
    recent = _prep_recent(df)

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("Gas Gen", "Coal Gen"),
        horizontal_spacing=0.08,
    )

    all_dates = sorted(recent["date"].unique())
    last_3 = all_dates[-3:]
    other_dates = all_dates[:-3]

    # Colors for last 3 days — most recent first
    last_3_rev = list(reversed(last_3))
    other_dates_rev = list(reversed(other_dates))
    colors = ["#EF553B", "#636EFA", "#00CC96"]

    for col_idx, (col, label) in enumerate([("gas", "Gas Gen"), ("coal", "Coal Gen")], start=1):
        hourly_stats = recent.groupby("hour_ending")[col].agg(["min", "max"])

        # 30-day envelope — first in legend
        fig.add_trace(go.Scatter(
            x=hourly_stats.index, y=hourly_stats["max"],
            mode="lines", name="30d Max",
            line=dict(color="rgba(99, 110, 250, 0.3)", width=0),
            showlegend=(col_idx == 1),
            legendgroup="envelope",
        ), row=1, col=col_idx)

        fig.add_trace(go.Scatter(
            x=hourly_stats.index, y=hourly_stats["min"],
            mode="lines", name="30d Min/Max Range",
            line=dict(color="rgba(99, 110, 250, 0.3)", width=0),
            fill="tonexty", fillcolor="rgba(99, 110, 250, 0.15)",
            showlegend=(col_idx == 1),
            legendgroup="envelope",
        ), row=1, col=col_idx)

        # Last 3 days — visible, most recent first in legend
        for i, dt in enumerate(last_3_rev):
            day = recent[recent["date"] == dt].sort_values("hour_ending")
            day_label = str(dt.date()) if hasattr(dt, "date") else str(dt)
            fig.add_trace(go.Scatter(
                x=day["hour_ending"], y=day[col],
                mode="lines+markers", name=day_label,
                line=dict(color=colors[i % len(colors)], width=2),
                marker=dict(size=4),
                showlegend=(col_idx == 1),
                legendgroup=day_label,
            ), row=1, col=col_idx)

        # Other days — toggled off, most recent first in legend
        for dt in other_dates_rev:
            day = recent[recent["date"] == dt].sort_values("hour_ending")
            day_label = str(dt.date()) if hasattr(dt, "date") else str(dt)
            fig.add_trace(go.Scatter(
                x=day["hour_ending"], y=day[col],
                mode="lines", name=day_label,
                line=dict(color="rgba(160, 174, 200, 0.4)", width=0.8),
                visible="legendonly",
                showlegend=(col_idx == 1),
                legendgroup=day_label,
            ), row=1, col=col_idx)

    fig.update_layout(
        title=f"Hourly Generation — Last {PROFILE_LOOKBACK_DAYS} Days",
        height=500,
        template=PLOTLY_TEMPLATE,
        legend=dict(font=dict(size=10)),
    )
    fig.update_xaxes(dtick=1, range=[0.5, 24.5], title_text="Hour Ending")
    fig.update_yaxes(title_text="MW", col=1)
    fig.update_yaxes(title_text="MW", col=2)
    return fig


# ── Section 2: Hourly Generation Ramps ───────────────────────────────


def _ramps_fig(df: pd.DataFrame) -> go.Figure:
    """Side-by-side gas & coal ramp bars with 30d envelope + last 3 days."""
    recent = _prep_recent(df)
    recent = recent.sort_values(["date", "hour_ending"])

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("Gas Gen Ramp", "Coal Gen Ramp"),
        horizontal_spacing=0.08,
    )

    for col_idx, (col, label) in enumerate([("gas", "Gas Gen"), ("coal", "Coal Gen")], start=1):
        # Compute ramp per day
        recent[f"{col}_ramp"] = recent.groupby("date")[col].diff()

    ramp_data = recent.dropna(subset=["gas_ramp", "coal_ramp"])
    all_dates = sorted(ramp_data["date"].unique())
    last_3 = all_dates[-3:]
    other_dates = all_dates[:-3]

    last_3_rev = list(reversed(last_3))
    other_dates_rev = list(reversed(other_dates))
    colors = ["#EF553B", "#636EFA", "#00CC96"]

    for col_idx, (col, ramp_col, label) in enumerate([
        ("gas", "gas_ramp", "Gas Gen"),
        ("coal", "coal_ramp", "Coal Gen"),
    ], start=1):
        hourly_stats = ramp_data.groupby("hour_ending")[ramp_col].agg(["min", "max"])

        # 30-day envelope — first in legend
        fig.add_trace(go.Scatter(
            x=hourly_stats.index, y=hourly_stats["max"],
            mode="lines", name="30d Max",
            line=dict(color="rgba(99, 110, 250, 0.3)", width=0),
            showlegend=(col_idx == 1),
            legendgroup="envelope",
        ), row=1, col=col_idx)

        fig.add_trace(go.Scatter(
            x=hourly_stats.index, y=hourly_stats["min"],
            mode="lines", name="30d Min/Max Range",
            line=dict(color="rgba(99, 110, 250, 0.3)", width=0),
            fill="tonexty", fillcolor="rgba(99, 110, 250, 0.15)",
            showlegend=(col_idx == 1),
            legendgroup="envelope",
        ), row=1, col=col_idx)

        # Last 3 days — visible grouped bars, most recent first in legend
        for i, dt in enumerate(last_3_rev):
            day = ramp_data[ramp_data["date"] == dt].sort_values("hour_ending")
            day_label = str(dt.date()) if hasattr(dt, "date") else str(dt)
            fig.add_trace(go.Bar(
                x=day["hour_ending"], y=day[ramp_col],
                name=day_label,
                marker_color=colors[i % len(colors)],
                opacity=0.8,
                showlegend=(col_idx == 1),
                legendgroup=day_label,
            ), row=1, col=col_idx)

        # Other days — toggled off, most recent first in legend
        for dt in other_dates_rev:
            day = ramp_data[ramp_data["date"] == dt].sort_values("hour_ending")
            day_label = str(dt.date()) if hasattr(dt, "date") else str(dt)
            fig.add_trace(go.Bar(
                x=day["hour_ending"], y=day[ramp_col],
                name=day_label,
                marker_color="rgba(160, 174, 200, 0.5)",
                visible="legendonly",
                showlegend=(col_idx == 1),
                legendgroup=day_label,
            ), row=1, col=col_idx)

        # Zero line
        fig.add_hline(y=0, line_dash="dash", line_color="rgba(255, 255, 255, 0.5)",
                      line_width=1, row=1, col=col_idx)

    fig.update_layout(
        title=f"Hourly Generation Ramp — Last {PROFILE_LOOKBACK_DAYS} Days",
        height=500,
        template=PLOTLY_TEMPLATE,
        legend=dict(font=dict(size=10)),
        barmode="group",
    )
    fig.update_xaxes(dtick=1, range=[1.5, 24.5], title_text="Hour Ending")
    fig.update_yaxes(title_text="MW/hr", col=1)
    fig.update_yaxes(title_text="MW/hr", col=2)
    return fig


# ── Section 3: Gen vs RT LMP Scatter ─────────────────────────────────


_PEAK_MARKERS = {
    "OnPeak":  "circle",
    "OffPeak": "diamond",
    "Weekend": "square",
}


def _gen_vs_lmp_scatter(df_fuel: pd.DataFrame, df_lmp: pd.DataFrame) -> go.Figure:
    """Side-by-side scatter: gas vs RT LMP (left) and coal vs RT LMP (right), one trace per date."""
    recent_fuel = _prep_recent(df_fuel)

    # Prep LMP — align to same date range & hour convention
    df_lmp = df_lmp.copy()
    df_lmp["date"] = pd.to_datetime(df_lmp["date"])
    cutoff = recent_fuel["date"].min()
    recent_lmp = df_lmp[df_lmp["date"] >= cutoff].copy()

    # Merge on date + hour_ending
    merged = recent_fuel.merge(
        recent_lmp[["date", "hour_ending", "lmp_total"]],
        on=["date", "hour_ending"],
        how="inner",
    )

    # Classify peak type: weekend takes priority, then on/off peak
    merged["dow"] = merged["date"].dt.dayofweek
    merged["peak_type"] = "OffPeak"
    merged.loc[merged["hour_ending"].isin(ONPEAK_HOURS), "peak_type"] = "OnPeak"
    merged.loc[merged["dow"] >= 5, "peak_type"] = "Weekend"

    # Map peak type to marker symbol per row
    merged["symbol"] = merged["peak_type"].map(_PEAK_MARKERS)

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("Gas Gen vs RT Western Hub", "Coal Gen vs RT Western Hub"),
        horizontal_spacing=0.08,
    )

    hover_tpl = (
        "<b>%{customdata[0]}</b> HE %{customdata[1]}<br>"
        "Gen: %{x:,.0f} MW<br>"
        "RT LMP: $%{y:,.2f}<br>"
        "Type: %{customdata[2]}"
        "<extra></extra>"
    )

    all_dates = sorted(merged["date"].unique())
    last_3 = all_dates[-3:]
    other_dates = all_dates[:-3]

    last_3_rev = list(reversed(last_3))
    other_dates_rev = list(reversed(other_dates))
    colors = ["#EF553B", "#636EFA", "#00CC96"]
    gen_cols = [("gas", "Gas Gen"), ("coal", "Coal Gen")]

    for col_idx, (gen_col, label) in enumerate(gen_cols, start=1):
        # 30-day min/max range box — first in legend
        gen_min, gen_max = merged[gen_col].min(), merged[gen_col].max()
        lmp_min, lmp_max = merged["lmp_total"].min(), merged["lmp_total"].max()

        # Draw a rectangle outline via scatter: bottom edge, right edge, top edge, left edge, close
        box_x = [gen_min, gen_max, gen_max, gen_min, gen_min]
        box_y = [lmp_min, lmp_min, lmp_max, lmp_max, lmp_min]

        fig.add_trace(go.Scatter(
            x=box_x, y=box_y,
            mode="lines",
            name="30d Min/Max Range",
            line=dict(color="rgba(99, 110, 250, 0.4)", width=1, dash="dot"),
            fill="toself",
            fillcolor="rgba(99, 110, 250, 0.08)",
            showlegend=(col_idx == 1),
            legendgroup="envelope",
            hoverinfo="skip",
        ), row=1, col=col_idx)

        # Last 3 days — visible, most recent first
        for i, dt in enumerate(last_3_rev):
            subset = merged[merged["date"] == dt]
            day_label = str(dt.date()) if hasattr(dt, "date") else str(dt)

            customdata = list(zip(
                subset["date"].dt.strftime("%Y-%m-%d"),
                subset["hour_ending"],
                subset["peak_type"],
            ))

            fig.add_trace(go.Scatter(
                x=subset[gen_col],
                y=subset["lmp_total"],
                mode="markers",
                name=day_label,
                marker=dict(
                    color=colors[i % len(colors)],
                    symbol=subset["symbol"],
                    size=7,
                    opacity=0.8,
                ),
                customdata=customdata,
                hovertemplate=hover_tpl,
                showlegend=(col_idx == 1),
                legendgroup=day_label,
            ), row=1, col=col_idx)

        # Other days — toggled off, most recent first
        for dt in other_dates_rev:
            subset = merged[merged["date"] == dt]
            day_label = str(dt.date()) if hasattr(dt, "date") else str(dt)

            customdata = list(zip(
                subset["date"].dt.strftime("%Y-%m-%d"),
                subset["hour_ending"],
                subset["peak_type"],
            ))

            fig.add_trace(go.Scatter(
                x=subset[gen_col],
                y=subset["lmp_total"],
                mode="markers",
                name=day_label,
                marker=dict(
                    color="rgba(160, 174, 200, 0.5)",
                    symbol=subset["symbol"],
                    size=6,
                ),
                customdata=customdata,
                hovertemplate=hover_tpl,
                visible="legendonly",
                showlegend=(col_idx == 1),
                legendgroup=day_label,
            ), row=1, col=col_idx)

    fig.update_layout(
        title=f"Generation vs RT Western Hub — Last {PROFILE_LOOKBACK_DAYS} Days",
        height=550,
        template=PLOTLY_TEMPLATE,
        legend=dict(font=dict(size=10)),
    )
    fig.update_xaxes(title_text="Gas Gen (MW)", col=1)
    fig.update_xaxes(title_text="Coal Gen (MW)", col=2)
    fig.update_yaxes(title_text="RT LMP ($/MWh)", col=1)
    fig.update_yaxes(title_text="RT LMP ($/MWh)", col=2)
    return fig
