"""RT metered load data validation report — charts-first, no tables.

Sections:
  1. Hourly Load Profile  — 30d min/max band, last 3 days visible, others toggled off
  2. Completeness          — gaps, 24h check, duplicates
  3. Nulls & Outliers      — null counts, z-scores, physical bounds
  4. Correlation Heatmap   — load feature correlations
  5. Weekday vs Weekend    — avg hourly by day type
  6. Monthly Pattern       — RT load boxplot by month
  7. Seasonal Diurnal      — avg hourly shape by season
"""
import logging
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from src.like_day_forecast import configs
from src.data import load_rt_metered_hourly
from src.like_day_forecast.features import load_features
from src.utils.cache_utils import pull_with_cache

logger = logging.getLogger(__name__)

LOAD_LOW = 30_000
LOAD_HIGH = 200_000
PLOTLY_TEMPLATE = "plotly_dark"
PROFILE_LOOKBACK_DAYS = 30

Section = tuple[str, Any, str | None]


# ── Public entry point ───────────────────────────────────────────────


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list[Section]:
    """Pull RT metered load, validate, return 7 report sections."""
    logger.info("Building load validation report fragments...")

    logger.info("Pulling RT metered load...")
    df_rt_raw = pull_with_cache(
        source_name="pjm_load_rt_metered_hourly",
        pull_fn=load_rt_metered_hourly.pull,
        pull_kwargs={},
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )
    df_rt = df_rt_raw[df_rt_raw["region"] == configs.LOAD_REGION].copy()
    df_rt["date"] = pd.to_datetime(df_rt["date"])

    logger.info("Building load features...")
    df_features = load_features.build(df_rt_load=df_rt_raw)

    return [
        ("Hourly Load Profile", _hourly_profile_fig(df_rt), None),
        ("Completeness", _completeness_html(df_rt), None),
        ("Nulls & Outliers", _nulls_outliers_html(df_rt), None),
        ("Correlation Heatmap", _correlation_fig(df_features), None),
        ("Weekday vs Weekend", _diurnal_daytype_fig(df_rt), None),
        ("Monthly Pattern", _monthly_boxplot(df_rt), None),
        ("Seasonal Diurnal", _diurnal_seasonal_fig(df_rt), None),
    ]


# ── Section 1: Hourly Load Profile ──────────────────────────────────


def _hourly_profile_fig(df_rt: pd.DataFrame) -> go.Figure:
    """30-day min/max envelope + last 3 days visible + other days toggled off."""
    cutoff = df_rt["date"].max() - pd.Timedelta(days=PROFILE_LOOKBACK_DAYS)
    recent = df_rt[df_rt["date"] >= cutoff].copy()

    all_dates = sorted(recent["date"].unique())
    last_3 = all_dates[-3:]
    other_dates = all_dates[:-3]

    fig = go.Figure()

    # 30-day hourly min/max envelope
    hourly_stats = recent.groupby("hour_ending")["rt_load_mw"].agg(["min", "max"])

    fig.add_trace(go.Scatter(
        x=hourly_stats.index,
        y=hourly_stats["max"],
        mode="lines",
        name="30d Max",
        line=dict(color="rgba(99, 110, 250, 0.3)", width=0),
    ))
    fig.add_trace(go.Scatter(
        x=hourly_stats.index,
        y=hourly_stats["min"],
        mode="lines",
        name="30d Min/Max Range",
        line=dict(color="rgba(99, 110, 250, 0.3)", width=0),
        fill="tonexty",
        fillcolor="rgba(99, 110, 250, 0.15)",
    ))

    # Other days — toggled off by default
    for dt in other_dates:
        day = recent[recent["date"] == dt].sort_values("hour_ending")
        label = str(dt.date()) if hasattr(dt, "date") else str(dt)
        fig.add_trace(go.Scatter(
            x=day["hour_ending"],
            y=day["rt_load_mw"],
            mode="lines",
            name=label,
            line=dict(color="rgba(160, 174, 200, 0.4)", width=0.8),
            visible="legendonly",
        ))

    # Last 3 days — visible, distinct colors
    colors = ["#EF553B", "#00CC96", "#FFA15A"]
    for i, dt in enumerate(last_3):
        day = recent[recent["date"] == dt].sort_values("hour_ending")
        label = str(dt.date()) if hasattr(dt, "date") else str(dt)
        fig.add_trace(go.Scatter(
            x=day["hour_ending"],
            y=day["rt_load_mw"],
            mode="lines+markers",
            name=label,
            line=dict(color=colors[i % len(colors)], width=2),
            marker=dict(size=4),
        ))

    fig.update_layout(
        title=f"Hourly RT Load — Last {PROFILE_LOOKBACK_DAYS} Days",
        xaxis_title="Hour Ending",
        yaxis_title="RT Load (MW)",
        height=500,
        template=PLOTLY_TEMPLATE,
        legend=dict(font=dict(size=10)),
    )
    fig.update_xaxes(dtick=1)
    return fig


# ── Section 2: Completeness ─────────────────────────────────────────


def _completeness_html(df_rt: pd.DataFrame) -> str:
    today = pd.Timestamp(date.today())
    html = '<div style="padding:16px;font-family:monospace;font-size:12px;">'

    df_excl = df_rt[df_rt["date"] < today]
    hours_per_day = df_excl.groupby("date")["hour_ending"].nunique()
    incomplete = hours_per_day[hours_per_day != 24]

    html += _kv("Total dates", f"{len(hours_per_day):,}")
    html += _kv("Dates != 24 hours", str(len(incomplete)))
    if len(incomplete) > 0:
        for d, h in list(incomplete.tail(10).items()):
            html += _warn(f"{d}: {h} hours")

    # Date gaps
    all_dates = pd.to_datetime(sorted(df_rt["date"].unique()))
    expected = pd.date_range(all_dates.min(), all_dates.max(), freq="D")
    missing = expected.difference(all_dates)
    html += _kv("Expected dates", f"{len(expected):,}")
    html += _kv("Missing dates (gaps)", str(len(missing)))
    if len(missing) > 0:
        html += _warn(str([str(d.date()) for d in missing[-10:]]))

    # Duplicates
    dupes = df_rt.duplicated(subset=["date", "hour_ending"], keep=False).sum()
    html += _kv("Duplicate rows", str(dupes))

    html += '</div>'
    return html


# ── Section 3: Nulls & Outliers ─────────────────────────────────────


def _nulls_outliers_html(df_rt: pd.DataFrame) -> str:
    col = "rt_load_mw"
    html = '<div style="padding:16px;font-family:monospace;font-size:12px;">'

    html += _kv("Nulls", str(df_rt[col].isnull().sum()))

    desc = df_rt[col].describe()
    for stat in ["count", "mean", "std", "min", "25%", "50%", "75%", "max"]:
        html += _kv(stat, f"{desc[stat]:,.0f}")

    z = (df_rt[col] - df_rt[col].mean()) / df_rt[col].std()
    z_out = df_rt[z.abs() > 3]
    html += _kv("Z-score outliers (|z|>3)", str(len(z_out)))
    if len(z_out) > 0:
        html += _warn(f"Range: {z_out[col].min():,.0f} to {z_out[col].max():,.0f} MW")

    html += _kv(f"Below {LOAD_LOW:,} MW", str((df_rt[col] < LOAD_LOW).sum()))
    html += _kv(f"Above {LOAD_HIGH:,} MW", str((df_rt[col] > LOAD_HIGH).sum()))

    html += '</div>'
    return html


# ── Section 4: Correlation Heatmap ──────────────────────────────────


def _correlation_fig(df_features: pd.DataFrame) -> go.Figure:
    feature_cols = [c for c in df_features.columns if c != "date"]
    if not feature_cols:
        fig = go.Figure()
        fig.add_annotation(text="No features available", x=0.5, y=0.5, showarrow=False)
        fig.update_layout(height=300, template=PLOTLY_TEMPLATE)
        return fig

    corr = df_features[feature_cols].corr()
    fig = px.imshow(
        corr, text_auto=".2f", color_continuous_scale="RdBu_r",
        zmin=-1, zmax=1, title="Load Feature Correlation",
        template=PLOTLY_TEMPLATE,
    )
    fig.update_layout(height=550, width=700)
    return fig


# ── Section 5: Weekday vs Weekend ───────────────────────────────────


def _diurnal_daytype_fig(df_rt: pd.DataFrame) -> go.Figure:
    df_plot = df_rt.copy()
    df_plot["day_type"] = df_plot["date"].dt.dayofweek.map(
        lambda d: "Weekend" if d >= 5 else "Weekday"
    )
    profile = df_plot.groupby(["day_type", "hour_ending"])["rt_load_mw"].mean().reset_index()

    fig = px.line(
        profile, x="hour_ending", y="rt_load_mw", color="day_type",
        title="Average Hourly RT Load — Weekday vs Weekend",
        labels={"rt_load_mw": "Avg RT Load (MW)", "hour_ending": "Hour Ending", "day_type": "Day Type"},
        template=PLOTLY_TEMPLATE,
        color_discrete_map={"Weekday": "#636EFA", "Weekend": "#EF553B"},
    )
    fig.update_layout(height=450)
    fig.update_xaxes(dtick=1)
    return fig


# ── Section 6: Monthly Pattern ──────────────────────────────────────


def _monthly_boxplot(df_rt: pd.DataFrame) -> go.Figure:
    df_plot = df_rt.copy()
    df_plot["month_name"] = df_plot["date"].dt.strftime("%b")
    month_order = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    fig = px.box(
        df_plot, x="month_name", y="rt_load_mw",
        category_orders={"month_name": month_order},
        title="RT Metered Load by Month",
        labels={"rt_load_mw": "RT Load (MW)", "month_name": "Month"},
        template=PLOTLY_TEMPLATE,
    )
    fig.update_layout(height=450)
    return fig


# ── Section 7: Seasonal Diurnal ─────────────────────────────────────


def _diurnal_seasonal_fig(df_rt: pd.DataFrame) -> go.Figure:
    df_plot = df_rt.copy()
    month = df_plot["date"].dt.month
    df_plot["season"] = month.map(
        lambda m: "Winter" if m in (12, 1, 2)
        else "Spring" if m in (3, 4, 5)
        else "Summer" if m in (6, 7, 8)
        else "Fall"
    )
    profile = df_plot.groupby(["season", "hour_ending"])["rt_load_mw"].mean().reset_index()

    fig = px.line(
        profile, x="hour_ending", y="rt_load_mw", color="season",
        category_orders={"season": ["Winter", "Spring", "Summer", "Fall"]},
        title="Average Hourly RT Load by Season",
        labels={"rt_load_mw": "Avg RT Load (MW)", "hour_ending": "Hour Ending"},
        template=PLOTLY_TEMPLATE,
    )
    fig.update_layout(height=450)
    fig.update_xaxes(dtick=1)
    return fig


# ── Helpers ──────────────────────────────────────────────────────────


def _kv(key: str, value: str) -> str:
    return f'<div style="color:#a6bad6;margin:2px 0;">{key}: {value}</div>'


def _warn(text: str) -> str:
    return f'<div style="color:#e7b33c;margin-left:16px;">{text}</div>'
