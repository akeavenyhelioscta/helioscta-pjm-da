"""Load Forecast Changes RTO report for PJM + Meteologica vintage comparison.

For each source, compares 5 forecast vintages:
Latest, DA Cutoff, DA -12h, DA -24h, and DA -48h.
Each vintage uses rank-based as-of selection per forecast date/hour.

Each source section includes:
  1. Vintage info badges (exact execution timestamps with EPT labels)
  2. Overlay line chart (all vintages on common delivery intervals)
  3. Ramp Evolution chart (hour-over-hour MW change by vintage)
"""
import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

from src.like_day_forecast import configs
from src.like_day_forecast.data import (
    pjm_load_forecast_hourly,
    meteologica_load_forecast_hourly,
)
from src.like_day_forecast.utils.cache_utils import pull_with_cache

logger = logging.getLogger(__name__)

PLOTLY_TEMPLATE = "plotly_dark"

Section = tuple[str, Any, str | None]

# ── Vintage styling ──────────────────────────────────────────────────

VINTAGE_COLORS: dict[str, str] = {
    "Latest": "#60a5fa",
    "DA Cutoff": "#f87171",
    "DA -12h": "#a78bfa",
    "DA -24h": "#34d399",
    "DA -48h": "#fbbf24",
}
VINTAGE_DASH: dict[str, str] = {
    "Latest": "solid",
    "DA Cutoff": "solid",
    "DA -12h": "dash",
    "DA -24h": "dot",
    "DA -48h": "dashdot",
}
VINTAGE_WIDTH: dict[str, float] = {
    "Latest": 2.5,
    "DA Cutoff": 2.2,
    "DA -12h": 2.0,
    "DA -24h": 1.8,
    "DA -48h": 1.5,
}

# Canonical display order
_VINTAGE_ORDER = ["Latest", "DA Cutoff", "DA -12h", "DA -24h", "DA -48h"]

_SOURCE_MODULES = {
    "pjm": pjm_load_forecast_hourly,
    "meteologica": meteologica_load_forecast_hourly,
}


# ── Public entry point ───────────────────────────────────────────────


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list:
    """Build Load Forecast Changes RTO report fragments for PJM + Meteologica."""
    logger.info("Building Load Forecast Changes RTO report...")

    cache_kwargs = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    fragments: list = []

    for source_label, source_key in [("PJM", "pjm"), ("Meteologica", "meteologica")]:
        logger.info(f"Processing {source_label} vintages...")

        df = _safe_pull(
            f"forecast_evolution_{source_key}_latest_da_cutoff_v2",
            _pull_source_vintages,
            {"source": source_key, "region": "RTO"},
            **cache_kwargs,
        )

        if df is None or len(df) == 0:
            fragments.append((
                f"{source_label} — Load Forecast Changes RTO",
                _empty(f"No {source_label} forecast vintage data available."),
                None,
            ))
            continue

        # Keep only delivery intervals present in ALL vintages
        df_common = _filter_common_intervals(df)

        if len(df_common) == 0:
            fragments.append((
                f"{source_label} — Load Forecast Changes RTO",
                _empty(f"No common delivery intervals across {source_label} vintages."),
                None,
            ))
            continue

        html = _build_source_section(source_key, source_label, df_common)
        fragments.append((f"{source_label} — Load Forecast Changes RTO", html, None))

    return fragments


# ── Data helpers ─────────────────────────────────────────────────────


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


def _pull_source_vintages(source: str, region: str = "RTO") -> pd.DataFrame:
    """Pull Latest + 4 DA cutoff vintages for one source.

    - Latest: most recent forecast per (date, hour_ending) from today onward,
      via the source's ``pull()`` function.
    - DA Cutoff / DA -12h / DA -24h / DA -48h: from the standalone
      ``load_forecast_da_cutoff_vintages.sql`` via ``pull_da_cutoff_vintages()``.
    """
    if source not in _SOURCE_MODULES:
        raise ValueError(f"Unknown source: {source!r}. Must be one of {list(_SOURCE_MODULES)}")

    mod = _SOURCE_MODULES[source]
    _cols = ["forecast_date", "hour_ending", "forecast_load_mw",
             "forecast_execution_datetime", "vintage_label",
             "vintage_anchor_execution_datetime"]

    # 1. Latest vintage
    df_latest = mod.pull(region=region)
    if df_latest is not None and len(df_latest) > 0:
        df_latest["forecast_date"] = pd.to_datetime(df_latest["forecast_date"])
        df_latest["vintage_label"] = "Latest"
        df_latest["vintage_anchor_execution_datetime"] = pd.to_datetime(
            df_latest["forecast_execution_datetime"]
        ).max()
        df_latest = df_latest[_cols]
    else:
        df_latest = pd.DataFrame(columns=_cols)

    # 2. DA cutoff vintages
    df_da = mod.pull_da_cutoff_vintages(region=region)
    if df_da is not None and len(df_da) > 0:
        df_da["forecast_date"] = pd.to_datetime(df_da["forecast_date"])
        df_da = df_da[_cols]
    else:
        df_da = pd.DataFrame(columns=_cols)

    df = pd.concat([df_latest, df_da], ignore_index=True)
    if len(df) == 0:
        logger.warning(f"No vintage rows found for source={source}, region={region}")
        return pd.DataFrame()

    df["hour_ending"] = pd.to_numeric(df["hour_ending"], errors="coerce").astype("Int64")
    df["forecast_load_mw"] = pd.to_numeric(df["forecast_load_mw"], errors="coerce")
    df["forecast_execution_datetime"] = pd.to_datetime(df["forecast_execution_datetime"])
    df["vintage_anchor_execution_datetime"] = pd.to_datetime(df["vintage_anchor_execution_datetime"])

    df = df.dropna(subset=["forecast_date", "hour_ending", "forecast_load_mw", "vintage_label"]).copy()
    df["hour_ending"] = df["hour_ending"].astype(int)
    return df


def _filter_common_intervals(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only (date, hour_ending) pairs present in ALL vintages."""
    labels = df["vintage_label"].unique()
    if len(labels) <= 1:
        return df

    common = None
    for label in labels:
        sub = df[df["vintage_label"] == label]
        intervals = set(zip(sub["forecast_date"], sub["hour_ending"]))
        common = intervals if common is None else (common & intervals)

    if not common:
        return pd.DataFrame()

    common_df = pd.DataFrame(list(common), columns=["forecast_date", "hour_ending"])
    return df.merge(common_df, on=["forecast_date", "hour_ending"], how="inner")


def _empty(text: str) -> str:
    return f"<div style='padding:16px;color:#e74c3c;'>{text}</div>"


def _prep(df: pd.DataFrame) -> pd.DataFrame:
    """Sort and add datetime + display columns for charting."""
    df = df.sort_values(["forecast_date", "hour_ending"]).copy()
    df["datetime"] = pd.to_datetime(df["forecast_date"]) + pd.to_timedelta(df["hour_ending"], unit="h")
    df["_date_label"] = pd.to_datetime(df["forecast_date"]).dt.strftime("%a %b-%d")
    df["_he"] = df["hour_ending"].astype(int)
    return df


def _customdata(df: pd.DataFrame) -> np.ndarray:
    return np.column_stack([df["_date_label"], df["_he"]])


def _vintage_order(df: pd.DataFrame) -> list[str]:
    """Return canonical vintage labels present in the DataFrame."""
    present = set(df["vintage_label"].unique())
    return [v for v in _VINTAGE_ORDER if v in present]


# ── Source section builder ───────────────────────────────────────────


def _build_source_section(source_key: str, source_label: str, df: pd.DataFrame) -> str:
    """Build complete HTML for one source: badges + selected charts."""
    order = _vintage_order(df)

    badges = _build_vintage_badges(df, order)
    overlay = _build_overlay_chart(f"{source_key}_overlay", df, order, source_label)
    ramp_overlay = _build_ramp_overlay(f"{source_key}_ramp", df, order, source_label)
    return badges + overlay + ramp_overlay


# ── Vintage info badges ─────────────────────────────────────────────


def _build_vintage_badges(df: pd.DataFrame, order: list[str]) -> str:
    """Color-coded badges displaying the exact execution timestamp for each anchor."""
    badges = ""
    for label in order:
        sub = df[df["vintage_label"] == label]
        if len(sub) == 0:
            continue
        exec_ts = _display_exec_ts(sub)
        ts_str = exec_ts.strftime("%a %b %d, %H:%M") if pd.notna(exec_ts) else "N/A"
        color = VINTAGE_COLORS.get(label, "#94a3b8")

        badges += (
            f'<div style="display:inline-flex;align-items:center;gap:8px;'
            f'padding:6px 14px;margin:4px;'
            f'background:#111d31;border:1px solid #253b59;border-radius:8px;">'
            f'<span style="display:inline-block;width:12px;height:4px;'
            f'border-radius:2px;background:{color};"></span>'
            f'<span style="font-size:11px;font-weight:700;color:#9eb4d3;'
            f'text-transform:uppercase;letter-spacing:0.5px;">{label}</span>'
            f'<span style="font-size:12px;font-family:monospace;color:#dbe7ff;">'
            f'{ts_str} EPT</span>'
            f'</div>'
        )

    return (
        f'<div style="padding:12px;display:flex;flex-wrap:wrap;gap:0;">'
        f'{badges}'
        f'</div>'
    )


def _display_exec_ts(df: pd.DataFrame):
    """Execution timestamp used in badges/tables for one vintage."""
    if (
        "vintage_anchor_execution_datetime" in df.columns
        and df["vintage_anchor_execution_datetime"].notna().any()
    ):
        return pd.to_datetime(df["vintage_anchor_execution_datetime"]).max()

    if "forecast_execution_datetime" in df.columns and df["forecast_execution_datetime"].notna().any():
        return pd.to_datetime(df["forecast_execution_datetime"]).max()

    return pd.NaT


# ── Overlay chart ────────────────────────────────────────────────────


def _build_overlay_chart(
    chart_id: str, df: pd.DataFrame, order: list[str], source_label: str,
) -> str:
    """Plotly line chart overlaying all vintages."""
    fig = go.Figure()

    for label in order:
        sub = _prep(df[df["vintage_label"] == label].copy())
        if len(sub) == 0:
            continue

        fig.add_trace(go.Scatter(
            x=sub["datetime"],
            y=sub["forecast_load_mw"],
            mode="lines",
            name=label,
            line=dict(
                color=VINTAGE_COLORS.get(label, "#94a3b8"),
                width=VINTAGE_WIDTH.get(label, 1.5),
                dash=VINTAGE_DASH.get(label, "solid"),
            ),
            customdata=_customdata(sub),
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "HE %{customdata[1]}<br>"
                f"{label}: %{{y:,.0f}} MW"
                "<extra></extra>"
            ),
        ))

    fig.update_layout(
        title=f"{source_label} — Load Forecast Vintage Overlay",
        xaxis=dict(
            tickformat="%a %b-%d %I %p",
            gridcolor="rgba(99,110,250,0.08)",
        ),
        yaxis=dict(
            title="Load (MW)", tickformat=".1s",
            gridcolor="rgba(99,110,250,0.1)",
        ),
        height=500,
        template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="top", y=-0.08, xanchor="left", x=0),
        margin=dict(l=60, r=60, t=40, b=60),
        hovermode="x unified",
    )

    dates = sorted(df["forecast_date"].dt.date.unique())
    return _assemble_chart(chart_id, fig, dates)


# ── Delta chart ──────────────────────────────────────────────────────


def _build_delta_chart(
    chart_id: str, df: pd.DataFrame, order: list[str], source_label: str,
) -> str:
    """Line chart showing each vintage's MW difference from Latest."""
    if "Latest" not in order or len(order) < 2:
        return ""

    latest_raw = (
        df[df["vintage_label"] == "Latest"][["forecast_date", "hour_ending", "forecast_load_mw"]]
        .rename(columns={"forecast_load_mw": "latest_mw"})
    )

    fig = go.Figure()

    # Zero reference line
    fig.add_hline(y=0, line_dash="dash", line_color="#4a6a8f", line_width=1)

    for label in order:
        if label == "Latest":
            continue

        sub = df[df["vintage_label"] == label].copy()
        merged = sub.merge(latest_raw, on=["forecast_date", "hour_ending"])
        merged["delta"] = merged["forecast_load_mw"] - merged["latest_mw"]
        merged = _prep(merged)

        if len(merged) == 0:
            continue

        fig.add_trace(go.Scatter(
            x=merged["datetime"],
            y=merged["delta"],
            mode="lines",
            name=f"{label} vs Latest",
            line=dict(color=VINTAGE_COLORS.get(label, "#94a3b8"), width=2),
            customdata=_customdata(merged),
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "HE %{customdata[1]}<br>"
                f"{label} \u2212 Latest: %{{y:+,.0f}} MW"
                "<extra></extra>"
            ),
        ))

    fig.update_layout(
        title=f"{source_label} — Delta vs Latest (MW)",
        xaxis=dict(
            tickformat="%a %b-%d %I %p",
            gridcolor="rgba(99,110,250,0.08)",
        ),
        yaxis=dict(
            title="Delta (MW)", tickformat=".1s",
            gridcolor="rgba(99,110,250,0.1)",
            zeroline=True, zerolinecolor="#4a6a8f", zerolinewidth=1,
        ),
        height=400,
        template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="top", y=-0.10, xanchor="left", x=0),
        margin=dict(l=60, r=60, t=40, b=60),
        hovermode="x unified",
    )

    dates = sorted(df["forecast_date"].dt.date.unique())
    return _assemble_chart(chart_id, fig, dates)


# ── Plot 1: Ramp Evolution Overlay ───────────────────────────────────


def _compute_ramp(df: pd.DataFrame, label: str) -> pd.DataFrame:
    """Compute hour-over-hour ramp for one vintage, resetting per date.

    Robustness rules:
    - Deduplicate (date, hour_ending) by mean forecast value.
    - Restrict to HE1-HE24.
    - Reindex each date to HE1-HE24 so missing hours do not bridge across gaps.
    """
    sub = df[df["vintage_label"] == label].copy()
    if len(sub) == 0:
        return _prep(sub.assign(ramp_mw=pd.Series(dtype=float)))

    sub["forecast_date"] = pd.to_datetime(sub["forecast_date"])
    sub["hour_ending"] = pd.to_numeric(sub["hour_ending"], errors="coerce")
    sub["forecast_load_mw"] = pd.to_numeric(sub["forecast_load_mw"], errors="coerce")
    sub = sub.dropna(subset=["forecast_date", "hour_ending"])
    sub["hour_ending"] = sub["hour_ending"].astype(int)
    sub = sub[sub["hour_ending"].between(1, 24)]

    # Keep one value per delivery interval.
    sub = (
        sub.groupby(["forecast_date", "hour_ending"], as_index=False)["forecast_load_mw"]
        .mean()
    )

    days: list[pd.DataFrame] = []
    for dt, day in sub.groupby("forecast_date", sort=True):
        day = day.set_index("hour_ending").reindex(range(1, 25))
        day["forecast_date"] = dt
        day["hour_ending"] = day.index
        days.append(day.reset_index(drop=True))

    norm = pd.concat(days, ignore_index=True) if days else sub
    norm = _prep(norm)
    norm["ramp_mw"] = norm.groupby("forecast_date")["forecast_load_mw"].diff()
    return norm


def _build_ramp_overlay(
    chart_id: str, df: pd.DataFrame, order: list[str], source_label: str,
) -> str:
    """Line chart: each vintage's hour-over-hour ramp overlaid."""
    fig = go.Figure()
    fig.add_hline(y=0, line_dash="dash", line_color="#4a6a8f", line_width=1)

    for label in order:
        sub = _compute_ramp(df, label)
        if len(sub) == 0:
            continue

        fig.add_trace(go.Scatter(
            x=sub["datetime"], y=sub["ramp_mw"],
            mode="lines", name=label,
            line=dict(
                color=VINTAGE_COLORS.get(label, "#94a3b8"),
                width=VINTAGE_WIDTH.get(label, 1.5),
                dash=VINTAGE_DASH.get(label, "solid"),
            ),
            customdata=_customdata(sub),
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "HE %{customdata[1]}<br>"
                f"{label} Ramp: %{{y:+,.0f}} MW/hr"
                "<extra></extra>"
            ),
        ))

    fig.update_layout(
        title=f"{source_label} — Ramp Evolution (MW/hr)",
        xaxis=dict(tickformat="%a %b-%d %I %p", gridcolor="rgba(99,110,250,0.08)"),
        yaxis=dict(title="Ramp (MW/hr)", tickformat=".1s",
                    gridcolor="rgba(99,110,250,0.1)",
                    zeroline=True, zerolinecolor="#4a6a8f", zerolinewidth=1),
        height=450, template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="top", y=-0.08, xanchor="left", x=0),
        margin=dict(l=60, r=60, t=40, b=60),
        hovermode="x unified",
    )

    dates = sorted(df["forecast_date"].dt.date.unique())
    return _assemble_chart(chart_id, fig, dates)


# ── Plot 2: Ramp Delta vs Latest ────────────────────────────────────


def _build_ramp_delta(
    chart_id: str, df: pd.DataFrame, order: list[str], source_label: str,
) -> str:
    """Line chart: ramp_latest - ramp_vintage per hour."""
    if "Latest" not in order or len(order) < 2:
        return ""

    latest_ramp = _compute_ramp(df, "Latest")[["forecast_date", "hour_ending", "ramp_mw", "datetime", "_date_label", "_he"]].rename(
        columns={"ramp_mw": "latest_ramp"},
    )

    fig = go.Figure()
    fig.add_hline(y=0, line_dash="dash", line_color="#4a6a8f", line_width=1)

    for label in order:
        if label == "Latest":
            continue

        sub = _compute_ramp(df, label)
        if len(sub) == 0:
            continue

        merged = sub[["forecast_date", "hour_ending", "ramp_mw"]].merge(
            latest_ramp[["forecast_date", "hour_ending", "latest_ramp"]],
            on=["forecast_date", "hour_ending"],
        )
        merged["ramp_delta"] = merged["latest_ramp"] - merged["ramp_mw"]
        merged = _prep(merged)

        fig.add_trace(go.Scatter(
            x=merged["datetime"], y=merged["ramp_delta"],
            mode="lines", name=f"{label} vs Latest",
            line=dict(color=VINTAGE_COLORS.get(label, "#94a3b8"), width=2),
            customdata=_customdata(merged),
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "HE %{customdata[1]}<br>"
                f"Ramp \u0394 (Latest \u2212 {label}): %{{y:+,.0f}} MW/hr"
                "<extra></extra>"
            ),
        ))

    fig.update_layout(
        title=f"{source_label} — Ramp Delta vs Latest (MW/hr)",
        xaxis=dict(tickformat="%a %b-%d %I %p", gridcolor="rgba(99,110,250,0.08)"),
        yaxis=dict(title="Ramp Delta (MW/hr)", tickformat=".1s",
                    gridcolor="rgba(99,110,250,0.1)",
                    zeroline=True, zerolinecolor="#4a6a8f", zerolinewidth=1),
        height=400, template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="top", y=-0.10, xanchor="left", x=0),
        margin=dict(l=60, r=60, t=40, b=60),
        hovermode="x unified",
    )

    dates = sorted(df["forecast_date"].dt.date.unique())
    return _assemble_chart(chart_id, fig, dates)


# ── Plot 3: Peak Ramp Tracker (bar chart) ───────────────────────────


def _build_peak_ramp_bars(
    df: pd.DataFrame, order: list[str], source_label: str,
) -> str:
    """Grouped bar chart: max absolute ramp per vintage for morning + evening periods."""
    morning_he = list(range(5, 10))   # HE5-9
    evening_he = list(range(16, 21))  # HE16-20

    fig = go.Figure()

    for label in order:
        sub = _compute_ramp(df, label)
        if len(sub) == 0:
            continue

        morning = sub[sub["hour_ending"].isin(morning_he)]["ramp_mw"].dropna()
        evening = sub[sub["hour_ending"].isin(evening_he)]["ramp_mw"].dropna()

        # Use signed value of the max-abs ramp (preserves direction)
        morning_peak = morning.iloc[morning.abs().argmax()] if len(morning) > 0 else 0
        evening_peak = evening.iloc[evening.abs().argmax()] if len(evening) > 0 else 0

        fig.add_trace(go.Bar(
            x=["Morning (HE5-9)", "Evening (HE16-20)"],
            y=[morning_peak, evening_peak],
            name=label,
            marker_color=VINTAGE_COLORS.get(label, "#94a3b8"),
            opacity=0.85,
            hovertemplate=(
                f"{label}<br>"
                "%{x}<br>"
                "Peak Ramp: %{y:+,.0f} MW/hr"
                "<extra></extra>"
            ),
        ))

    fig.update_layout(
        title=f"{source_label} — Peak Hourly Ramp by Period",
        barmode="group",
        yaxis=dict(title="Peak Ramp (MW/hr)", tickformat=".1s",
                    gridcolor="rgba(99,110,250,0.1)",
                    zeroline=True, zerolinecolor="#4a6a8f", zerolinewidth=1),
        height=380, template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="left", x=0),
        margin=dict(l=60, r=40, t=40, b=60),
    )

    fig_json = pio.to_json(fig)
    return (
        f'<div style="padding:4px 12px;">'
        f'<div id="{source_label.lower()}_peak_ramp" style="width:100%;"></div>'
        f'</div>'
        f'<script>'
        f'(function(){{'
        f'var fig={fig_json};'
        f'Plotly.newPlot("{source_label.lower()}_peak_ramp",fig.data,fig.layout,{{responsive:true}});'
        f'}})();'
        f'</script>'
    )


# ── Plot 4: Cumulative Ramp (running sum from HE1) ─────────────────


def _build_cumulative_ramp(
    chart_id: str, df: pd.DataFrame, order: list[str], source_label: str,
) -> str:
    """Line chart: cumulative sum of hourly ramps from HE1, per vintage per date."""
    fig = go.Figure()

    for label in order:
        sub = _compute_ramp(df, label)
        if len(sub) == 0:
            continue

        # Cumulative ramp within each date (resets at HE1)
        sub["cum_ramp"] = sub.groupby("forecast_date")["ramp_mw"].cumsum()

        fig.add_trace(go.Scatter(
            x=sub["datetime"], y=sub["cum_ramp"],
            mode="lines", name=label,
            line=dict(
                color=VINTAGE_COLORS.get(label, "#94a3b8"),
                width=VINTAGE_WIDTH.get(label, 1.5),
                dash=VINTAGE_DASH.get(label, "solid"),
            ),
            customdata=_customdata(sub),
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "HE %{customdata[1]}<br>"
                f"{label} Cumul Ramp: %{{y:+,.0f}} MW"
                "<extra></extra>"
            ),
        ))

    fig.add_hline(y=0, line_dash="dash", line_color="#4a6a8f", line_width=1)

    fig.update_layout(
        title=f"{source_label} — Cumulative Ramp from HE1 (MW)",
        xaxis=dict(tickformat="%a %b-%d %I %p", gridcolor="rgba(99,110,250,0.08)"),
        yaxis=dict(title="Cumulative Ramp (MW)", tickformat=".1s",
                    gridcolor="rgba(99,110,250,0.1)",
                    zeroline=True, zerolinecolor="#4a6a8f", zerolinewidth=1),
        height=450, template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="top", y=-0.08, xanchor="left", x=0),
        margin=dict(l=60, r=60, t=40, b=60),
        hovermode="x unified",
    )

    dates = sorted(df["forecast_date"].dt.date.unique())
    return _assemble_chart(chart_id, fig, dates)


# ── Plot 5: Ramp Volatility Heatmap ────────────────────────────────


def _build_ramp_heatmap(
    df: pd.DataFrame, order: list[str], source_label: str,
) -> str:
    """Heatmap: HE on x-axis, vintage on y-axis, colored by avg absolute ramp."""
    rows = []
    for label in order:
        sub = _compute_ramp(df, label)
        if len(sub) == 0:
            continue
        hourly = sub.groupby("hour_ending")["ramp_mw"].agg(
            avg_abs_ramp=lambda x: x.abs().mean(),
        ).reset_index()
        for _, r in hourly.iterrows():
            rows.append({
                "vintage": label,
                "HE": int(r["hour_ending"]),
                "Avg |Ramp| (MW/hr)": round(r["avg_abs_ramp"], 0),
            })

    if not rows:
        return ""

    heat_df = pd.DataFrame(rows)
    pivot = heat_df.pivot(index="vintage", columns="HE", values="Avg |Ramp| (MW/hr)")
    # Reorder rows to match vintage order
    pivot = pivot.reindex([v for v in order if v in pivot.index])

    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=[f"HE{h}" for h in pivot.columns],
        y=pivot.index.tolist(),
        colorscale="YlOrRd",
        colorbar=dict(title="MW/hr", tickformat=".0f"),
        hovertemplate=(
            "%{y}<br>"
            "%{x}<br>"
            "Avg |Ramp|: %{z:,.0f} MW/hr"
            "<extra></extra>"
        ),
    ))

    fig.update_layout(
        title=f"{source_label} — Ramp Volatility by Hour & Vintage",
        xaxis=dict(title="", dtick=1),
        yaxis=dict(title="", autorange="reversed"),
        height=280, template=PLOTLY_TEMPLATE,
        margin=dict(l=80, r=40, t=40, b=40),
    )

    chart_id = f"{source_label.lower()}_ramp_heatmap"
    fig_json = pio.to_json(fig)
    return (
        f'<div style="padding:4px 12px;">'
        f'<div id="{chart_id}" style="width:100%;"></div>'
        f'</div>'
        f'<script>'
        f'(function(){{'
        f'var fig={fig_json};'
        f'Plotly.newPlot("{chart_id}",fig.data,fig.layout,{{responsive:true}});'
        f'}})();'
        f'</script>'
    )


# ── Summary table ────────────────────────────────────────────────────


def _build_summary_table(df: pd.DataFrame, order: list[str]) -> str:
    """HTML table: MAE, max delta, and delivery hour where max occurs."""
    if "Latest" not in order or len(order) < 2:
        return ""

    latest = df[df["vintage_label"] == "Latest"][["forecast_date", "hour_ending", "forecast_load_mw"]].rename(
        columns={"forecast_load_mw": "latest_mw"},
    )

    rows_html = ""
    for label in order:
        if label == "Latest":
            continue

        sub = df[df["vintage_label"] == label]
        if len(sub) == 0:
            continue

        color = VINTAGE_COLORS.get(label, "#94a3b8")
        exec_ts = _display_exec_ts(sub)
        exec_str = exec_ts.strftime("%a %b %d, %H:%M") + " EPT" if pd.notna(exec_ts) else "N/A"

        merged = sub.merge(latest, on=["forecast_date", "hour_ending"])
        merged["delta"] = merged["forecast_load_mw"] - merged["latest_mw"]
        merged["abs_delta"] = merged["delta"].abs()

        if len(merged) == 0 or merged["abs_delta"].isna().all():
            rows_html += _summary_row(label, color, exec_str, None, None, None, None)
            continue

        mae = merged["abs_delta"].mean()
        max_idx = merged["abs_delta"].idxmax()
        max_delta = merged.loc[max_idx, "delta"]
        max_he = int(merged.loc[max_idx, "hour_ending"])
        max_date = merged.loc[max_idx, "forecast_date"]
        max_date_str = pd.to_datetime(max_date).strftime("%b %d") if pd.notna(max_date) else ""

        rows_html += _summary_row(label, color, exec_str, mae, max_delta, max_he, max_date_str)

    th_style = (
        'padding:8px 12px;font-size:11px;text-transform:uppercase;'
        'color:#e6efff;letter-spacing:0.3px;'
    )

    return f"""
    <div style="padding:12px;">
        <div style="font-size:11px;font-weight:700;color:#6f8db1;text-transform:uppercase;
                    letter-spacing:0.5px;padding:0 0 8px 0;">
            Vintage Comparison Summary
        </div>
        <table style="width:100%;border-collapse:collapse;font-size:13px;">
            <thead>
                <tr style="background:#16263d;">
                    <th style="{th_style}text-align:left;">Vintage</th>
                    <th style="{th_style}text-align:left;">Execution Time</th>
                    <th style="{th_style}text-align:right;">MAE (MW)</th>
                    <th style="{th_style}text-align:right;">Max Delta (MW)</th>
                    <th style="{th_style}text-align:right;">Max Delta At</th>
                </tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
    </div>
    """


def _summary_row(
    label: str,
    color: str,
    exec_str: str,
    mae: float | None,
    max_delta: float | None,
    max_he: int | None,
    max_date_str: str | None,
) -> str:
    """Render one <tr> for the summary table."""
    td = 'padding:8px 12px;'
    mono = f'{td}font-family:monospace;'

    if mae is None:
        return (
            f'<tr style="border-top:1px solid #253b59;">'
            f'<td style="{td}">'
            f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;'
            f'background:{color};margin-right:8px;"></span>{label}</td>'
            f'<td style="{mono}color:#9eb4d3;">{exec_str}</td>'
            f'<td style="{mono}text-align:right;">\u2014</td>'
            f'<td style="{mono}text-align:right;">\u2014</td>'
            f'<td style="{mono}text-align:right;">\u2014</td>'
            f'</tr>'
        )

    delta_color = "#2ecc71" if max_delta >= 0 else "#e74c3c"

    return (
        f'<tr style="border-top:1px solid #253b59;">'
        f'<td style="{td}">'
        f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;'
        f'background:{color};margin-right:8px;"></span>{label}</td>'
        f'<td style="{mono}color:#9eb4d3;">{exec_str}</td>'
        f'<td style="{mono}text-align:right;color:#dbe7ff;">{mae:,.0f}</td>'
        f'<td style="{mono}text-align:right;color:{delta_color};">{max_delta:+,.0f}</td>'
        f'<td style="{mono}text-align:right;color:#9eb4d3;">{max_date_str} HE {max_he}</td>'
        f'</tr>'
    )


# ── Chart assembly (date-filter pills + Plotly) ─────────────────────


def _assemble_chart(chart_id: str, fig: go.Figure, dates: list) -> str:
    """Serialize Plotly figure + date-filter pill buttons into HTML/JS."""
    fig_json = pio.to_json(fig)

    btn_html = (
        f'<button class="fc-btn fc-btn-{chart_id} fc-active" data-date="all" '
        f"onclick=\"fcFilter('{chart_id}',this,'all')\">All</button>\n"
    )
    for dt in dates:
        label = dt.strftime("%a %b-%d")
        iso = dt.isoformat()
        btn_html += (
            f'<button class="fc-btn fc-btn-{chart_id}" data-date="{iso}" '
            f"onclick=\"fcFilter('{chart_id}',this,'{iso}')\">{label}</button>\n"
        )

    return (
        _CHART_TEMPLATE
        .replace("__CHART_ID__", chart_id)
        .replace("__DATE_BTNS__", btn_html)
        .replace("__FIG_JSON__", fig_json)
    )


# ── HTML/JS template ────────────────────────────────────────────────

_CHART_TEMPLATE = """
<div style="position:relative;">

  <div style="display:flex;align-items:center;gap:6px;padding:10px 12px;overflow-x:auto;flex-wrap:nowrap;">
    <span style="font-size:11px;font-weight:600;color:#6f8db1;white-space:nowrap;margin-right:4px;">
      FORECAST DATE
    </span>
    __DATE_BTNS__
  </div>

  <div id="__CHART_ID__" style="width:100%;"></div>
</div>

<style>
  .fc-btn {
    padding: 4px 12px; font-size: 11px; font-weight: 600;
    background: #101d31; color: #9eb4d3; border: 1px solid #2a3f60;
    border-radius: 16px; cursor: pointer; white-space: nowrap;
    font-family: inherit; transition: all 0.12s; flex-shrink: 0;
  }
  .fc-btn:hover { background: #1a2b44; color: #dbe7ff; }
  .fc-btn.fc-active { background: #20314d; color: #fff; border-color: #4cc9f0; }
</style>

<script>
(function() {
  var cid = '__CHART_ID__';
  var fig = __FIG_JSON__;
  Plotly.newPlot(cid, fig.data, fig.layout, {responsive: true});
  var sourceKey = String(cid).split('_')[0];

  if (!window._fcChartGroups) window._fcChartGroups = {};
  if (!window._fcChartGroups[sourceKey]) window._fcChartGroups[sourceKey] = [];
  if (window._fcChartGroups[sourceKey].indexOf(cid) === -1) {
    window._fcChartGroups[sourceKey].push(cid);
  }

  if (!window.fcFilter) {
    window.fcFilter = function(chartId, btn, dateStr) {
      function pad2(n) {
        return String(n).padStart(2, '0');
      }
      function toLocalTs(d) {
        return d.getFullYear() + '-' +
               pad2(d.getMonth() + 1) + '-' +
               pad2(d.getDate()) + 'T' +
               pad2(d.getHours()) + ':' +
               pad2(d.getMinutes()) + ':' +
               pad2(d.getSeconds());
      }
      function setActiveButton(targetChartId, targetDateStr) {
        document.querySelectorAll('.fc-btn-' + targetChartId).forEach(function(b) {
          b.classList.remove('fc-active');
        });
        var active = document.querySelector(
          '.fc-btn-' + targetChartId + '[data-date="' + targetDateStr + '"]'
        );
        if (active) active.classList.add('fc-active');
      }

      var groupKey = String(chartId).split('_')[0];
      var targets = (window._fcChartGroups && window._fcChartGroups[groupKey])
        ? window._fcChartGroups[groupKey].slice()
        : [chartId];
      if (targets.indexOf(chartId) === -1) targets.push(chartId);

      targets.forEach(function(targetChartId) {
        setActiveButton(targetChartId, dateStr);
        var chartEl = document.getElementById(targetChartId);
        if (!chartEl || !chartEl.data) return;

        if (dateStr === 'all') {
          Plotly.relayout(targetChartId, {'xaxis.autorange': true});
        } else {
          var d = new Date(dateStr + 'T00:00:00');
          var start = new Date(d.getTime() + 1 * 3600000);   // HE1
          var end   = new Date(d.getTime() + 24 * 3600000);  // HE24 (next-day 00:00)
          Plotly.relayout(targetChartId, {
            'xaxis.autorange': false,
            'xaxis.range': [toLocalTs(start), toLocalTs(end)]
          });
        }
      });
    };
  }
})();
</script>
"""

