"""Combined load forecast vintage dashboard — RTO + WEST + MIDATL + SOUTH.

For each region shows two charts:
  1. PJM vintage overlay (5 vintages)
  2. Meteologica vintage overlay (5 vintages + Euro ensemble band)

Vintages: Latest, DA Cutoff, DA -12h, DA -24h, DA -48h.

Global controls at the top:
  1. Date pills — filter x-axis across all charts
  2. Vintage pills — show/hide vintage traces across all charts

PJM and Meteologica charts have an independent RAMP toggle showing
hour-over-hour MW change for all vintages on a secondary y-axis.

Regions: RTO, WEST, MIDATL, SOUTH
"""
import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

from src.like_day_forecast import configs
from src.data import load_forecast_vintages
from src.utils.cache_utils import pull_with_cache

logger = logging.getLogger(__name__)

PLOTLY_TEMPLATE = "plotly_dark"

REGIONS = ["RTO", "WEST", "MIDATL", "SOUTH"]
_REGION_LABELS = {
    "RTO": "RTO",
    "WEST": "Western",
    "MIDATL": "Mid-Atlantic",
    "SOUTH": "Southern",
}
_PREFIX = "vintComb"

# ── Vintage styling ─────────────────────────────────────────────────

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
_VINTAGE_ORDER = ["Latest", "DA Cutoff", "DA -12h", "DA -24h", "DA -48h"]

Section = tuple[str, Any, str | None]


# ── Public entry point ──────────────────────────────────────────────


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list:
    """Pull vintage data for all regions × sources, return combined fragments."""
    logger.info("Building combined vintage load forecast report...")

    cache_kwargs = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    # ── Pull data (one cache file per source, all regions) ──────
    pjm_all = _safe_pull(
        "pjm_load_forecast_vintages",
        load_forecast_vintages.pull_combined_vintages,
        {"source": "pjm"},
        **cache_kwargs,
    )
    meteo_all = _safe_pull(
        "meteologica_load_forecast_vintages",
        load_forecast_vintages.pull_combined_vintages,
        {"source": "meteologica"},
        **cache_kwargs,
    )
    # Split into per-region dicts for chart building
    vintage_data: dict[str, dict[str, pd.DataFrame | None]] = {}
    euro_data: dict[str, pd.DataFrame | None] = {}
    for region in REGIONS:
        vintage_data[region] = {}
        for source_key, df_all in [("pjm", pjm_all), ("meteologica", meteo_all)]:
            if df_all is not None and len(df_all) > 0 and "region" in df_all.columns:
                sub = df_all[df_all["region"] == region]
                vintage_data[region][source_key] = sub if len(sub) > 0 else None
            else:
                vintage_data[region][source_key] = None
        euro_data[region] = None

    # ── Collect all dates for the global filter ──────────────────
    all_dates: set = set()
    for region in REGIONS:
        for df in vintage_data[region].values():
            if df is not None and len(df) > 0:
                all_dates |= set(pd.to_datetime(df["forecast_date"]).dt.date.unique())
    all_dates_sorted = sorted(all_dates)

    # ── Build chart ID list ──────────────────────────────────────
    chart_ids: list[str] = []
    for region in REGIONS:
        chart_ids.append(f"{_PREFIX}Pjm{region}")
        chart_ids.append(f"{_PREFIX}Meteo{region}")

    fragments: list = []

    # 1. Global control bar (date pills + vintage toggle pills)
    filter_html = _build_global_controls(
        f"{_PREFIX}Filter", chart_ids, all_dates_sorted,
    )
    fragments.append(("", filter_html, None))

    # 2. Vintage badges (using RTO PJM data for execution timestamps)
    badges_df = vintage_data["RTO"].get("pjm")
    if badges_df is not None and len(badges_df) > 0:
        order = _vintage_order(badges_df)
        badges_html = _build_vintage_badges(badges_df, order)
        fragments.append(("Vintage Info", badges_html, None))

    # 3-10. Region sections
    for region in REGIONS:
        label = _REGION_LABELS[region]
        fragments.append(f"Load Forecast Vintages — {label}")

        # PJM chart
        df_pjm = vintage_data[region]["pjm"]
        pjm_chart_id = f"{_PREFIX}Pjm{region}"
        if df_pjm is not None and len(df_pjm) > 0:
            df_common = _filter_common_intervals(df_pjm)
            if len(df_common) > 0:
                chart = _build_vintage_chart(
                    pjm_chart_id, df_common, None,
                    f"PJM {label}",
                )
                fragments.append((f"PJM {label}", chart, None))
            else:
                fragments.append((f"PJM {label}",
                                  _empty(f"No common intervals for PJM {label}."), None))
        else:
            fragments.append((f"PJM {label}",
                              _empty(f"No PJM vintage data for {label}."), None))

        # Meteologica chart
        df_meteo = vintage_data[region]["meteologica"]
        df_euro = euro_data[region]
        meteo_chart_id = f"{_PREFIX}Meteo{region}"
        if df_meteo is not None and len(df_meteo) > 0:
            df_common = _filter_common_intervals(df_meteo)
            if len(df_common) > 0:
                # Prep euro data
                prepped_euro = None
                if df_euro is not None and len(df_euro) > 0:
                    prepped_euro = _prep_euro(df_euro)
                chart = _build_vintage_chart(
                    meteo_chart_id, df_common, prepped_euro,
                    f"Meteologica {label}",
                )
                fragments.append((f"Meteologica {label}", chart, None))
            else:
                fragments.append((f"Meteologica {label}",
                                  _empty(f"No common intervals for Meteologica {label}."), None))
        else:
            fragments.append((f"Meteologica {label}",
                              _empty(f"No Meteologica vintage data for {label}."), None))


    return fragments


# ── Data helpers ────────────────────────────────────────────────────


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


def _vintage_order(df: pd.DataFrame) -> list[str]:
    """Return canonical vintage labels present in the DataFrame."""
    present = set(df["vintage_label"].unique())
    return [v for v in _VINTAGE_ORDER if v in present]


def _empty(text: str) -> str:
    return f"<div style='padding:16px;color:#e74c3c;'>{text}</div>"


def _prep(df: pd.DataFrame) -> pd.DataFrame:
    """Sort and add datetime + customdata columns."""
    df = df.sort_values(["forecast_date", "hour_ending"]).copy()
    df["datetime"] = pd.to_datetime(df["forecast_date"]) + pd.to_timedelta(df["hour_ending"], unit="h")
    df["_date_label"] = pd.to_datetime(df["forecast_date"]).dt.strftime("%a %b-%d")
    df["_he"] = df["hour_ending"].astype(int)
    return df


def _prep_euro(df: pd.DataFrame) -> pd.DataFrame:
    """Prep Euro ensemble data."""
    df = df.copy()
    df["forecast_date"] = pd.to_datetime(df["forecast_date"])
    df = df.sort_values(["forecast_date", "hour_ending"])
    df["datetime"] = df["forecast_date"] + pd.to_timedelta(df["hour_ending"], unit="h")
    df["_date_label"] = df["forecast_date"].dt.strftime("%a %b-%d")
    df["_he"] = df["hour_ending"].astype(int)
    return df


def _customdata(df: pd.DataFrame) -> np.ndarray:
    return np.column_stack([df["_date_label"], df["_he"]])


# ── Vintage badges ─────────────────────────────────────────────────


def _build_vintage_badges(df: pd.DataFrame, order: list[str]) -> str:
    """Color-coded badges displaying execution timestamps."""
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
    if (
        "vintage_anchor_execution_datetime" in df.columns
        and df["vintage_anchor_execution_datetime"].notna().any()
    ):
        return pd.to_datetime(df["vintage_anchor_execution_datetime"]).max()
    if "forecast_execution_datetime" in df.columns and df["forecast_execution_datetime"].notna().any():
        return pd.to_datetime(df["forecast_execution_datetime"]).max()
    return pd.NaT


# ── Chart builder ──────────────────────────────────────────────────


def _build_vintage_chart(
    chart_id: str,
    df: pd.DataFrame,
    df_euro: pd.DataFrame | None,
    title: str,
) -> str:
    """Build one vintage overlay chart with optional Euro band and ramp toggle.

    Returns HTML/JS string.  Trace order:
      - [Euro band: top, bottom, avg]  (Meteo only, always visible)
      - [Vintage load lines: Latest, DA Cutoff, ...]
      - [Vintage ramp lines: Latest, DA Cutoff, ...]  (hidden by default)
    """
    fig = go.Figure()
    order = _vintage_order(df)
    vintage_trace_map: dict[str, list[int]] = {v: [] for v in _VINTAGE_ORDER}

    # Euro ensemble band (always visible, not vintage-controlled)
    if df_euro is not None and len(df_euro) > 0:
        fig.add_trace(go.Scatter(
            x=df_euro["datetime"], y=df_euro["forecast_load_top_mw"],
            mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip",
        ))
        fig.add_trace(go.Scatter(
            x=df_euro["datetime"], y=df_euro["forecast_load_bottom_mw"],
            mode="lines", line=dict(width=0),
            fill="tonexty", fillcolor="rgba(99,110,250,0.10)",
            showlegend=False, hoverinfo="skip",
        ))
        fig.add_trace(go.Scatter(
            x=df_euro["datetime"], y=df_euro["forecast_load_average_mw"],
            mode="lines", name="Euro (Avg + Band)",
            line=dict(color="#636EFA", width=1, dash="dot"),
            customdata=_customdata(df_euro),
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "HE %{customdata[1]}<br>"
                "Euro Avg: %{y:,.0f} MW"
                "<extra></extra>"
            ),
        ))

    # Vintage load lines
    for label in order:
        sub = _prep(df[df["vintage_label"] == label].copy())
        if len(sub) == 0:
            continue
        idx = len(fig.data)
        vintage_trace_map[label].append(idx)
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
                "HE %{customdata[1]}<br>" +
                f"{label}: %{{y:,.0f}} MW"
                "<extra></extra>"
            ),
        ))

    # Vintage ramp bars (hidden by default, grouped on secondary y-axis)
    ramp_start_idx = len(fig.data)
    for label in order:
        sub = _prep(df[df["vintage_label"] == label].copy())
        if len(sub) == 0:
            continue
        ramp = sub["forecast_load_mw"].diff()
        idx = len(fig.data)
        vintage_trace_map[label].append(idx)
        fig.add_trace(go.Bar(
            x=sub["datetime"],
            y=ramp,
            name=f"{label} Ramp",
            marker_color=VINTAGE_COLORS.get(label, "#94a3b8"),
            opacity=0.6,
            visible=False,
            yaxis="y2",
            customdata=_customdata(sub),
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "HE %{customdata[1]}<br>" +
                f"{label} Ramp: %{{y:+,.0f}} MW/hr"
                "<extra></extra>"
            ),
        ))
    ramp_end_idx = len(fig.data) - 1

    # Layout
    fig.update_layout(
        title=title,
        xaxis=dict(tickformat="%a %b-%d %I %p",
                    gridcolor="rgba(99,110,250,0.08)"),
        yaxis=dict(title="Load (MW)", tickformat=".1s",
                    gridcolor="rgba(99,110,250,0.1)"),
        yaxis2=dict(title="Ramp (MW/hr)", overlaying="y", side="right",
                     showgrid=False, tickformat=".1s"),
        height=500, template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="top", y=-0.08, xanchor="left", x=0),
        margin=dict(l=60, r=60, t=40, b=60),
        barmode="group",
        bargap=0.1,
        hovermode="x unified",
    )

    # Build trace map JSON for vintage toggle JS
    trace_map_json = {v: vintage_trace_map[v] for v in order if vintage_trace_map[v]}

    return _assemble_chart(chart_id, fig, trace_map_json, ramp_start_idx, ramp_end_idx)


# ── Diff chart builder ─────────────────────────────────────────────


def _build_diff_chart(
    chart_id: str,
    df_pjm: pd.DataFrame,
    df_meteo: pd.DataFrame,
    title: str,
) -> str | None:
    """Build a PJM - Meteologica diff chart per vintage.

    For each vintage present in both sources, merges on (forecast_date,
    hour_ending) and plots the MW difference as a line.  A zero reference
    line is drawn for context.  Returns None if no common data exists.
    """
    _join_cols = ["vintage_label", "forecast_date", "hour_ending"]

    pjm = df_pjm[["vintage_label", "forecast_date", "hour_ending", "forecast_load_mw"]].copy()
    meteo = df_meteo[["vintage_label", "forecast_date", "hour_ending", "forecast_load_mw"]].copy()

    pjm["forecast_date"] = pd.to_datetime(pjm["forecast_date"])
    meteo["forecast_date"] = pd.to_datetime(meteo["forecast_date"])

    merged = pjm.merge(meteo, on=_join_cols, suffixes=("_pjm", "_meteo"), how="inner")
    if len(merged) == 0:
        return None

    merged["diff_mw"] = merged["forecast_load_mw_pjm"] - merged["forecast_load_mw_meteo"]

    order = [v for v in _VINTAGE_ORDER if v in merged["vintage_label"].unique()]
    if not order:
        return None

    fig = go.Figure()
    vintage_trace_map: dict[str, list[int]] = {v: [] for v in _VINTAGE_ORDER}

    # Zero reference line
    merged_prepped = _prep_diff(merged)
    fig.add_trace(go.Scatter(
        x=[merged_prepped["datetime"].min(), merged_prepped["datetime"].max()],
        y=[0, 0],
        mode="lines",
        line=dict(color="#4a5568", width=1, dash="dash"),
        showlegend=False,
        hoverinfo="skip",
    ))

    # Diff lines per vintage
    for label in order:
        sub = _prep_diff(merged[merged["vintage_label"] == label].copy())
        if len(sub) == 0:
            continue
        idx = len(fig.data)
        vintage_trace_map[label].append(idx)
        fig.add_trace(go.Scatter(
            x=sub["datetime"],
            y=sub["diff_mw"],
            mode="lines",
            name=label,
            line=dict(
                color=VINTAGE_COLORS.get(label, "#94a3b8"),
                width=VINTAGE_WIDTH.get(label, 1.5),
                dash=VINTAGE_DASH.get(label, "solid"),
            ),
            customdata=np.column_stack([
                sub["_date_label"], sub["_he"],
                sub["forecast_load_mw_pjm"].map("{:,.0f}".format),
                sub["forecast_load_mw_meteo"].map("{:,.0f}".format),
            ]),
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "HE %{customdata[1]}<br>"
                f"{label} Diff: %{{y:+,.0f}} MW<br>"
                "PJM: %{customdata[2]} MW | Meteo: %{customdata[3]} MW"
                "<extra></extra>"
            ),
        ))

    # Layout
    fig.update_layout(
        title=title,
        xaxis=dict(tickformat="%a %b-%d %I %p",
                    gridcolor="rgba(99,110,250,0.08)"),
        yaxis=dict(title="PJM − Meteologica (MW)", tickformat=",.0f",
                    gridcolor="rgba(99,110,250,0.1)",
                    zeroline=True, zerolinecolor="#4a5568", zerolinewidth=1),
        height=400, template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="top", y=-0.08, xanchor="left", x=0),
        margin=dict(l=60, r=60, t=40, b=60),
        hovermode="x unified",
    )

    trace_map_json = {v: vintage_trace_map[v] for v in order if vintage_trace_map[v]}
    # No ramp traces on diff chart
    return _assemble_chart(chart_id, fig, trace_map_json, len(fig.data), len(fig.data) - 1)


def _prep_diff(df: pd.DataFrame) -> pd.DataFrame:
    """Sort and add datetime + customdata columns for diff data."""
    df = df.sort_values(["forecast_date", "hour_ending"]).copy()
    df["datetime"] = pd.to_datetime(df["forecast_date"]) + pd.to_timedelta(df["hour_ending"], unit="h")
    df["_date_label"] = pd.to_datetime(df["forecast_date"]).dt.strftime("%a %b-%d")
    df["_he"] = df["hour_ending"].astype(int)
    return df


# ── Chart assembly ─────────────────────────────────────────────────


def _assemble_chart(chart_id, fig, trace_map, ramp_start, ramp_end):
    """Serialize figure + build per-chart ramp toggle + register vintage map."""
    import json
    fig_json = pio.to_json(fig)
    trace_map_json = json.dumps(trace_map)

    ramp_indices = list(range(ramp_start, ramp_end + 1)) if ramp_end >= ramp_start else []
    ramp_indices_json = json.dumps(ramp_indices)

    return (
        _CHART_TEMPLATE
        .replace("__CHART_ID__", chart_id)
        .replace("__FIG_JSON__", fig_json)
        .replace("__TRACE_MAP__", trace_map_json)
        .replace("__RAMP_INDICES__", ramp_indices_json)
        .replace("__RAMP_FN__", f"{_PREFIX}Ramp")
    )


# ── Global controls builder ────────────────────────────────────────


def _build_global_controls(filter_id, chart_ids, dates):
    """Build shared date pills + vintage toggle pills."""
    # Date pill buttons
    date_btns = (
        f'<button class="fc-btn fc-btn-{filter_id} fc-active" '
        f"onclick=\"{_PREFIX}DateFilter('{filter_id}',this,'all')\">All</button>\n"
    )
    for dt in dates:
        label = dt.strftime("%a %b-%d")
        iso = dt.isoformat()
        date_btns += (
            f'<button class="fc-btn fc-btn-{filter_id}" '
            f"onclick=\"{_PREFIX}DateFilter('{filter_id}',this,'{iso}')\">{label}</button>\n"
        )

    # Vintage toggle buttons
    vintage_btns = ""
    for v in _VINTAGE_ORDER:
        color = VINTAGE_COLORS[v]
        vintage_btns += (
            f'<button class="fc-btn fc-btn-vint-{_PREFIX} fc-active" '
            f'style="border-left:3px solid {color};" '
            f"onclick=\"{_PREFIX}Vintage(this,'{v}')\">{v}</button>\n"
        )

    chart_ids_js = "[" + ",".join(f"'{cid}'" for cid in chart_ids) + "]"

    return (
        _GLOBAL_CONTROLS_TEMPLATE
        .replace("__FILTER_ID__", filter_id)
        .replace("__DATE_BTNS__", date_btns)
        .replace("__VINTAGE_BTNS__", vintage_btns)
        .replace("__CHART_IDS__", chart_ids_js)
        .replace("__DATE_FILTER_FN__", f"{_PREFIX}DateFilter")
        .replace("__VINTAGE_FN__", f"{_PREFIX}Vintage")
        .replace("__STATE_KEY__", f"_vintCombState")
    )


# ── HTML/JS templates ──────────────────────────────────────────────

_GLOBAL_CONTROLS_TEMPLATE = """
<div style="display:flex;flex-direction:column;gap:4px;padding:10px 12px;">

  <div style="display:flex;align-items:center;gap:6px;overflow-x:auto;flex-wrap:nowrap;">
    <span style="font-size:11px;font-weight:600;color:#6f8db1;white-space:nowrap;margin-right:4px;">
      FORECAST DATE
    </span>
    __DATE_BTNS__
  </div>

  <div style="display:flex;align-items:center;gap:6px;overflow-x:auto;flex-wrap:nowrap;">
    <span style="font-size:11px;font-weight:600;color:#6f8db1;white-space:nowrap;margin-right:4px;">
      VINTAGE
    </span>
    __VINTAGE_BTNS__
  </div>

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
  var chartIds = __CHART_IDS__;

  /* ── Global state ───────────────────────────────── */
  if (!window.__STATE_KEY__) {
    window.__STATE_KEY__ = {
      visible: {},
      charts: {}
    };
  }
  var STATE = window.__STATE_KEY__;

  /* Initialise all vintages as visible */
  var vintageOrder = ['Latest','DA Cutoff','DA -12h','DA -24h','DA -48h'];
  vintageOrder.forEach(function(v) { STATE.visible[v] = true; });

  /* ── Date filter (broadcast relayout) ───────────── */
  function pad2(n) { return String(n).padStart(2, '0'); }
  function toLocalTs(d) {
    return d.getFullYear() + '-' +
           pad2(d.getMonth() + 1) + '-' +
           pad2(d.getDate()) + 'T' +
           pad2(d.getHours()) + ':' +
           pad2(d.getMinutes()) + ':' +
           pad2(d.getSeconds());
  }

  window.__DATE_FILTER_FN__ = function(filterId, btn, dateStr) {
    document.querySelectorAll('.fc-btn-' + filterId).forEach(function(b) {
      b.classList.remove('fc-active');
    });
    btn.classList.add('fc-active');

    chartIds.forEach(function(chartId) {
      var el = document.getElementById(chartId);
      if (!el) return;
      if (dateStr === 'all') {
        Plotly.relayout(chartId, {'xaxis.autorange': true});
      } else {
        var d = new Date(dateStr + 'T00:00:00');
        var start = new Date(d.getTime() + 1 * 3600000);
        var end   = new Date(d.getTime() + 24 * 3600000);
        Plotly.relayout(chartId, {
          'xaxis.autorange': false,
          'xaxis.range': [toLocalTs(start), toLocalTs(end)]
        });
      }
    });
  };

  /* ── Vintage toggle (broadcast restyle) ─────────── */
  window.__VINTAGE_FN__ = function(btn, vintageLabel) {
    STATE.visible[vintageLabel] = !STATE.visible[vintageLabel];
    var vis = STATE.visible[vintageLabel];
    btn.classList.toggle('fc-active', vis);

    Object.keys(STATE.charts).forEach(function(chartId) {
      var traceMap = STATE.charts[chartId];
      var indices = traceMap[vintageLabel];
      if (!indices || indices.length === 0) return;

      /* Only show load traces (first index); ramp traces respect ramp toggle */
      var rampState = (window._fcState && window._fcState[chartId]) || {};
      indices.forEach(function(idx) {
        var isRamp = (rampState.rampIndices && rampState.rampIndices.indexOf(idx) !== -1);
        if (isRamp) {
          /* Ramp trace: visible only if BOTH vintage is on AND ramp is on */
          Plotly.restyle(chartId, {'visible': vis && rampState.rampVisible}, [idx]);
        } else {
          Plotly.restyle(chartId, {'visible': vis}, [idx]);
        }
      });
    });
  };
})();
</script>
"""

_CHART_TEMPLATE = """
<div style="position:relative;">

  <div style="display:flex;align-items:center;gap:6px;padding:10px 12px;justify-content:flex-end;">
    <button id="rampToggle___CHART_ID__" onclick="__RAMP_FN__('__CHART_ID__')"
      style="padding:4px 12px;font-size:11px;font-weight:600;
             background:#1a2a42;color:#9eb4d3;border:1px solid #2a3f60;
             border-radius:4px;cursor:pointer;font-family:inherit;
             text-transform:uppercase;letter-spacing:0.5px;white-space:nowrap;flex-shrink:0;">
      SHOW RAMP
    </button>
  </div>

  <div id="__CHART_ID__" style="width:100%;"></div>
</div>

<script>
(function() {
  var cid = '__CHART_ID__';
  var fig = __FIG_JSON__;
  var traceMap = __TRACE_MAP__;
  var rampIndices = __RAMP_INDICES__;

  Plotly.newPlot(cid, fig.data, fig.layout, {responsive: true});

  /* Register vintage trace map in global state */
  var STATE = window._vintCombState;
  if (STATE) { STATE.charts[cid] = traceMap; }

  /* Per-chart ramp state */
  if (!window._fcState) window._fcState = {};
  window._fcState[cid] = {
    rampIndices: rampIndices,
    rampVisible: false
  };

  window.__RAMP_FN__ = function(chartId) {
    var st = window._fcState[chartId];
    st.rampVisible = !st.rampVisible;
    var vintState = window._vintCombState;

    st.rampIndices.forEach(function(idx) {
      /* Find which vintage this ramp trace belongs to */
      var vintVisible = true;
      if (vintState) {
        var map = vintState.charts[chartId] || {};
        Object.keys(map).forEach(function(v) {
          if (map[v].indexOf(idx) !== -1) {
            vintVisible = vintState.visible[v] !== false;
          }
        });
      }
      Plotly.restyle(chartId, {'visible': st.rampVisible && vintVisible}, [idx]);
    });

    var btn = document.getElementById('rampToggle_' + chartId);
    btn.textContent = st.rampVisible ? 'HIDE RAMP' : 'SHOW RAMP';
    btn.style.borderColor = st.rampVisible ? '#4cc9f0' : '#2a3f60';
  };
})();
</script>
"""
