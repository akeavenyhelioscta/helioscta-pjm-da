"""Shared chart utilities for forecast vintage dashboards.

Provides vintage styling constants, chart builders, and HTML/JS templates
used across load, solar, wind, and net load vintage dashboards.
"""
import json
import logging
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

logger = logging.getLogger(__name__)

PLOTLY_TEMPLATE = "plotly_dark"

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
VINTAGE_ORDER = ["Latest", "DA Cutoff", "DA -12h", "DA -24h", "DA -48h"]

REGIONS = ["RTO", "WEST", "MIDATL", "SOUTH"]
REGION_LABELS = {
    "RTO": "RTO",
    "WEST": "Western",
    "MIDATL": "Mid-Atlantic",
    "SOUTH": "Southern",
}

Section = tuple[str, Any, str | None]


# ── Data helpers ────────────────────────────────────────────────────


def vintage_order(df: pd.DataFrame) -> list[str]:
    """Return canonical vintage labels present in the DataFrame."""
    present = set(df["vintage_label"].unique())
    return [v for v in VINTAGE_ORDER if v in present]


def empty(text: str) -> str:
    return f"<div style='padding:16px;color:#e74c3c;'>{text}</div>"


def prep(df: pd.DataFrame) -> pd.DataFrame:
    """Sort and add datetime + customdata columns."""
    df = df.sort_values(["forecast_date", "hour_ending"]).copy()
    df["datetime"] = pd.to_datetime(df["forecast_date"]) + pd.to_timedelta(df["hour_ending"], unit="h")
    df["_date_label"] = pd.to_datetime(df["forecast_date"]).dt.strftime("%a %b-%d")
    df["_he"] = df["hour_ending"].astype(int)
    return df


def customdata(df: pd.DataFrame) -> np.ndarray:
    return np.column_stack([df["_date_label"], df["_he"]])


def filter_common_intervals(df: pd.DataFrame) -> pd.DataFrame:
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


def display_exec_ts(df: pd.DataFrame):
    if (
        "vintage_anchor_execution_datetime" in df.columns
        and df["vintage_anchor_execution_datetime"].notna().any()
    ):
        return pd.to_datetime(df["vintage_anchor_execution_datetime"]).max()
    if "forecast_execution_datetime" in df.columns and df["forecast_execution_datetime"].notna().any():
        return pd.to_datetime(df["forecast_execution_datetime"]).max()
    return pd.NaT


# ── Vintage badges ─────────────────────────────────────────────────


def build_vintage_badges(df: pd.DataFrame, order: list[str]) -> str:
    """Color-coded badges displaying execution timestamps."""
    badges = ""
    for label in order:
        sub = df[df["vintage_label"] == label]
        if len(sub) == 0:
            continue
        exec_ts = display_exec_ts(sub)
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


# ── Chart builders ─────────────────────────────────────────────────


def build_vintage_chart(
    chart_id: str,
    df: pd.DataFrame,
    title: str,
    value_col: str = "forecast_mw",
    y_title: str = "MW",
    prefix: str = "vintComb",
) -> str:
    """Build a vintage overlay chart with ramp toggle.

    Args:
        chart_id: Unique DOM ID for the chart.
        df: DataFrame with vintage_label, forecast_date, hour_ending, and value_col.
        title: Chart title.
        value_col: Column name for the forecast value.
        y_title: Y-axis title.
        prefix: JS namespace prefix.
    """
    fig = go.Figure()
    order = vintage_order(df)
    vintage_trace_map: dict[str, list[int]] = {v: [] for v in VINTAGE_ORDER}

    # Vintage lines
    for label in order:
        sub = prep(df[df["vintage_label"] == label].copy())
        if len(sub) == 0:
            continue
        idx = len(fig.data)
        vintage_trace_map[label].append(idx)
        fig.add_trace(go.Scatter(
            x=sub["datetime"],
            y=sub[value_col],
            mode="lines",
            name=label,
            line=dict(
                color=VINTAGE_COLORS.get(label, "#94a3b8"),
                width=VINTAGE_WIDTH.get(label, 1.5),
                dash=VINTAGE_DASH.get(label, "solid"),
            ),
            customdata=customdata(sub),
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "HE %{customdata[1]}<br>" +
                f"{label}: %{{y:,.0f}} MW"
                "<extra></extra>"
            ),
        ))

    # Vintage ramp bars (hidden by default)
    ramp_start_idx = len(fig.data)
    for label in order:
        sub = prep(df[df["vintage_label"] == label].copy())
        if len(sub) == 0:
            continue
        ramp = sub[value_col].diff()
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
            customdata=customdata(sub),
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "HE %{customdata[1]}<br>" +
                f"{label} Ramp: %{{y:+,.0f}} MW/hr"
                "<extra></extra>"
            ),
        ))
    ramp_end_idx = len(fig.data) - 1

    fig.update_layout(
        title=title,
        xaxis=dict(tickformat="%a %b-%d %I %p",
                    gridcolor="rgba(99,110,250,0.08)"),
        yaxis=dict(title=y_title, tickformat=".1s",
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

    trace_map_json = {v: vintage_trace_map[v] for v in order if vintage_trace_map[v]}
    return assemble_chart(chart_id, fig, trace_map_json, ramp_start_idx, ramp_end_idx, prefix)


def build_diff_chart(
    chart_id: str,
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    title: str,
    value_col: str = "forecast_mw",
    label_a: str = "PJM",
    label_b: str = "Meteo",
    prefix: str = "vintComb",
) -> str | None:
    """Build a diff chart (A − B) per vintage.

    Returns None if no common data exists.
    """
    _join_cols = ["vintage_label", "forecast_date", "hour_ending"]

    a = df_a[["vintage_label", "forecast_date", "hour_ending", value_col]].copy()
    b = df_b[["vintage_label", "forecast_date", "hour_ending", value_col]].copy()

    a["forecast_date"] = pd.to_datetime(a["forecast_date"])
    b["forecast_date"] = pd.to_datetime(b["forecast_date"])

    merged = a.merge(b, on=_join_cols, suffixes=("_a", "_b"), how="inner")
    if len(merged) == 0:
        return None

    val_a = f"{value_col}_a"
    val_b = f"{value_col}_b"
    merged["diff_mw"] = merged[val_a] - merged[val_b]

    order = [v for v in VINTAGE_ORDER if v in merged["vintage_label"].unique()]
    if not order:
        return None

    fig = go.Figure()
    vintage_trace_map: dict[str, list[int]] = {v: [] for v in VINTAGE_ORDER}

    # Zero reference line
    merged_prepped = prep(merged)
    fig.add_trace(go.Scatter(
        x=[merged_prepped["datetime"].min(), merged_prepped["datetime"].max()],
        y=[0, 0],
        mode="lines",
        line=dict(color="#4a5568", width=1, dash="dash"),
        showlegend=False,
        hoverinfo="skip",
    ))

    for label in order:
        sub = prep(merged[merged["vintage_label"] == label].copy())
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
                sub[val_a].map("{:,.0f}".format),
                sub[val_b].map("{:,.0f}".format),
            ]),
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "HE %{customdata[1]}<br>"
                f"{label} Diff: %{{y:+,.0f}} MW<br>"
                f"{label_a}: %{{customdata[2]}} MW | {label_b}: %{{customdata[3]}} MW"
                "<extra></extra>"
            ),
        ))

    fig.update_layout(
        title=title,
        xaxis=dict(tickformat="%a %b-%d %I %p",
                    gridcolor="rgba(99,110,250,0.08)"),
        yaxis=dict(title=f"{label_a} − {label_b} (MW)", tickformat=",.0f",
                    gridcolor="rgba(99,110,250,0.1)",
                    zeroline=True, zerolinecolor="#4a5568", zerolinewidth=1),
        height=400, template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="top", y=-0.08, xanchor="left", x=0),
        margin=dict(l=60, r=60, t=40, b=60),
        hovermode="x unified",
    )

    trace_map_json = {v: vintage_trace_map[v] for v in order if vintage_trace_map[v]}
    return assemble_chart(chart_id, fig, trace_map_json, len(fig.data), len(fig.data) - 1, prefix)


# ── Chart assembly ─────────────────────────────────────────────────


def assemble_chart(chart_id, fig, trace_map, ramp_start, ramp_end, prefix="vintComb"):
    """Serialize figure + build per-chart ramp toggle + register vintage map."""
    fig_json = pio.to_json(fig)
    trace_map_json = json.dumps(trace_map)

    ramp_indices = list(range(ramp_start, ramp_end + 1)) if ramp_end >= ramp_start else []
    ramp_indices_json = json.dumps(ramp_indices)

    return (
        CHART_TEMPLATE
        .replace("__CHART_ID__", chart_id)
        .replace("__FIG_JSON__", fig_json)
        .replace("__TRACE_MAP__", trace_map_json)
        .replace("__RAMP_INDICES__", ramp_indices_json)
        .replace("__RAMP_FN__", f"{prefix}Ramp")
        .replace("__STATE_KEY__", f"_{prefix}State")
    )


# ── Global controls builder ────────────────────────────────────────


def build_global_controls(filter_id, chart_ids, dates, prefix="vintComb"):
    """Build shared date pills + vintage toggle pills."""
    date_btns = (
        f'<button class="fc-btn fc-btn-{filter_id} fc-active" '
        f"onclick=\"{prefix}DateFilter('{filter_id}',this,'all')\">All</button>\n"
    )
    for dt in dates:
        label = dt.strftime("%a %b-%d")
        iso = dt.isoformat()
        date_btns += (
            f'<button class="fc-btn fc-btn-{filter_id}" '
            f"onclick=\"{prefix}DateFilter('{filter_id}',this,'{iso}')\">{label}</button>\n"
        )

    vintage_btns = ""
    for v in VINTAGE_ORDER:
        color = VINTAGE_COLORS[v]
        vintage_btns += (
            f'<button class="fc-btn fc-btn-vint-{prefix} fc-active" '
            f'style="border-left:3px solid {color};" '
            f"onclick=\"{prefix}Vintage(this,'{v}')\">{v}</button>\n"
        )

    chart_ids_js = "[" + ",".join(f"'{cid}'" for cid in chart_ids) + "]"

    return (
        GLOBAL_CONTROLS_TEMPLATE
        .replace("__FILTER_ID__", filter_id)
        .replace("__DATE_BTNS__", date_btns)
        .replace("__VINTAGE_BTNS__", vintage_btns)
        .replace("__CHART_IDS__", chart_ids_js)
        .replace("__DATE_FILTER_FN__", f"{prefix}DateFilter")
        .replace("__VINTAGE_FN__", f"{prefix}Vintage")
        .replace("__STATE_KEY__", f"_{prefix}State")
    )


# ── HTML/JS templates ──────────────────────────────────────────────

GLOBAL_CONTROLS_TEMPLATE = """
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

  if (!window.__STATE_KEY__) {
    window.__STATE_KEY__ = {
      visible: {},
      charts: {}
    };
  }
  var STATE = window.__STATE_KEY__;

  var vintageOrder = ['Latest','DA Cutoff','DA -12h','DA -24h','DA -48h'];
  vintageOrder.forEach(function(v) { STATE.visible[v] = true; });

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

  window.__VINTAGE_FN__ = function(btn, vintageLabel) {
    STATE.visible[vintageLabel] = !STATE.visible[vintageLabel];
    var vis = STATE.visible[vintageLabel];
    btn.classList.toggle('fc-active', vis);

    Object.keys(STATE.charts).forEach(function(chartId) {
      var traceMap = STATE.charts[chartId];
      var indices = traceMap[vintageLabel];
      if (!indices || indices.length === 0) return;

      var rampState = (window._fcState && window._fcState[chartId]) || {};
      indices.forEach(function(idx) {
        var isRamp = (rampState.rampIndices && rampState.rampIndices.indexOf(idx) !== -1);
        if (isRamp) {
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

CHART_TEMPLATE = """
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

  var STATE = window.__STATE_KEY__;
  if (STATE) { STATE.charts[cid] = traceMap; }

  if (!window._fcState) window._fcState = {};
  window._fcState[cid] = {
    rampIndices: rampIndices,
    rampVisible: false
  };

  window.__RAMP_FN__ = function(chartId) {
    var st = window._fcState[chartId];
    st.rampVisible = !st.rampVisible;
    var vintState = window.__STATE_KEY__;

    st.rampIndices.forEach(function(idx) {
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
