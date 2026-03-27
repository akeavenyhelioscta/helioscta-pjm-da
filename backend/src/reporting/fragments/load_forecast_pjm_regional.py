"""PJM regional load forecast report — WEST, MIDATL, SOUTH.

One chart per region, controlled by a shared global date-filter bar.
Each chart has an independent ramp toggle.
Hover shows: Date, HE, Source: Value MW.
"""
import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

from src.like_day_forecast import configs
from src.data import pjm_load_forecast_hourly
from src.utils.cache_utils import pull_with_cache

logger = logging.getLogger(__name__)

PLOTLY_TEMPLATE = "plotly_dark"
REGIONS = ["WEST", "MIDATL", "SOUTH"]
_REGION_LABELS = {"WEST": "Western", "MIDATL": "Mid-Atlantic", "SOUTH": "Southern"}
_PREFIX = "pjmReg"

Section = tuple[str, Any, str | None]


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list:
    """Pull PJM forecast strips for WEST / MIDATL / SOUTH, return fragments."""
    logger.info("Building PJM regional load forecast report...")

    cache_kwargs = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    # Pull data per region
    region_data: dict[str, pd.DataFrame | None] = {}
    for region in REGIONS:
        region_data[region] = _safe_pull(
            f"pjm_load_strip_{region.lower()}",
            pjm_load_forecast_hourly.pull_strip,
            {"region": region},
            **cache_kwargs,
        )

    # Collect all dates across regions for the global filter
    all_dates: set = set()
    for df in region_data.values():
        if df is not None and len(df) > 0:
            all_dates |= set(pd.to_datetime(df["forecast_date"]).dt.date.unique())
    all_dates_sorted = sorted(all_dates)

    chart_ids = [f"{_PREFIX}{region}" for region in REGIONS]

    fragments: list = []

    # 1. Global date filter bar
    filter_html = _build_global_filter(f"{_PREFIX}Filter", chart_ids, all_dates_sorted)
    fragments.append(("", filter_html, None))

    # 2. Divider
    fragments.append("Regional Load Forecasts — PJM")

    # 3-5. One chart per region
    for region in REGIONS:
        chart_id = f"{_PREFIX}{region}"
        label = _REGION_LABELS[region]
        df = region_data[region]

        if df is not None and len(df) > 0:
            vintage = _vintage_label(df)
            chart = _build_chart(chart_id, df, f"PJM {label} — Load",
                                 "PJM Load", "#FFA15A")
            fragments.append((f"PJM {label} Forecast{vintage}", chart, None))
        else:
            fragments.append((f"PJM {label} Forecast", _empty(f"No PJM forecast data for {label}."), None))

    return fragments


# -- Helpers ------------------------------------------------------------------


def _safe_pull(source_name, pull_fn, pull_kwargs, **cache_kwargs):
    try:
        df = pull_with_cache(source_name=source_name, pull_fn=pull_fn,
                             pull_kwargs=pull_kwargs, **cache_kwargs)
        df["forecast_date"] = pd.to_datetime(df["forecast_date"])
        return df
    except Exception as e:
        logger.warning(f"{source_name} pull failed: {e}")
        return None


def _vintage_label(df):
    if "forecast_execution_datetime" in df.columns:
        exec_ts = pd.to_datetime(df["forecast_execution_datetime"], errors="coerce").dropna()
        if len(exec_ts) == 0:
            return ""
        dt_min = exec_ts.min()
        dt_max = exec_ts.max()
        if dt_min == dt_max:
            return f" - Vintage {dt_max.strftime('%m/%d/%Y, %I:%M %p')}"
        return (
            f" - Composite Vintage {dt_min.strftime('%m/%d/%Y, %I:%M %p')}"
            f" to {dt_max.strftime('%m/%d/%Y, %I:%M %p')}"
        )
    return ""


def _empty(text):
    return f"<div style='padding:16px;color:#e74c3c;'>{text}</div>"


def _prep(df):
    """Sort and add datetime + customdata columns."""
    df = df.sort_values(["forecast_date", "hour_ending"]).copy()
    df["datetime"] = df["forecast_date"] + pd.to_timedelta(df["hour_ending"], unit="h")
    df["_date_label"] = df["forecast_date"].dt.strftime("%a %b-%d")
    df["_he"] = df["hour_ending"].astype(int)
    return df


def _customdata(df):
    """Column stack for hovertemplate: [date_label, HE]."""
    return np.column_stack([df["_date_label"], df["_he"]])


def _hover(source_label):
    """Standard hovertemplate: Date, HE, Source: Value MW."""
    return (
        "<b>%{customdata[0]}</b><br>"
        "HE %{customdata[1]}<br>" +
        source_label + ": %{y:,.0f} MW"
        "<extra></extra>"
    )


# -- Single-source chart (no per-chart date pills) ---------------------------


def _build_chart(chart_id, df_load, title, load_label, load_color):
    df_load = _prep(df_load)
    fig = go.Figure()

    # Load line
    fig.add_trace(go.Scatter(
        x=df_load["datetime"], y=df_load["forecast_load_mw"],
        mode="lines", name=load_label,
        line=dict(color=load_color, width=2),
        customdata=_customdata(df_load),
        hovertemplate=_hover(load_label),
    ))

    # Ramp (hidden)
    ramp = df_load["forecast_load_mw"].diff()
    ramp_colors = ["#2ecc71" if v >= 0 else "#e74c3c" for v in ramp.fillna(0)]
    fig.add_trace(go.Bar(
        x=df_load["datetime"], y=ramp,
        name="Ramp (MW/hr)", marker_color=ramp_colors, opacity=0.6,
        visible=False, yaxis="y2",
        customdata=_customdata(df_load),
        hovertemplate=_hover("Ramp"),
    ))
    ramp_idx = len(fig.data) - 1

    _apply_layout(fig, title)
    return _assemble_chart(chart_id, fig, ramp_idx)


# -- Shared layout + assembly ------------------------------------------------


def _apply_layout(fig, title, y2_title="Ramp (MW/hr)"):
    fig.update_layout(
        title=title,
        xaxis=dict(tickformat="%a %b-%d %I %p",
                    gridcolor="rgba(99,110,250,0.08)"),
        yaxis=dict(title="Load (MW)", tickformat=".1s",
                    gridcolor="rgba(99,110,250,0.1)"),
        yaxis2=dict(title=y2_title, overlaying="y", side="right",
                     showgrid=False, tickformat=".1s"),
        height=500, template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="top", y=-0.08, xanchor="left", x=0),
        margin=dict(l=60, r=60, t=40, b=60),
        bargap=0.1,
        hovermode="x unified",
    )


def _assemble_chart(chart_id, fig, ramp_idx, toggle_label="RAMP"):
    """Serialize figure + per-chart ramp toggle (no date pills)."""
    fig_json = pio.to_json(fig)
    show_label = f"SHOW {toggle_label}"
    hide_label = f"HIDE {toggle_label}"

    return (
        _CHART_TEMPLATE
        .replace("__CHART_ID__", chart_id)
        .replace("__FIG_JSON__", fig_json)
        .replace("__RAMP_IDX__", str(ramp_idx))
        .replace("__SHOW_LABEL__", show_label)
        .replace("__HIDE_LABEL__", hide_label)
        .replace("__RAMP_FN__", f"{_PREFIX}Ramp")
    )


# -- Global date filter -------------------------------------------------------


def _build_global_filter(filter_id, chart_ids, dates):
    """Build a shared date-pill row that broadcasts Plotly.relayout to all charts."""
    btn_html = (
        f'<button class="fc-btn fc-btn-{filter_id} fc-active" '
        f"onclick=\"{_PREFIX}Filter('{filter_id}',this,'all')\">All</button>\n"
    )
    for dt in dates:
        label = dt.strftime("%a %b-%d")
        iso = dt.isoformat()
        btn_html += (
            f'<button class="fc-btn fc-btn-{filter_id}" '
            f"onclick=\"{_PREFIX}Filter('{filter_id}',this,'{iso}')\">{label}</button>\n"
        )

    chart_ids_js = "[" + ",".join(f"'{cid}'" for cid in chart_ids) + "]"

    return (
        _GLOBAL_FILTER_TEMPLATE
        .replace("__FILTER_ID__", filter_id)
        .replace("__DATE_BTNS__", btn_html)
        .replace("__CHART_IDS__", chart_ids_js)
        .replace("__FILTER_FN__", f"{_PREFIX}Filter")
    )


# -- HTML/JS templates -------------------------------------------------------

_GLOBAL_FILTER_TEMPLATE = """
<div style="display:flex;align-items:center;gap:6px;padding:10px 12px;overflow-x:auto;flex-wrap:nowrap;">
  <span style="font-size:11px;font-weight:600;color:#6f8db1;white-space:nowrap;margin-right:4px;">
    FORECAST DATE
  </span>
  __DATE_BTNS__
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

  function pad2(n) { return String(n).padStart(2, '0'); }
  function toLocalTs(d) {
    return d.getFullYear() + '-' +
           pad2(d.getMonth() + 1) + '-' +
           pad2(d.getDate()) + 'T' +
           pad2(d.getHours()) + ':' +
           pad2(d.getMinutes()) + ':' +
           pad2(d.getSeconds());
  }

  window.__FILTER_FN__ = function(filterId, btn, dateStr) {
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
      __SHOW_LABEL__
    </button>
  </div>

  <div id="__CHART_ID__" style="width:100%;"></div>
</div>

<script>
(function() {
  var cid = '__CHART_ID__';
  var fig = __FIG_JSON__;
  Plotly.newPlot(cid, fig.data, fig.layout, {responsive: true});

  if (!window._fcState) window._fcState = {};
  window._fcState[cid] = {
    rampIdx: __RAMP_IDX__,
    rampVisible: false,
    showLabel: '__SHOW_LABEL__',
    hideLabel: '__HIDE_LABEL__'
  };

  window.__RAMP_FN__ = function(chartId) {
    var st = window._fcState[chartId];
    st.rampVisible = !st.rampVisible;
    Plotly.restyle(chartId, {'visible': st.rampVisible}, [st.rampIdx]);
    var btn = document.getElementById('rampToggle_' + chartId);
    btn.textContent = st.rampVisible ? st.hideLabel : st.showLabel;
    btn.style.borderColor = st.rampVisible ? '#4cc9f0' : '#2a3f60';
  };
})();
</script>
"""
