"""LMP History dashboard — DA, RT, and DART for Western Hub.

Sections:
  1. Global controls  — page-level component toggle (Total LMP / Congestion / System Energy)
  2. LMP Summary      — today's HE1-24 table for DA, RT, DART with OnPeak/OffPeak/Avg
  3. LMP Profiles     — 3 overlaid hourly profiles (DA, RT, DART) in one row

The component toggle broadcasts to both the summary table and all 3 charts,
following the same global-state pattern as load_forecast_vintage_combined.
"""
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

from src.like_day_forecast import configs
from src.data import lmps_hourly
from src.utils.cache_utils import pull_with_cache

logger = logging.getLogger(__name__)

PLOTLY_TEMPLATE = "plotly_dark"
LOOKBACK_DAYS = 30

_PREFIX = "lmpHist"

COMPONENTS = [
    ("lmp_total", "Total LMP"),
    ("lmp_congestion_price", "Congestion"),
    ("lmp_system_energy_price", "System Energy"),
]

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
) -> list:
    """Pull DA and RT LMP data, compute DART, return control + table + chart fragments."""
    logger.info("Building LMP history report fragments...")

    cache_kwargs = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    logger.info("Pulling DA LMP hourly...")
    df_da = pull_with_cache(
        source_name="lmps_hourly_da",
        pull_fn=lmps_hourly.pull,
        pull_kwargs={"schema": schema, "market": "da"},
        **cache_kwargs,
    )
    df_da = df_da[df_da["hub"] == configs.HUB]

    logger.info("Pulling RT LMP hourly...")
    df_rt = pull_with_cache(
        source_name="lmps_hourly_rt",
        pull_fn=lmps_hourly.pull,
        pull_kwargs={"schema": schema, "market": "rt"},
        **cache_kwargs,
    )
    df_rt = df_rt[df_rt["hub"] == configs.HUB]

    df_da = _prep(df_da)
    df_rt = _prep(df_rt)
    df_dart = _compute_dart(df_da, df_rt)

    chart_ids = [f"{_PREFIX}DA", f"{_PREFIX}RT", f"{_PREFIX}DART"]

    controls_html = _build_global_controls(chart_ids)
    # Use the latest date present in BOTH DA and RT so all 3 rows populate
    table_date = min(df_da["date"].max(), df_rt["date"].max())
    table_html = _build_summary_table(df_da, df_rt, df_dart, table_date)
    charts_html = _build_charts_row(df_da, df_rt, df_dart, chart_ids)

    return [
        ("", controls_html, None),
        ("LMP Summary — Western Hub", table_html, None),
        ("LMP Profiles — Western Hub", charts_html, None),
    ]


# ── Data prep ────────────────────────────────────────────────────────


def _prep(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to last N days and remap hour 0 → HE 24."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    cutoff = df["date"].max() - pd.Timedelta(days=LOOKBACK_DAYS)
    df = df[df["date"] >= cutoff].copy()
    df["hour_ending"] = df["hour_ending"].replace(0, 24)
    df = df[df["hour_ending"].between(1, 24)]
    return df


def _compute_dart(df_da: pd.DataFrame, df_rt: pd.DataFrame) -> pd.DataFrame:
    """Compute DA - RT for each LMP component."""
    merge_cols = ["date", "hour_ending"]
    lmp_cols = [col for col, _ in COMPONENTS]

    da_sub = df_da[merge_cols + lmp_cols].copy()
    rt_sub = df_rt[merge_cols + lmp_cols].copy()

    merged = da_sub.merge(rt_sub, on=merge_cols, suffixes=("_da", "_rt"), how="inner")

    dart = merged[merge_cols].copy()
    for col in lmp_cols:
        dart[col] = merged[f"{col}_da"] - merged[f"{col}_rt"]

    return dart.sort_values(merge_cols)


# ── Summary Table ────────────────────────────────────────────────────


def _build_summary_table(
    df_da: pd.DataFrame,
    df_rt: pd.DataFrame,
    df_dart: pd.DataFrame,
    table_date,
) -> str:
    """Build today's LMP summary tables (one per component, toggled by JS)."""
    html = ""

    for comp_idx, (col, comp_label) in enumerate(COMPONENTS):
        display = "block" if comp_idx == 0 else "none"
        div_id = f"{_PREFIX}Table_{col}"

        da_today = df_da[df_da["date"] == table_date].set_index("hour_ending")[col]
        rt_today = df_rt[df_rt["date"] == table_date].set_index("hour_ending")[col]
        dart_today = df_dart[df_dart["date"] == table_date].set_index("hour_ending")[col]

        date_str = pd.Timestamp(table_date).strftime("%m/%d/%Y")

        html += f'<div id="{div_id}" style="display:{display};overflow-x:auto;padding:8px 0;">\n'
        html += (
            f'<div style="font-size:11px;color:#6f8db1;padding:4px 8px;font-weight:600;">'
            f'{date_str} &mdash; {comp_label}</div>\n'
        )
        html += (
            '<table style="width:100%;border-collapse:collapse;font-size:11px;'
            "font-family:'IBM Plex Sans',monospace;\">\n"
        )

        # Header
        html += '<tr style="border-bottom:1px solid #2a3f60;">'
        html += _th("Market", align="left")
        for he in range(1, 25):
            html += _th(f"HE{he}")
        html += _th("OnPeak")
        html += _th("OffPeak")
        html += _th("Avg")
        html += "</tr>\n"

        # Rows
        for market, series in [("DA", da_today), ("RT", rt_today), ("DART", dart_today)]:
            is_dart = market == "DART"
            html += '<tr style="border-bottom:1px solid #1a2a42;">'
            html += (
                f'<td style="padding:4px 8px;font-weight:600;color:#dbe7ff;'
                f'white-space:nowrap;">{market}</td>'
            )

            vals = []
            for he in range(1, 25):
                val = series.get(he, None)
                if val is not None and not pd.isna(val):
                    color = _cell_color(val, is_dart)
                    html += (
                        f'<td style="padding:4px 6px;text-align:right;color:{color};">'
                        f"{val:.2f}</td>"
                    )
                    vals.append(val)
                else:
                    html += '<td style="padding:4px 6px;text-align:right;color:#3a4a60;">&mdash;</td>'

            onpeak = [series.get(h) for h in ONPEAK_HOURS
                      if h in series.index and not pd.isna(series.get(h))]
            offpeak = [series.get(h) for h in OFFPEAK_HOURS
                       if h in series.index and not pd.isna(series.get(h))]

            for agg in [onpeak, offpeak, vals]:
                if agg:
                    avg = sum(agg) / len(agg)
                    color = _cell_color(avg, is_dart)
                    html += (
                        f'<td style="padding:4px 8px;text-align:right;font-weight:600;'
                        f'color:{color};">{avg:.2f}</td>'
                    )
                else:
                    html += (
                        '<td style="padding:4px 8px;text-align:right;'
                        'color:#3a4a60;">&mdash;</td>'
                    )

            html += "</tr>\n"

        html += "</table>\n</div>\n"

    return html


def _th(text: str, align: str = "right") -> str:
    return (
        f'<th style="padding:4px 6px;text-align:{align};color:#f0b429;'
        f'font-weight:600;white-space:nowrap;">{text}</th>'
    )


def _cell_color(val: float, is_dart: bool) -> str:
    if is_dart:
        if val < 0:
            return "#ef4444"
        if val > 0:
            return "#34d399"
    return "#dbe7ff"


# ── Charts Row ───────────────────────────────────────────────────────


def _build_charts_row(
    df_da: pd.DataFrame,
    df_rt: pd.DataFrame,
    df_dart: pd.DataFrame,
    chart_ids: list[str],
) -> str:
    """Build 3 overlaid hourly profile charts side-by-side in a flex container."""
    datasets = [
        (df_da, "DA LMP", chart_ids[0]),
        (df_rt, "RT LMP", chart_ids[1]),
        (df_dart, "DART Spread", chart_ids[2]),
    ]

    html = '<div style="display:flex;gap:8px;width:100%;">\n'

    for df, title, chart_id in datasets:
        fig, trace_map = _build_profile_fig(df, title)
        fig_json = pio.to_json(fig)
        trace_map_json = json.dumps(trace_map)

        html += f"""
        <div style="flex:1;min-width:0;">
            <div id="{chart_id}" style="width:100%;"></div>
            <script>
            (function() {{
                var cid = '{chart_id}';
                var fig = {fig_json};
                var traceMap = {trace_map_json};
                Plotly.newPlot(cid, fig.data, fig.layout, {{responsive: true}});
                var STATE = window.{_PREFIX}State;
                if (STATE) {{ STATE.charts[cid] = traceMap; }}
            }})();
            </script>
        </div>
        """

    html += "</div>\n"
    return html


def _build_profile_fig(
    df: pd.DataFrame,
    title_prefix: str,
) -> tuple[go.Figure, dict]:
    """Build overlaid hourly profile with all 3 component trace groups.

    Returns (figure, trace_map) where trace_map is:
      {"Total LMP": [{"idx": 0, "vis": true}, ...], ...}
    """
    all_dates = sorted(df["date"].unique())
    last_3 = all_dates[-3:]
    other_dates = all_dates[:-3]

    last_3_colors = ["#EF553B", "#00CC96", "#FFA15A"]

    fig = go.Figure()
    trace_map: dict[str, list[dict]] = {}

    for comp_idx, (col, comp_label) in enumerate(COMPONENTS):
        is_default = comp_idx == 0
        comp_traces: list[dict] = []

        # ── 30d min/max envelope ──
        hourly_stats = df.groupby("hour_ending")[col].agg(["min", "max"])

        idx = len(fig.data)
        comp_traces.append({"idx": idx, "vis": True})
        fig.add_trace(go.Scatter(
            x=hourly_stats.index.tolist(),
            y=hourly_stats["max"].tolist(),
            mode="lines",
            name="30d Max",
            line=dict(color="rgba(99, 110, 250, 0.3)", width=0),
            showlegend=True,
            visible=True if is_default else False,
        ))

        idx = len(fig.data)
        comp_traces.append({"idx": idx, "vis": True})
        fig.add_trace(go.Scatter(
            x=hourly_stats.index.tolist(),
            y=hourly_stats["min"].tolist(),
            mode="lines",
            name="30d Min/Max Range",
            line=dict(color="rgba(99, 110, 250, 0.3)", width=0),
            fill="tonexty",
            fillcolor="rgba(99, 110, 250, 0.15)",
            showlegend=True,
            visible=True if is_default else False,
        ))

        # ── Other days — visible as background, no legend entries ──
        for dt in other_dates:
            day = df[df["date"] == dt].sort_values("hour_ending")
            idx = len(fig.data)
            comp_traces.append({"idx": idx, "vis": True})
            fig.add_trace(go.Scatter(
                x=day["hour_ending"].tolist(),
                y=day[col].tolist(),
                mode="lines",
                name="",
                line=dict(color="rgba(160, 174, 200, 0.25)", width=0.7),
                showlegend=False,
                hoverinfo="skip",
                visible=True if is_default else False,
            ))

        # ── Last 3 days — visible ──
        for i, dt in enumerate(last_3):
            day = df[df["date"] == dt].sort_values("hour_ending")
            day_label = str(dt.date()) if hasattr(dt, "date") else str(dt)
            idx = len(fig.data)
            comp_traces.append({"idx": idx, "vis": True})
            fig.add_trace(go.Scatter(
                x=day["hour_ending"].tolist(),
                y=day[col].tolist(),
                mode="lines+markers",
                name=day_label,
                line=dict(color=last_3_colors[i % len(last_3_colors)], width=2),
                marker=dict(size=4),
                showlegend=True,
                visible=True if is_default else False,
            ))

        trace_map[comp_label] = comp_traces

    fig.update_layout(
        title=dict(text=title_prefix, font=dict(size=13)),
        xaxis_title="Hour Ending",
        yaxis_title="Total LMP ($/MWh)",
        height=420,
        template=PLOTLY_TEMPLATE,
        legend=dict(
            font=dict(size=9),
            orientation="h",
            yanchor="top",
            y=-0.12,
            xanchor="left",
            x=0,
        ),
        margin=dict(l=50, r=20, t=40, b=60),
    )
    fig.update_xaxes(dtick=2, range=[0.5, 24.5])

    return fig, trace_map


# ── Global Controls ──────────────────────────────────────────────────


def _build_global_controls(chart_ids: list[str]) -> str:
    """Build page-level LMP component toggle buttons + JS state setup."""
    comp_btns = ""
    for i, (col, label) in enumerate(COMPONENTS):
        active = " fc-active" if i == 0 else ""
        comp_btns += (
            f'<button class="fc-btn fc-btn-comp-{_PREFIX}{active}" '
            f"onclick=\"{_PREFIX}Component(this,'{label}','{col}')\">"
            f"{label}</button>\n"
        )

    chart_ids_js = json.dumps(chart_ids)
    comp_labels_js = json.dumps([label for _, label in COMPONENTS])
    comp_cols_js = json.dumps([col for col, _ in COMPONENTS])

    return f"""
<div style="display:flex;align-items:center;gap:6px;padding:10px 12px;
            overflow-x:auto;flex-wrap:nowrap;">
  <span style="font-size:11px;font-weight:600;color:#6f8db1;
               white-space:nowrap;margin-right:4px;">
    LMP COMPONENT
  </span>
  {comp_btns}
</div>

<style>
  .fc-btn-comp-{_PREFIX} {{
    padding: 4px 12px; font-size: 11px; font-weight: 600;
    background: #101d31; color: #9eb4d3; border: 1px solid #2a3f60;
    border-radius: 16px; cursor: pointer; white-space: nowrap;
    font-family: inherit; transition: all 0.12s; flex-shrink: 0;
  }}
  .fc-btn-comp-{_PREFIX}:hover {{ background: #1a2b44; color: #dbe7ff; }}
  .fc-btn-comp-{_PREFIX}.fc-active {{
    background: #20314d; color: #fff; border-color: #4cc9f0;
  }}
</style>

<script>
(function() {{
  var chartIds = {chart_ids_js};
  var compLabels = {comp_labels_js};
  var compCols = {comp_cols_js};

  if (!window.{_PREFIX}State) {{
    window.{_PREFIX}State = {{
      activeComponent: compLabels[0],
      charts: {{}}
    }};
  }}
  var STATE = window.{_PREFIX}State;

  window.{_PREFIX}Component = function(btn, compLabel, compCol) {{
    STATE.activeComponent = compLabel;

    document.querySelectorAll('.fc-btn-comp-{_PREFIX}').forEach(function(b) {{
      b.classList.remove('fc-active');
    }});
    btn.classList.add('fc-active');

    /* Toggle table visibility */
    compCols.forEach(function(col, i) {{
      var el = document.getElementById('{_PREFIX}Table_' + col);
      if (el) el.style.display = (compLabels[i] === compLabel) ? '' : 'none';
    }});

    /* Broadcast to all charts */
    Object.keys(STATE.charts).forEach(function(chartId) {{
      var traceMap = STATE.charts[chartId];
      Object.keys(traceMap).forEach(function(comp) {{
        var traces = traceMap[comp];
        var isActive = (comp === compLabel);
        traces.forEach(function(t) {{
          Plotly.restyle(chartId, {{'visible': isActive ? t.vis : false}}, [t.idx]);
        }});
      }});
      Plotly.relayout(chartId, {{'yaxis.title.text': compLabel + ' ($/MWh)'}});
    }});
  }};
}})();
</script>
"""
