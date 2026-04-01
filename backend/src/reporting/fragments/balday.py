"""Balance of Day report — today's DA / RT / DART / Override for all LMP hubs.

Tables are grouped by market (DA, RT, DART, Override), each with rows for
LMP, Congestion, and Energy.  Override defaults unsettled hours to DA and is
editable.  DART = DA − Override.  Chart shows all four traces per component.

Each hub gets its own section in the report sidebar.
"""
import json
import logging
import re
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

from src.like_day_forecast import configs
from src.data import lmps_hourly
from src.utils.cache_utils import pull_with_cache

logger = logging.getLogger(__name__)

PLOTLY_TEMPLATE = "plotly_dark"
ONPEAK_HOURS = list(range(8, 24))       # HE 8–23
OFFPEAK_HOURS = list(range(1, 8)) + [24]  # HE 1–7, 24

COMPONENTS = [
    ("lmp_total", "LMP"),
    ("lmp_congestion_price", "Congestion"),
    ("lmp_system_energy_price", "Energy"),
]

Section = tuple[str, Any, str | None]


def _hub_slug(hub: str) -> str:
    """Convert hub name to a safe HTML id prefix (e.g. 'WESTERN HUB' -> 'western_hub')."""
    return re.sub(r"[^a-z0-9]+", "_", hub.lower()).strip("_")


# ── Public entry point ───────────────────────────────────────────────


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list[Section]:
    """Pull today's DA/RT LMP for all hubs, return Bal Day tables + chart per hub."""
    logger.info("Building Bal Day report...")

    today = date.today()
    today_str = today.isoformat()

    cache_kwargs = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    # ── Pull DA and RT LMP for all hubs ────────────────────────────
    logger.info("Pulling DA LMP hourly (all hubs)...")
    df_da = pull_with_cache(
        source_name="pjm_lmps_hourly_da",
        pull_fn=lmps_hourly.pull,
        pull_kwargs={"schema": schema, "market": "da"},
        **cache_kwargs,
    )

    logger.info("Pulling RT LMP hourly (all hubs)...")
    df_rt = pull_with_cache(
        source_name="pjm_lmps_hourly_rt",
        pull_fn=lmps_hourly.pull,
        pull_kwargs={"schema": schema, "market": "rt"},
        **cache_kwargs,
    )

    # Prep
    df_da["date"] = pd.to_datetime(df_da["date"]).dt.date
    df_rt["date"] = pd.to_datetime(df_rt["date"]).dt.date
    df_da["hour_ending"] = df_da["hour_ending"].replace(0, 24).astype(int)
    df_rt["hour_ending"] = df_rt["hour_ending"].replace(0, 24).astype(int)

    # Discover all hubs present in either DA or RT, sorted alphabetically
    all_hubs = sorted(set(df_da["hub"].unique()) | set(df_rt["hub"].unique()))
    logger.info(f"Found {len(all_hubs)} hubs: {all_hubs}")

    # ── Build one section per hub ──────────────────────────────────
    sections: list[Section] = []

    for hub in all_hubs:
        slug = _hub_slug(hub)
        prefix = f"balday_{slug}"
        chart_id = f"{prefix}_chart"

        da_hub = df_da[df_da["hub"] == hub].copy()
        rt_hub = df_rt[df_rt["hub"] == hub].copy()

        da_today = da_hub[da_hub["date"] == today].set_index("hour_ending")
        rt_today = rt_hub[rt_hub["date"] == today].set_index("hour_ending")

        settled_hours = sorted(rt_today["lmp_total"].dropna().index.tolist())

        # Build HTML for this hub
        html = ""
        html += _build_market_table("DA", da_today, rt_today, settled_hours, readonly=True, prefix=prefix)
        html += _build_market_table("RT", da_today, rt_today, settled_hours, readonly=True, prefix=prefix)
        html += _build_market_table("DART", da_today, rt_today, settled_hours, readonly=True, prefix=prefix)
        html += _build_market_table("Override", da_today, rt_today, settled_hours, readonly=False, prefix=prefix)
        html += _build_chart_html(da_today, rt_today, settled_hours, today_str, hub=hub, prefix=prefix, chart_id=chart_id)
        html += _build_js(da_today, rt_today, settled_hours, prefix=prefix, chart_id=chart_id)

        sections.append((f"{hub} — {today_str}", html, None))

    return sections


# ── Table builders ───────────────────────────────────────────────────


def _build_market_table(
    market: str,
    da_today: pd.DataFrame,
    rt_today: pd.DataFrame,
    settled_hours: list[int],
    readonly: bool,
    prefix: str,
) -> str:
    """One table for a market type, with a row per component."""
    tid = f"{prefix}_{market.lower()}"

    html = '<div style="overflow-x:auto;padding:8px 8px 12px 8px;">'

    if market == "Override":
        html += (
            f'<button id="{prefix}-reset" style="display:none;margin-bottom:8px;'
            f"padding:4px 12px;font-size:11px;font-family:monospace;"
            f"background:#1b3a5c;color:#e6efff;border:1px solid #2a3f60;"
            f'border-radius:4px;cursor:pointer;">Reset to DA</button>'
        )

    html += (
        f'<table id="{tid}" style="width:100%;border-collapse:collapse;'
        f"font-size:11px;font-family:'IBM Plex Sans',monospace;\">\n"
    )

    # Header
    html += '<tr style="border-bottom:1px solid #2a3f60;">'
    html += _th("Data", align="left")
    html += _th("Type", align="left")
    for he in range(1, 25):
        html += _th(f"HE{he}")
    html += _th("OnPeak Avg")
    html += _th("OffPeak Avg")
    html += _th("Avg")
    html += "</tr>\n"

    for col_key, type_label in COMPONENTS:
        da_series = da_today[col_key] if col_key in da_today.columns else pd.Series(dtype=float)
        rt_series = rt_today[col_key] if col_key in rt_today.columns else pd.Series(dtype=float)

        html += '<tr style="border-bottom:1px solid #1a2a42;">'
        html += _td_label(market, _market_color(market))
        html += _td_label(type_label)

        if market == "DA":
            for he in range(1, 25):
                v = da_series.get(he, None)
                html += _td_val(v) if _ok(v) else _td_blank()
            html += _td_avg([da_series.get(h) for h in ONPEAK_HOURS])
            html += _td_avg([da_series.get(h) for h in OFFPEAK_HOURS])
            html += _td_avg([da_series.get(h) for h in range(1, 25)])

        elif market == "RT":
            for he in range(1, 25):
                v = rt_series.get(he, None) if he in settled_hours else None
                html += _td_val(v) if _ok(v) else _td_blank()
            html += _td_avg([rt_series.get(h) for h in ONPEAK_HOURS if h in settled_hours])
            html += _td_avg([rt_series.get(h) for h in OFFPEAK_HOURS if h in settled_hours])
            html += _td_avg([rt_series.get(h) for h in range(1, 25) if h in settled_hours])

        elif market == "DART":
            # Cells are targets for JS — rendered blank, filled on recompute()
            row_id = f"{prefix}-dart-{col_key}"
            for he in range(1, 25):
                html += (
                    f'<td data-row="{row_id}" data-hour="{he}" '
                    f'style="padding:4px 6px;text-align:right;color:#3a4a60;">&mdash;</td>'
                )
            for sc in ["OnPeak", "OffPeak", "Avg"]:
                html += (
                    f'<td data-row="{row_id}" data-col="{sc}" '
                    f'style="padding:4px 8px;text-align:right;font-weight:600;'
                    f'color:#3a4a60;">&mdash;</td>'
                )

        elif market == "Override":
            row_id = f"{prefix}-ovr-{col_key}"
            for he in range(1, 25):
                rt_v = rt_series.get(he, None)
                da_v = da_series.get(he, None)
                is_settled = he in settled_hours and _ok(rt_v)

                if is_settled:
                    # Settled: show RT actual, read-only
                    html += (
                        f'<td data-row="{row_id}" data-hour="{he}" data-settled="1" '
                        f'style="padding:4px 6px;text-align:right;color:#dbe7ff;">'
                        f'{rt_v:.2f}</td>'
                    )
                elif _ok(da_v):
                    # Unsettled: editable, defaults to DA
                    html += (
                        f'<td data-row="{row_id}" data-hour="{he}" '
                        f'data-original="{da_v:.2f}" contenteditable="true" '
                        f'style="padding:4px 6px;text-align:right;color:#FFA15A;'
                        f'border-bottom:1px dashed #FFA15A;cursor:text;">'
                        f'{da_v:.2f}</td>'
                    )
                else:
                    html += (
                        f'<td data-row="{row_id}" data-hour="{he}" '
                        f'contenteditable="true" '
                        f'style="padding:4px 6px;text-align:right;color:#FFA15A;'
                        f'border-bottom:1px dashed #FFA15A;cursor:text;">&mdash;</td>'
                    )
            for sc in ["OnPeak", "OffPeak", "Avg"]:
                html += (
                    f'<td data-row="{row_id}" data-col="{sc}" '
                    f'style="padding:4px 8px;text-align:right;font-weight:600;'
                    f'color:#FFA15A;">&mdash;</td>'
                )

        html += "</tr>\n"

    html += "</table>\n</div>\n"
    return html


# ── Chart builder ────────────────────────────────────────────────────


def _build_chart_html(
    da_today: pd.DataFrame,
    rt_today: pd.DataFrame,
    settled_hours: list[int],
    today_str: str,
    hub: str,
    prefix: str,
    chart_id: str,
) -> str:
    """Three side-by-side subplots (LMP / Congestion / Energy) with DA, RT, Override, DART."""
    hours = list(range(1, 25))
    subplot_titles = [label for _, label in COMPONENTS]

    fig = make_subplots(
        rows=1, cols=3,
        subplot_titles=subplot_titles,
        shared_yaxes=False,
        horizontal_spacing=0.05,
    )

    # Track Override trace indices so JS can restyle them
    override_trace_indices: dict[str, int] = {}

    for ci, (col, label) in enumerate(COMPONENTS):
        col_idx = ci + 1
        show_legend = ci == 0

        da_vals = [da_today[col].get(h, None) if col in da_today.columns else None for h in hours]
        rt_vals = [
            rt_today[col].get(h, None) if (col in rt_today.columns and h in settled_hours) else None
            for h in hours
        ]
        # Override initial = RT for settled, DA for unsettled
        ovr_vals = []
        for i, h in enumerate(hours):
            if h in settled_hours and rt_vals[i] is not None and not pd.isna(rt_vals[i]):
                ovr_vals.append(rt_vals[i])
            else:
                ovr_vals.append(da_vals[i])

        dart_vals = [
            (da_vals[i] - ovr_vals[i])
            if (_ok(da_vals[i]) and _ok(ovr_vals[i]))
            else None
            for i in range(24)
        ]

        # DA
        fig.add_trace(go.Scatter(
            x=hours, y=da_vals,
            mode="lines+markers", name="DA",
            line=dict(color="#FFA15A", width=2.5), marker=dict(size=4),
            showlegend=show_legend, legendgroup="DA",
            hovertemplate=f"HE %{{x}}<br>DA {label}: $%{{y:.2f}}<extra></extra>",
        ), row=1, col=col_idx)

        # RT
        fig.add_trace(go.Scatter(
            x=hours, y=rt_vals,
            mode="lines+markers", name="RT",
            line=dict(color="#4cc9f0", width=2.5), marker=dict(size=4),
            connectgaps=False,
            showlegend=show_legend, legendgroup="RT",
            hovertemplate=f"HE %{{x}}<br>RT {label}: $%{{y:.2f}}<extra></extra>",
        ), row=1, col=col_idx)

        # Override (dashed green — updated via JS)
        override_trace_indices[col] = len(fig.data)
        fig.add_trace(go.Scatter(
            x=hours, y=ovr_vals,
            mode="lines+markers", name="Override",
            line=dict(color="#00CC96", width=2, dash="dash"), marker=dict(size=3),
            showlegend=show_legend, legendgroup="Override",
            hovertemplate=f"HE %{{x}}<br>Override {label}: $%{{y:.2f}}<extra></extra>",
        ), row=1, col=col_idx)

        # DART bars
        dart_colors = [
            "#34d399" if (_ok(v) and v >= 0) else "#ef4444" for v in dart_vals
        ]
        fig.add_trace(go.Bar(
            x=hours, y=dart_vals,
            name="DART",
            marker_color=dart_colors, opacity=0.45,
            showlegend=show_legend, legendgroup="DART",
            hovertemplate=f"HE %{{x}}<br>DART {label}: $%{{y:.2f}}<extra></extra>",
        ), row=1, col=col_idx)

        fig.update_xaxes(dtick=2, range=[0.5, 24.5], title_text="Hour Ending", row=1, col=col_idx)

    fig.update_yaxes(title_text="$/MWh", row=1, col=1)
    fig.update_layout(
        title=dict(text=f"DA vs RT vs Override — {hub} — {today_str}", font=dict(size=13)),
        height=420,
        template=PLOTLY_TEMPLATE,
        legend=dict(font=dict(size=10), orientation="h", yanchor="top", y=-0.15, xanchor="left", x=0),
        margin=dict(l=50, r=20, t=50, b=70),
        barmode="overlay",
    )

    chart_html = fig.to_html(include_plotlyjs="cdn", full_html=False, div_id=chart_id)

    # Embed the override trace index map so JS can restyle
    chart_html += (
        f'\n<script>window.{prefix}_ovrIdx = {json.dumps(override_trace_indices)};</script>'
    )

    return chart_html


# ── JS ───────────────────────────────────────────────────────────────


def _build_js(
    da_today: pd.DataFrame,
    rt_today: pd.DataFrame,
    settled_hours: list[int],
    prefix: str,
    chart_id: str,
) -> str:
    """Single script block that wires Override edits → DART table + chart."""

    # Emit per-component DA values as JS objects
    da_blocks = ""
    for col_key, _ in COMPONENTS:
        series = da_today[col_key] if col_key in da_today.columns else pd.Series(dtype=float)
        da_blocks += f"  da['{col_key}'] = {{}};\n"
        for h in range(1, 25):
            v = series.get(h, None)
            if _ok(v):
                da_blocks += f"  da['{col_key}'][{h}] = {v:.4f};\n"

    settled_js = "{" + ",".join(f"{h}:1" for h in settled_hours) + "}"
    comp_keys_js = json.dumps([k for k, _ in COMPONENTS])

    return f"""
<script>
(function() {{
  var ONPEAK  = [8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23];
  var OFFPEAK = [1,2,3,4,5,6,7,24];
  var COMP_KEYS = {comp_keys_js};
  var settled = {settled_js};
  var da = {{}};
{da_blocks}
  var ovrIdx = window.{prefix}_ovrIdx || {{}};
  var chartEl = document.getElementById('{chart_id}');
  var resetBtn = document.getElementById('{prefix}-reset');

  function q(sel, hour) {{
    return document.querySelector('td[data-row="'+sel+'"][data-hour="'+hour+'"]');
  }}
  function qs(sel, col) {{
    return document.querySelector('td[data-row="'+sel+'"][data-col="'+col+'"]');
  }}
  function val(cell) {{
    if (!cell) return NaN;
    var t = cell.textContent.trim();
    return (t === '' || t === '\\u2014' || t === '&mdash;') ? NaN : parseFloat(t);
  }}
  function mean(arr) {{
    var s = 0, n = 0;
    for (var i = 0; i < arr.length; i++) if (!isNaN(arr[i])) {{ s += arr[i]; n++; }}
    return n ? s / n : NaN;
  }}
  function fmt2(v) {{ return isNaN(v) ? '\\u2014' : v.toFixed(2); }}
  function dartColor(v) {{
    if (isNaN(v)) return '#3a4a60';
    return v < 0 ? '#ef4444' : (v > 0 ? '#34d399' : '#dbe7ff');
  }}

  function recompute() {{
    var anyEdited = false;

    for (var ci = 0; ci < COMP_KEYS.length; ci++) {{
      var ck = COMP_KEYS[ci];
      var ovrRow = '{prefix}-ovr-' + ck;
      var dartRow = '{prefix}-dart-' + ck;
      var daC = da[ck] || {{}};

      var ovrOn = [], ovrOff = [], ovrAll = [];
      var dartOn = [], dartOff = [], dartAll = [];
      var ovrPlotY = [];

      for (var h = 1; h <= 24; h++) {{
        var ovrCell = q(ovrRow, h);
        var dartCell = q(dartRow, h);
        var ov = val(ovrCell);

        if (!isNaN(ov)) {{
          if (ONPEAK.indexOf(h) >= 0) ovrOn.push(ov);
          if (OFFPEAK.indexOf(h) >= 0) ovrOff.push(ov);
          ovrAll.push(ov);
        }}
        ovrPlotY.push(isNaN(ov) ? null : ov);

        // DART = DA - Override
        if (daC[h] !== undefined && !isNaN(ov)) {{
          var d = daC[h] - ov;
          dartCell.textContent = fmt2(d);
          dartCell.style.color = dartColor(d);
          if (ONPEAK.indexOf(h) >= 0) dartOn.push(d);
          if (OFFPEAK.indexOf(h) >= 0) dartOff.push(d);
          dartAll.push(d);
        }} else if (dartCell) {{
          dartCell.textContent = '\\u2014';
          dartCell.style.color = '#3a4a60';
        }}

        // Track edits
        if (ovrCell && !ovrCell.hasAttribute('data-settled') && ovrCell.hasAttribute('data-original')) {{
          var orig = parseFloat(ovrCell.getAttribute('data-original'));
          var cur = val(ovrCell);
          if (!isNaN(orig) && !isNaN(cur) && Math.abs(cur - orig) > 0.001) {{
            anyEdited = true;
            ovrCell.style.background = 'rgba(255,161,90,0.15)';
          }} else {{
            ovrCell.style.background = '';
          }}
        }}
      }}

      // Override summary cells
      var ovrSums = [
        [qs(ovrRow, 'OnPeak'), mean(ovrOn)],
        [qs(ovrRow, 'OffPeak'), mean(ovrOff)],
        [qs(ovrRow, 'Avg'), mean(ovrAll)]
      ];
      for (var i = 0; i < ovrSums.length; i++) {{
        var c = ovrSums[i][0], v = ovrSums[i][1];
        if (c) {{ c.textContent = fmt2(v); c.style.color = '#FFA15A'; c.style.fontWeight = '600'; }}
      }}

      // DART summary cells
      var dartSums = [
        [qs(dartRow, 'OnPeak'), mean(dartOn)],
        [qs(dartRow, 'OffPeak'), mean(dartOff)],
        [qs(dartRow, 'Avg'), mean(dartAll)]
      ];
      for (var i = 0; i < dartSums.length; i++) {{
        var c = dartSums[i][0], v = dartSums[i][1];
        if (c) {{ c.textContent = fmt2(v); c.style.color = dartColor(v); c.style.fontWeight = '600'; }}
      }}

      // Update Override trace on chart
      if (chartEl && chartEl.data && ovrIdx[ck] !== undefined) {{
        Plotly.restyle('{chart_id}', {{y: [ovrPlotY]}}, [ovrIdx[ck]]);
      }}
    }}

    if (resetBtn) resetBtn.style.display = anyEdited ? 'inline-block' : 'none';
  }}

  // Listen for edits on any Override table cell
  var ovrTbl = document.getElementById('{prefix}_override');
  if (ovrTbl) {{
    ovrTbl.addEventListener('input', function(e) {{
      var row = e.target.getAttribute('data-row');
      if (row && row.indexOf('{prefix}-ovr-') === 0) recompute();
    }});
  }}

  // Reset button
  if (resetBtn) {{
    resetBtn.addEventListener('click', function() {{
      var cells = ovrTbl ? ovrTbl.querySelectorAll('td[contenteditable="true"]') : [];
      for (var i = 0; i < cells.length; i++) {{
        if (cells[i].hasAttribute('data-original'))
          cells[i].textContent = cells[i].getAttribute('data-original');
      }}
      recompute();
    }});
  }}

  recompute();
}})();
</script>"""


# ── Helpers ──────────────────────────────────────────────────────────


def _ok(v) -> bool:
    return v is not None and not pd.isna(v)


def _market_color(market: str) -> str:
    return {
        "DA": "#dbe7ff",
        "RT": "#4cc9f0",
        "DART": "#34d399",
        "Override": "#FFA15A",
    }.get(market, "#dbe7ff")


def _th(text: str, align: str = "right") -> str:
    return (
        f'<th style="padding:4px 6px;text-align:{align};color:#f0b429;'
        f'font-weight:600;white-space:nowrap;">{text}</th>'
    )


def _td_label(text: str, color: str = "#dbe7ff") -> str:
    return (
        f'<td style="padding:4px 8px;font-weight:600;color:{color};'
        f'white-space:nowrap;">{text}</td>'
    )


def _td_val(val: float) -> str:
    return (
        f'<td style="padding:4px 6px;text-align:right;color:#dbe7ff;">'
        f'{val:.2f}</td>'
    )


def _td_blank() -> str:
    return '<td style="padding:4px 6px;text-align:right;color:#3a4a60;">&mdash;</td>'


def _td_avg(vals: list) -> str:
    clean = [v for v in vals if _ok(v)]
    if clean:
        avg = sum(clean) / len(clean)
        return (
            f'<td style="padding:4px 8px;text-align:right;font-weight:600;'
            f'color:#dbe7ff;">{avg:.2f}</td>'
        )
    return (
        '<td style="padding:4px 8px;text-align:right;'
        'color:#3a4a60;">&mdash;</td>'
    )
