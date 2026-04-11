"""Forecast results report — tomorrow's DA LMP prediction with actuals comparison.

Sections:
  1. Analog Days    — Table of analog days with rank, distance, similarity, weight
  2. Quantile Bands  — Pivoted band table with editable Override + Ovr-Fcst diff
  3. Quantile Band Chart — Plotly chart with bands, median, forecast, override, actuals
"""
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go

from src.like_day_forecast import configs
from src.like_day_forecast.pipelines.forecast import run as run_forecast
from src.views.like_day_forecast_results import build_view_model

logger = logging.getLogger(__name__)

PLOTLY_TEMPLATE = "plotly_dark"
HE_COLS = [f"HE{h}" for h in range(1, 25)]
SUMMARY_COLS = ["OnPeak", "OffPeak", "Flat"]
Section = tuple[str, Any, str | None]


# ── Public entry point ───────────────────────────────────────────────


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list[Section]:
    """Run tomorrow's forecast and return report sections."""
    logger.info("Building forecast results report...")

    result = run_forecast(
        forecast_date=None,
        config=configs.ScenarioConfig(schema=schema),
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        cache_ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    if "error" in result:
        return [("Forecast Error", _error_html(result["error"]), None)]

    # Build structured view model (domain interpretation layer)
    vm = build_view_model(result)
    logger.info(f"View model: {vm['forecast_date']}, "
                f"actuals={'yes' if vm['has_actuals'] else 'no'}, "
                f"analogs={vm['n_analogs_used']}")

    output_table = result["output_table"]
    quantiles_table = result["quantiles_table"]
    df_forecast = result["df_forecast"]
    has_actuals = result["has_actuals"]
    forecast_date = result["forecast_date"]
    reference_date = result["reference_date"]
    analogs_df = result.get("analogs")

    sections: list[Section] = []

    # 1. Analog days table
    if analogs_df is not None and len(analogs_df) > 0:
        sections.append((
            f"Analog Days — {forecast_date}",
            _analog_days_table_html(analogs_df, forecast_date, reference_date),
            None,
        ))

    # 2. Quantile bands table (with editable Override + Ovr-Fcst diff)
    sections.append((
        f"Quantile Bands — {forecast_date}",
        _quartile_bands_table_html(quantiles_table, output_table, has_actuals),
        None,
    ))

    # 3. Quantile bands plot (with Override trace)
    sections.append((
        "Quantile Band Chart",
        _quartile_bands_plot_html(df_forecast, output_table, has_actuals),
        None,
    ))

    return sections


# ── Section 1: Analog Days Table ──────────────────────────────────────


def _analog_days_table_html(
    analogs_df: pd.DataFrame,
    forecast_date: str,
    reference_date: str,
) -> str:
    """Table of analog days with rank, date, distance, similarity, and weight."""
    n_total = len(analogs_df)
    top5_weight = analogs_df.head(5)["weight"].sum()
    dist_min = analogs_df["distance"].min()
    dist_max = analogs_df["distance"].max()

    # Summary cards
    html = '<div style="padding:12px 8px 4px 8px;">'
    html += (
        '<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:12px;">'
    )

    card = (
        "background:#111d31;border:1px solid #253b59;border-radius:10px;"
        "padding:12px 16px;min-width:140px;flex:1;"
    )
    label_s = (
        "font-size:10px;font-weight:600;color:#6f8db1;"
        "text-transform:uppercase;letter-spacing:0.5px;"
    )
    val_s = (
        "font-size:18px;font-weight:700;color:#dbe7ff;"
        "font-family:'Space Grotesk',monospace;margin-top:2px;"
    )

    cards = [
        ("Forecast Date", str(forecast_date)),
        ("Reference Date", str(reference_date)),
        ("Analogs Used", str(n_total)),
        ("Top-5 Weight", f"{top5_weight:.1%}"),
        ("Distance Range", f"{dist_min:.4f} — {dist_max:.4f}"),
    ]
    for lbl, val in cards:
        html += (
            f'<div style="{card}">'
            f'<div style="{label_s}">{lbl}</div>'
            f'<div style="{val_s}">{val}</div>'
            f'</div>'
        )
    html += '</div>'  # close cards row

    # Table
    columns = ["Rank", "Date", "Distance", "Similarity", "Weight"]
    html += (
        '<table style="width:100%;border-collapse:collapse;'
        'font-size:12px;font-family:monospace;">'
    )

    # Header
    html += "<thead><tr>"
    for col in columns:
        align = "left" if col in ("Date",) else "right"
        html += (
            f'<th style="padding:6px 10px;background:#16263d;color:#e6efff;'
            f'text-align:{align};font-size:11px;position:sticky;top:0;">{col}</th>'
        )
    html += "</tr></thead><tbody>"

    # Rows
    for i, (_, row) in enumerate(analogs_df.iterrows()):
        bg = "rgba(22, 38, 61, 0.5)" if i % 2 == 0 else "transparent"
        rank = int(row["rank"])
        dt = str(row["date"])
        dist = row["distance"]
        sim = row["similarity"]
        wt = row["weight"]

        # Weight bar — visual indicator proportional to max weight
        max_weight = analogs_df["weight"].max()
        bar_pct = (wt / max_weight * 100) if max_weight > 0 else 0

        html += f'<tr style="background:{bg};">'
        html += f'<td style="padding:5px 10px;border-bottom:1px solid #1e3350;text-align:right;color:#a6bad6;">{rank}</td>'
        html += f'<td style="padding:5px 10px;border-bottom:1px solid #1e3350;text-align:left;color:#dbe7ff;font-weight:600;">{dt}</td>'
        html += f'<td style="padding:5px 10px;border-bottom:1px solid #1e3350;text-align:right;color:#dbe7ff;">{dist:.4f}</td>'
        html += f'<td style="padding:5px 10px;border-bottom:1px solid #1e3350;text-align:right;color:#dbe7ff;">{sim:.2%}</td>'
        html += (
            f'<td style="padding:5px 10px;border-bottom:1px solid #1e3350;text-align:right;">'
            f'<div style="display:flex;align-items:center;justify-content:flex-end;gap:8px;">'
            f'<div style="width:60px;height:6px;background:#1a2d48;border-radius:3px;overflow:hidden;">'
            f'<div style="width:{bar_pct:.0f}%;height:100%;background:#4cc9f0;border-radius:3px;"></div>'
            f'</div>'
            f'<span style="color:#dbe7ff;">{wt:.4f}</span>'
            f'</div></td>'
        )
        html += "</tr>"

    html += "</tbody></table></div>"
    return html


# ── Section 2: Quartile Bands Table ───────────────────────────────────


def _quartile_bands_table_html(
    quantiles_table: pd.DataFrame,
    output_table: pd.DataFrame,
    has_actuals: bool,
) -> str:
    """Pivoted quantile bands table: Date | Band | HE1-24 | OnPk | OffPk | Flat.

    Matches the same layout as the forecast table — one row per quantile band
    (P01, P05, P10, P25, P50, P75, P90, P95, P99), plus Forecast/Actual rows.
    """
    cols = ["Date", "Band"] + HE_COLS + SUMMARY_COLS

    # Combine quantiles + forecast + actual into one display table
    display_rows = []
    for _, row in quantiles_table.iterrows():
        r = {"Date": row["Date"], "Band": row["Type"]}
        for c in HE_COLS + SUMMARY_COLS:
            r[c] = row[c]
        display_rows.append(r)

    forecast_src = output_table[output_table["Type"] == "Forecast"].iloc[0]
    r = {"Date": forecast_src["Date"], "Band": "Forecast"}
    for c in HE_COLS + SUMMARY_COLS:
        r[c] = forecast_src[c]
    display_rows.append(r)

    # Override row (reads live values from the Override table via JS)
    r = {"Date": forecast_src["Date"], "Band": "Override"}
    for c in HE_COLS + SUMMARY_COLS:
        r[c] = forecast_src[c]  # defaults to Forecast; JS will sync
    display_rows.append(r)

    if has_actuals:
        actual_src = output_table[output_table["Type"] == "Actual"].iloc[0]
        r = {"Date": actual_src["Date"], "Band": "Actual"}
        for c in HE_COLS + SUMMARY_COLS:
            r[c] = actual_src[c]
        display_rows.append(r)

    # Row type styling
    band_row_colors = {
        "Forecast": "#FFA15A",
        "Override": "#00CC96",
        "Actual": "#4cc9f0",
    }

    ncols = len(cols)
    tid = "qb"  # table id for JS targeting

    html = '<div style="overflow-x:auto;padding:8px;">'

    # Reset button (hidden until an edit)
    html += (
        f'<button id="{tid}-reset" style="display:none;margin-bottom:8px;'
        f"padding:4px 12px;font-size:11px;font-family:monospace;"
        f"background:#1b3a5c;color:#e6efff;border:1px solid #2a3f60;"
        f'border-radius:4px;cursor:pointer;">Reset Overrides</button>'
    )

    html += (
        f'<table id="{tid}" style="width:100%;border-collapse:collapse;'
        f'font-size:12px;font-family:monospace;">'
    )

    # Header
    html += "<thead><tr>"
    for col in cols:
        html += (
            f'<th style="padding:6px 8px;background:#16263d;color:#e6efff;'
            f'text-align:right;font-size:11px;position:sticky;top:0;'
            f'{"text-align:left;" if col in ("Date", "Band") else ""}">{col}</th>'
        )
    html += "</tr></thead><tbody>"

    # Store forecast values for difference computation
    forecast_vals: dict[str, float] = {}
    for c in HE_COLS + SUMMARY_COLS:
        forecast_vals[c] = forecast_src[c]

    # Row background highlights for key rows
    band_row_bg = {
        "Forecast": "rgba(255, 161, 90, 0.08)",
        "Override": "rgba(0, 204, 150, 0.08)",
        "Actual":   "rgba(76, 201, 240, 0.08)",
    }

    # Alternating band for quantile rows
    quantile_even_bg = "rgba(22, 38, 61, 0.5)"
    quantile_odd_bg = "transparent"

    quantile_idx = 0
    for row in display_rows:
        band = row["Band"]
        label_color = band_row_colors.get(band, "#a6bad6")
        is_special = band in band_row_colors

        # Build tr style
        tr_parts: list[str] = []
        if band == "Forecast":
            # Thick top border to separate from quantile bands
            tr_parts.append("border-top:3px solid #3a5070")
        if band in band_row_bg:
            tr_parts.append(f"background:{band_row_bg[band]}")
        elif band not in band_row_colors:
            # Alternating quantile row shading
            tr_parts.append(f"background:{quantile_even_bg if quantile_idx % 2 == 0 else quantile_odd_bg}")
            quantile_idx += 1
        tr_style = ";".join(tr_parts) + ";" if tr_parts else ""
        html += f'<tr style="{tr_style}">'

        for col in cols:
            val = row[col]
            style = "padding:5px 8px;border-bottom:1px solid #1e3350;text-align:right;"

            attrs = ""
            is_hourly = col.startswith("HE") and col[2:].isdigit()
            is_editable = band == "Override" and is_hourly

            if band == "Override":
                if is_hourly:
                    h = int(col[2:])
                    attrs = f' data-row="qb-override" data-hour="{h}"'
                    if pd.notna(val):
                        attrs += f' data-original="{val:.1f}" contenteditable="true"'
                elif col in SUMMARY_COLS:
                    attrs = f' data-row="qb-override" data-col="{col}"'

            if is_editable:
                style += "border-bottom-style:dashed;border-bottom-color:#00CC96;cursor:text;"

            if col in ("Date", "Band"):
                style += f"text-align:left;font-weight:600;color:{label_color};"
                html += f'<td style="{style}">{val}</td>'
            elif pd.notna(val):
                cell_color = label_color if is_special else "#dbe7ff"
                html += f'<td{attrs} style="{style}color:{cell_color};">{val:.1f}</td>'
            else:
                html += f'<td{attrs} style="{style}color:#556;">—</td>'

        html += "</tr>"

    # ── Thick divider before diff rows ──────────────────────────────
    html += (
        f'<tr><td colspan="{ncols}" style="padding:0;height:3px;'
        f'background:linear-gradient(90deg,#4a6a8a 0%,#4a6a8a60 100%);'
        f'border:none;"></td></tr>'
    )

    # ── Helper: render a single diff row ─────────────────────────────
    diff_bg = "rgba(14, 25, 42, 0.6)"

    def _diff_row(label: str, data_key: str, vals: dict[str, float | None]) -> str:
        row_html = f'<tr style="background:{diff_bg};">'
        for col in cols:
            style = "padding:4px 8px;border-bottom:1px solid #1a2d48;text-align:right;font-size:11px;"
            if col == "Date":
                style += "text-align:left;font-weight:600;color:#7a92b0;"
                row_html += f'<td style="{style}">{forecast_src["Date"]}</td>'
            elif col == "Band":
                style += "text-align:left;font-weight:600;color:#7a92b0;font-style:italic;"
                row_html += f'<td style="{style}">{label}</td>'
            else:
                attrs = ""
                if col.startswith("HE") and col[2:].isdigit():
                    attrs = f' data-row="{data_key}" data-hour="{int(col[2:])}"'
                elif col in SUMMARY_COLS:
                    attrs = f' data-row="{data_key}" data-col="{col}"'
                v = vals.get(col)
                if v is not None:
                    fmt_v = f"{v:+.1f}"
                    row_html += f'<td{attrs} style="{style}color:#8ea8c4;">{fmt_v}</td>'
                else:
                    row_html += f'<td{attrs} style="{style}color:#556;">\u2014</td>'
        row_html += "</tr>"
        return row_html

    divider = (
        f'<tr><td colspan="{ncols}" style="padding:0;height:3px;'
        f'background:linear-gradient(90deg,#4a6a8a 0%,#4a6a8a60 100%);'
        f'border:none;"></td></tr>'
    )

    # ── Fcst − Ovr (own section) ──────────────────────────────────
    fcst_ovr_vals: dict[str, float | None] = {c: 0.0 for c in HE_COLS + SUMMARY_COLS}
    html += _diff_row("Fcst\u2013Ovr", "qb-diff-fo", fcst_ovr_vals)

    # ── Actuals diffs (Fcst−Act, Ovr−Act) ─────────────────────────
    if has_actuals:
        actual_src = output_table[output_table["Type"] == "Actual"].iloc[0]
        fa_vals: dict[str, float | None] = {}
        for c in HE_COLS + SUMMARY_COLS:
            fv = forecast_vals.get(c)
            av = actual_src[c]
            fa_vals[c] = (fv - av) if (pd.notna(fv) and pd.notna(av)) else None

        html += divider
        html += _diff_row("Fcst\u2013Act", "qb-diff-fa", fa_vals)
        html += _diff_row("Ovr\u2013Act", "qb-diff-oa", dict(fa_vals))

    html += "</tbody></table></div>"

    # ── Build JS static values for actuals ──
    actual_js_lines = ""
    if has_actuals:
        actual_src = output_table[output_table["Type"] == "Actual"].iloc[0]
        actual_js_lines = "  var act = {};\n" + "".join(
            f"  act[{int(c[2:])}] = {actual_src[c]:.4f};\n"
            for c in HE_COLS if pd.notna(actual_src[c])
        )
    else:
        actual_js_lines = "  var act = null;\n"

    # ── JS: Override editing, diff rows, reset button, plot update ──
    html += """
<script>
(function() {
  var qbT = document.getElementById('qb');
  var resetBtn = document.getElementById('qb-reset');
  if (!qbT) return;

  var ONPEAK  = [8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23];
  var OFFPEAK = [1,2,3,4,5,6,7,24];

  function qbQ(row, hour) {
    return qbT.querySelector('td[data-row="'+row+'"][data-hour="'+hour+'"]');
  }
  function qbS(row, col) {
    return qbT.querySelector('td[data-row="'+row+'"][data-col="'+col+'"]');
  }
  function val(cell) {
    if (!cell) return NaN;
    var t = cell.textContent.trim();
    return (t === '' || t === '\u2014') ? NaN : parseFloat(t);
  }
  function mean(arr) {
    var s = 0, n = 0;
    for (var i = 0; i < arr.length; i++) {
      if (!isNaN(arr[i])) { s += arr[i]; n++; }
    }
    return n ? s / n : NaN;
  }
  function fmt(v) { return isNaN(v) ? '\u2014' : v.toFixed(1); }
  function fmtDiff(v) { return isNaN(v) ? '\u2014' : (v >= 0 ? '+' : '') + v.toFixed(1); }
  function errBg(v) {
    var a = Math.abs(v);
    if (a < 0.05) return '';
    if (a < 5)  return 'rgba(0,204,150,0.20)';
    if (a < 15) return 'rgba(255,161,90,0.25)';
    return 'rgba(239,85,59,0.30)';
  }

  // Static forecast values
  var fcst = {};
  """ + "".join(
        f"  fcst[{int(c[2:])}] = {forecast_vals[c]:.4f};\n"
        for c in HE_COLS if pd.notna(forecast_vals[c])
    ) + """

  // Static actual values (null if no actuals)
  """ + actual_js_lines + """

  function updateDiffRow(key, computeHourly) {
    var on = [], off = [], all = [];
    for (var h = 1; h <= 24; h++) {
      var d = computeHourly(h);
      var cell = qbQ(key, h);
      if (cell) {
        cell.textContent = fmtDiff(d);
        cell.style.background = isNaN(d) ? '' : errBg(d);
        cell.style.color = isNaN(d) ? '#556' : '#dbe7ff';
      }
      if (ONPEAK.indexOf(h) >= 0) on.push(d);
      if (OFFPEAK.indexOf(h) >= 0) off.push(d);
      all.push(d);
    }
    var cells = [
      [qbS(key,'OnPeak'), mean(on)],
      [qbS(key,'OffPeak'), mean(off)],
      [qbS(key,'Flat'), mean(all)]
    ];
    for (var i = 0; i < cells.length; i++) {
      var c = cells[i][0], v = cells[i][1];
      if (c) {
        c.textContent = fmtDiff(v);
        c.style.background = isNaN(v) ? '' : errBg(v);
        c.style.color = isNaN(v) ? '#556' : '#dbe7ff';
      }
    }
  }

  function recompute() {
    var ov = {};
    for (var h = 1; h <= 24; h++) ov[h] = val(qbQ('qb-override', h));

    // Summary cells for Override
    var onArr  = ONPEAK.map(function(h){ return ov[h]; });
    var offArr = OFFPEAK.map(function(h){ return ov[h]; });
    var allArr = []; for (var h = 1; h <= 24; h++) allArr.push(ov[h]);
    var cOn  = qbS('qb-override','OnPeak');
    var cOff = qbS('qb-override','OffPeak');
    var cFl  = qbS('qb-override','Flat');
    if (cOn)  cOn.textContent  = fmt(mean(onArr));
    if (cOff) cOff.textContent = fmt(mean(offArr));
    if (cFl)  cFl.textContent  = fmt(mean(allArr));

    // Fcst − Ovr
    updateDiffRow('qb-diff-fo', function(h) {
      return (!isNaN(ov[h]) && fcst[h] !== undefined) ? fcst[h] - ov[h] : NaN;
    });

    // Fcst − Actual and Ovr − Actual (only if actuals exist)
    if (act) {
      updateDiffRow('qb-diff-fa', function(h) {
        return (fcst[h] !== undefined && act[h] !== undefined) ? fcst[h] - act[h] : NaN;
      });
      updateDiffRow('qb-diff-oa', function(h) {
        return (!isNaN(ov[h]) && act[h] !== undefined) ? ov[h] - act[h] : NaN;
      });
    }

    // Highlight edited cells, show/hide reset
    var anyEdited = false;
    for (var h = 1; h <= 24; h++) {
      var cell = qbQ('qb-override', h);
      if (!cell || !cell.hasAttribute('data-original')) continue;
      var orig = parseFloat(cell.getAttribute('data-original'));
      if (!isNaN(orig) && !isNaN(ov[h]) && Math.abs(ov[h] - orig) > 0.001) {
        anyEdited = true;
        cell.style.background = 'rgba(0,204,150,0.15)';
      } else {
        cell.style.background = '';
      }
    }
    resetBtn.style.display = anyEdited ? 'inline-block' : 'none';

    // Update Override trace on the QB plot
    var qbPlot = document.getElementById('qb-plot');
    if (qbPlot && qbPlot.data) {
      var idx = -1;
      for (var i = 0; i < qbPlot.data.length; i++) {
        if (qbPlot.data[i].name === 'Override') { idx = i; break; }
      }
      if (idx >= 0) {
        var newY = [];
        for (var h = 1; h <= 24; h++) newY.push(isNaN(ov[h]) ? null : ov[h]);
        Plotly.restyle('qb-plot', {y: [newY]}, [idx]);
      }
    }
  }

  // Listen for edits in Override row
  qbT.addEventListener('input', function(e) {
    if (e.target.getAttribute('data-row') === 'qb-override') {
      recompute();
    }
  });

  // Reset button
  resetBtn.addEventListener('click', function() {
    for (var h = 1; h <= 24; h++) {
      var cell = qbQ('qb-override', h);
      if (cell && cell.hasAttribute('data-original'))
        cell.textContent = cell.getAttribute('data-original');
    }
    recompute();
  });
})();
</script>"""

    return html


# ── Section 2: Quartile Bands Plot ───────────────────────────────────


def _quartile_bands_plot_html(
    df_forecast: pd.DataFrame,
    output_table: pd.DataFrame,
    has_actuals: bool,
) -> str:
    """Plotly chart with quantile bands, median, forecast, actual, and outlier markers."""
    hours = df_forecast["hour_ending"].values
    fig = go.Figure()

    # P10-P90 outer band
    if "q_0.10" in df_forecast.columns and "q_0.90" in df_forecast.columns:
        fig.add_trace(go.Scatter(
            x=hours, y=df_forecast["q_0.90"],
            mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip",
        ))
        fig.add_trace(go.Scatter(
            x=hours, y=df_forecast["q_0.10"],
            mode="lines", line=dict(width=0),
            fill="tonexty", fillcolor="rgba(99, 110, 250, 0.10)",
            name="P10\u2013P90", hoverinfo="skip",
        ))

    # P25-P75 inner band
    if "q_0.25" in df_forecast.columns and "q_0.75" in df_forecast.columns:
        fig.add_trace(go.Scatter(
            x=hours, y=df_forecast["q_0.75"],
            mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip",
        ))
        fig.add_trace(go.Scatter(
            x=hours, y=df_forecast["q_0.25"],
            mode="lines", line=dict(width=0),
            fill="tonexty", fillcolor="rgba(99, 110, 250, 0.25)",
            name="P25\u2013P75", hoverinfo="skip",
        ))

    # P50 median line
    if "q_0.50" in df_forecast.columns:
        fig.add_trace(go.Scatter(
            x=hours, y=df_forecast["q_0.50"],
            mode="lines", name="P50 (Median)",
            line=dict(color="#AB63FA", width=2, dash="dash"),
            hovertemplate="HE %{x}<br>Median: $%{y:.1f}/MWh<extra></extra>",
        ))

    # Forecast line
    fig.add_trace(go.Scatter(
        x=hours, y=df_forecast["point_forecast"],
        mode="lines+markers", name="Forecast",
        line=dict(color="#FFA15A", width=2.5),
        marker=dict(size=5),
        hovertemplate="HE %{x}<br>Forecast: $%{y:.1f}/MWh<extra></extra>",
    ))

    # Override trace (initially identical to Forecast, dashed green — updated via JS)
    forecast_row = output_table[output_table["Type"] == "Forecast"].iloc[0]
    override_vals = [forecast_row[f"HE{h}"] for h in range(1, 25)]
    fig.add_trace(go.Scatter(
        x=list(range(1, 25)), y=override_vals,
        mode="lines+markers", name="Override",
        line=dict(color="#00CC96", width=2, dash="dash"),
        marker=dict(size=4),
        hovertemplate="HE %{x}<br>Override: $%{y:.1f}/MWh<extra></extra>",
    ))

    # Actual line + outlier markers
    if has_actuals:
        actual_row = output_table[output_table["Type"] == "Actual"].iloc[0]
        actual_vals = [actual_row[f"HE{h}"] for h in range(1, 25)]

        fig.add_trace(go.Scatter(
            x=list(range(1, 25)), y=actual_vals,
            mode="lines+markers", name="Actual",
            line=dict(color="#4cc9f0", width=2),
            marker=dict(size=5),
            hovertemplate="HE %{x}<br>Actual: $%{y:.1f}/MWh<extra></extra>",
        ))

        # Mark hours where actual falls outside P25-P75
        if "q_0.25" in df_forecast.columns and "q_0.75" in df_forecast.columns:
            q25_map = dict(zip(df_forecast["hour_ending"].astype(int), df_forecast["q_0.25"]))
            q75_map = dict(zip(df_forecast["hour_ending"].astype(int), df_forecast["q_0.75"]))
            outlier_hours, outlier_vals = [], []
            for h in range(1, 25):
                a = actual_vals[h - 1]
                lo, hi = q25_map.get(h), q75_map.get(h)
                if pd.notna(a) and lo is not None and hi is not None and (a < lo or a > hi):
                    outlier_hours.append(h)
                    outlier_vals.append(a)
            if outlier_hours:
                fig.add_trace(go.Scatter(
                    x=outlier_hours, y=outlier_vals,
                    mode="markers", name="Outside P25\u2013P75",
                    marker=dict(
                        color="#EF553B", size=10, symbol="diamond",
                        line=dict(width=1.5, color="#EF553B"),
                    ),
                    hovertemplate=(
                        "HE %{x}<br>Actual: $%{y:.1f}/MWh<br>"
                        "<b>Outside IQR</b><extra></extra>"
                    ),
                ))

    fig.update_layout(
        title="Quantile Bands vs Actual & Forecast",
        xaxis_title="Hour Ending",
        yaxis_title="$/MWh",
        height=500,
        template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="top", y=-0.12, xanchor="left", x=0),
        margin=dict(l=60, r=40, t=40, b=70),
        hovermode="x unified",
    )
    fig.update_xaxes(dtick=1, range=[0.5, 24.5])

    return fig.to_html(include_plotlyjs="cdn", full_html=False, div_id="qb-plot")


# ── Helpers ──────────────────────────────────────────────────────────


def _error_html(msg: str) -> str:
    return f'<div style="padding:16px;color:#e74c3c;font-size:14px;">{msg}</div>'
