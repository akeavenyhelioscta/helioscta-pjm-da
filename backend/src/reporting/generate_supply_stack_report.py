"""Generate a standalone supply stack input data validation HTML report.

Visualises the hourly inputs that feed the supply stack dispatch model:
load, solar, wind, net load, gas prices, and outages.

Usage:
    python -m src.reporting.generate_supply_stack_report
    python -m src.reporting.generate_supply_stack_report --date 2026-04-14
    python -m src.reporting.generate_supply_stack_report --region-preset dominion
    python -m src.reporting.generate_supply_stack_report --gas-hub gas_dom_south
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

_BACKEND = str(Path(__file__).resolve().parent.parent.parent)
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

from src.reporting.html_dashboard import HTMLDashboardBuilder
from src.supply_stack_model.data.fleet import load_fleet
from src.supply_stack_model.data.sources import (
    REGION_PRESETS,
    pull_hourly_inputs,
    resolve_forecast_date,
)

logger = logging.getLogger(__name__)

PLOTLY_TEMPLATE = "plotly_dark"
HE_COLS = [f"HE{h}" for h in range(1, 25)]
ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = [h for h in range(1, 25) if h not in ONPEAK_HOURS]
SUMMARY_COLS = ["OnPeak", "OffPeak", "Flat"]
REPORT_OUTPUT_DIR = Path(__file__).parent / "output"
GENERATORS_PARQUET_PATH = (
    Path(__file__).resolve().parent.parent
    / "supply_stack_model" / "data" / "pjm_fleet_generators.parquet"
)
NUCLEAR_VALIDATION_PATH = (
    Path(__file__).resolve().parent.parent
    / "supply_stack_model" / "data" / "nuclear_validation.parquet"
)
NRC_VALIDATION_PATH = (
    Path(__file__).resolve().parent.parent
    / "supply_stack_model" / "data" / "nrc_nuclear_validation.parquet"
)
NRC_DAILY_PATH = (
    Path(__file__).resolve().parent.parent
    / "supply_stack_model" / "data" / "nrc_daily_status.parquet"
)
NRC_PLANT_DAILY_PATH = (
    Path(__file__).resolve().parent.parent
    / "supply_stack_model" / "data" / "nrc_plant_daily.parquet"
)
HYDRO_VALIDATION_PATH = (
    Path(__file__).resolve().parent.parent
    / "supply_stack_model" / "data" / "hydro_validation.parquet"
)
GAS_VALIDATION_PATH = (
    Path(__file__).resolve().parent.parent
    / "supply_stack_model" / "data" / "gas_validation.parquet"
)
GAS_MONTHLY_PATH = (
    Path(__file__).resolve().parent.parent
    / "supply_stack_model" / "data" / "gas_monthly.parquet"
)

FUEL_COLORS = {
    "nuclear": "#AB63FA",
    "coal": "#8B6914",
    "cc_gas": "#636EFA",
    "ct_gas": "#FFA15A",
    "oil": "#EF553B",
    "hydro": "#00CC96",
    "storage": "#19D3F3",
    "other": "#B6E880",
}

PLOTLY_LOCKED_CONFIG = {
    "displaylogo": False,
    "scrollZoom": False,
    "doubleClick": False,
    "modeBarButtonsToRemove": [
        "zoom2d", "pan2d", "select2d", "lasso2d",
        "zoomIn2d", "zoomOut2d", "autoScale2d", "resetScale2d",
    ],
}


# ══════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════


def generate(
    forecast_date: str | date | None = None,
    region: str = "RTO",
    region_preset: str | None = None,
    gas_hub_col: str | None = None,
    output_dir: Path | None = None,
) -> Path:
    """Generate the supply stack input validation report and return the output path."""
    target = resolve_forecast_date(forecast_date)
    output_dir = output_dir or REPORT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Generating supply stack input report for %s (region=%s, preset=%s, gas=%s)",
        target, region, region_preset or "none", gas_hub_col or "auto",
    )

    df = pull_hourly_inputs(
        forecast_date=target,
        region=region,
        region_preset=region_preset,
        gas_hub_col=gas_hub_col,
    )

    preset_label = region_preset or region
    title = f"Supply Stack Inputs — {target} ({preset_label})"
    builder = HTMLDashboardBuilder(title=title, theme="dark")

    # # Section 1: Summary table
    # builder.add_content(
    #     f"Input Summary — {target}",
    #     _build_summary_table(df, target, region_preset, gas_hub_col),
    # )
    #
    # # Section 2: Component charts (Load | Solar | Wind)
    # builder.add_content(
    #     "Load / Solar / Wind Profiles",
    #     _build_component_charts_row(df),
    # )
    #
    # # Section 3: Net Load + Ramp
    # builder.add_content(
    #     "Net Load Breakdown",
    #     _build_net_load_row(df),
    # )
    #
    # # Section 4: Gas Price Profile
    # builder.add_content(
    #     "Gas Price Profile",
    #     _build_gas_price_chart(df, gas_hub_col),
    # )
    #
    # # Section 5: Outages
    # builder.add_content(
    #     "Outages",
    #     _build_outages_card(df),
    # )

    # ── Fleet Database ────────────────────────────────────────────
    fleet_csv = load_fleet()
    gen_df = (
        pd.read_parquet(GENERATORS_PARQUET_PATH)
        if GENERATORS_PARQUET_PATH.exists()
        else None
    )

    if gen_df is not None:
        # builder.add_divider("Fleet Database")
        #
        # builder.add_content(
        #     "Fleet Summary",
        #     _build_fleet_summary_cards(gen_df, fleet_csv),
        # )
        #
        # builder.add_content(
        #     "Capacity by Fuel Type",
        #     _build_fuel_bar(gen_df),
        # )
        #
        # builder.add_content(
        #     "Capacity by State",
        #     _build_state_bar(gen_df),
        # )
        #
        # builder.add_content(
        #     "Heat Rate Distribution",
        #     _build_heat_rate_box(gen_df),
        # )

        builder.add_content(
            "Generator Table",
            _build_filterable_table(gen_df),
        )

    # ── Nuclear Fleet ─────────────────────────────────────────────
    _has_eia = NUCLEAR_VALIDATION_PATH.exists()
    _has_nrc = NRC_VALIDATION_PATH.exists() and NRC_DAILY_PATH.exists()

    _has_plant_daily = NRC_PLANT_DAILY_PATH.exists()

    if _has_nrc or _has_eia:
        eia_df = pd.read_parquet(NUCLEAR_VALIDATION_PATH) if _has_eia else None
        nrc_units = pd.read_parquet(NRC_VALIDATION_PATH) if _has_nrc else None
        nrc_daily = pd.read_parquet(NRC_DAILY_PATH) if _has_nrc else None
        plant_daily = pd.read_parquet(NRC_PLANT_DAILY_PATH) if _has_plant_daily else None

        builder.add_divider("Nuclear Fleet")

        builder.add_content(
            "Nuclear Plant Status",
            _build_nuclear_merged_table(eia_df, nrc_units, plant_daily),
        )

        if nrc_daily is not None:
            try:
                from src.data import fuel_mix_hourly
                df_mix = fuel_mix_hourly.pull()
            except Exception as exc:
                logger.warning("Could not pull fuel mix for NRC overlay: %s", exc)
                df_mix = None

            builder.add_content(
                "NRC vs Fuel Mix (365d)",
                _build_nrc_fuelmix_overlay(nrc_daily, df_mix),
            )

    # ── Hydro Fleet ───────────────────────────────────────────────
    if HYDRO_VALIDATION_PATH.exists():
        hydro_df = pd.read_parquet(HYDRO_VALIDATION_PATH)
        builder.add_divider("Hydro Fleet")

        builder.add_content(
            "Hydro Plant Status",
            _build_hydro_table(hydro_df),
        )

        # Hydro monthly: fuel mix vs EIA-923
        try:
            if "df_mix" not in dir() or df_mix is None:
                from src.data import fuel_mix_hourly
                df_mix = fuel_mix_hourly.pull()
            builder.add_content(
                "Hydro Monthly: Fuel Mix vs EIA-923",
                _build_hydro_monthly_chart(df_mix, hydro_df),
            )
        except Exception as exc:
            logger.warning("Could not build hydro chart: %s", exc)

    # ── Gas Fleet (CEMS) ──────────────────────────────────────────
    if GAS_VALIDATION_PATH.exists():
        gas_val = pd.read_parquet(GAS_VALIDATION_PATH)
        gas_monthly = pd.read_parquet(GAS_MONTHLY_PATH) if GAS_MONTHLY_PATH.exists() else None

        builder.add_divider("Gas Fleet (CEMS)")

        builder.add_content(
            "Gas Plant Status",
            _build_gas_table(gas_val),
        )

        if gas_monthly is not None:
            try:
                if "df_mix" not in dir() or df_mix is None:
                    from src.data import fuel_mix_hourly
                    df_mix = fuel_mix_hourly.pull()
                builder.add_content(
                    "Gas Monthly: CEMS vs Fuel Mix",
                    _build_gas_monthly_chart(gas_monthly, df_mix),
                )
            except Exception as exc:
                logger.warning("Could not build gas chart: %s", exc)

    filename = f"supply_stack_inputs_{target}.html"
    out_path = output_dir / filename
    builder.save(str(out_path))
    logger.info("Saved: %s", out_path)
    return out_path


# ══════════════════════════════════════════════════════════════════════
# Section 1: Summary table with outright / ramp toggle
# ══════════════════════════════════════════════════════════════════════


def _build_summary_table(
    df: pd.DataFrame,
    target: date,
    region_preset: str | None,
    gas_hub_col: str | None,
) -> str:
    meta = (
        f"Forecast date: {target} | "
        f"Preset: {region_preset or 'none'} | "
        f"Gas hub: {gas_hub_col or 'auto'}"
    )

    tid = "ss-tbl"

    outright_rows = [
        _tbl_row("Load", "MW", df["load_mw"]),
        _tbl_row("Wind", "MW", df["wind_mw"]),
        _tbl_row("Solar", "MW", df["solar_mw"]),
        _tbl_row("Gas", "$/MMBtu", df["gas_price_usd_mmbtu"], decimals=2),
        _tbl_row("Outages", "MW", df["outages_mw"]),
    ]
    outright_net = [
        _tbl_row("Net Load", "MW", df["net_load_mw"]),
    ]

    ramp_rows = [
        _tbl_row("Load Ramp", "MW/hr", df["load_mw"].diff(), signed=True, sign_colors=True),
        _tbl_row("Wind Ramp", "MW/hr", df["wind_mw"].diff(), signed=True, sign_colors=True),
        _tbl_row("Solar Ramp", "MW/hr", df["solar_mw"].diff(), signed=True, sign_colors=True),
    ]
    ramp_net = [
        _tbl_row("Net Load Ramp", "MW/hr", df["net_load_mw"].diff(), signed=True, sign_colors=True),
    ]

    outright_body = _tbl_body(outright_rows, outright_net)
    ramp_body = _tbl_body(ramp_rows, ramp_net)

    cols = ["Metric", "Unit"] + HE_COLS + SUMMARY_COLS
    header = "<thead><tr>"
    for col in cols:
        cls = ' class="metric"' if col == "Metric" else ' class="unit"' if col == "Unit" else ""
        header += f"<th{cls}>{col}</th>"
    header += "</tr></thead>"

    toggle_btn = (
        f'<button class="ss-toggle" onclick="ssToggle(\'{tid}\')" id="{tid}-btn">Show Ramp</button>'
    )

    html = _STYLE
    html += f'<div class="ss-wrap"><div class="ss-meta">{meta}</div>'
    html += toggle_btn
    html += f'<div class="ss-tw"><table class="ss-t" id="{tid}">{header}'
    html += f'<tbody id="{tid}-outright">{outright_body}</tbody>'
    html += f'<tbody style="display:none;" id="{tid}-ramp">{ramp_body}</tbody>'
    html += "</table></div></div>"

    html += f"""<script>
function ssToggle(tid) {{
  var o = document.getElementById(tid + '-outright');
  var r = document.getElementById(tid + '-ramp');
  var b = document.getElementById(tid + '-btn');
  if (o.style.display === 'none') {{
    o.style.display = ''; r.style.display = 'none'; b.textContent = 'Show Ramp';
  }} else {{
    o.style.display = 'none'; r.style.display = ''; b.textContent = 'Show Outright';
  }}
}}
</script>"""
    return html


# ══════════════════════════════════════════════════════════════════════
# Section 2: Load / Solar / Wind component charts
# ══════════════════════════════════════════════════════════════════════


def _build_component_charts_row(df: pd.DataFrame) -> str:
    """Three subplots side by side: Load, Solar, Wind — each with outright/ramp toggle.

    Uses a single ``make_subplots`` figure so Plotly handles column layout
    internally, avoiding flex-container sizing issues that can truncate charts.
    """
    chart_id = "ss-comp"

    fig = make_subplots(
        rows=1, cols=3,
        subplot_titles=("Load Forecast", "Solar Forecast", "Wind Forecast"),
        horizontal_spacing=0.06,
    )

    hours = df["hour_ending"]
    components = [
        ("Load", df["load_mw"], "#60a5fa", 1),
        ("Solar", df["solar_mw"], "#fbbf24", 2),
        ("Wind", df["wind_mw"], "#34d399", 3),
    ]

    comp_traces: dict[str, dict[str, list[int]]] = {}

    for comp_label, values, color, col in components:
        comp_traces[comp_label] = {"outright": [], "ramp": []}
        ramp = values.diff()

        # Outright trace — visible
        comp_traces[comp_label]["outright"].append(len(fig.data))
        fig.add_trace(go.Scatter(
            x=hours, y=values, mode="lines+markers", name=f"{comp_label} Forecast",
            line=dict(color=color, width=2.2), marker=dict(size=4),
            showlegend=(col == 1),
            hovertemplate=f"HE %{{x}}<br>%{{y:,.0f}} MW<extra></extra>",
        ), row=1, col=col)

        # Ramp trace — hidden
        comp_traces[comp_label]["ramp"].append(len(fig.data))
        fig.add_trace(go.Bar(
            x=hours, y=ramp, name=f"{comp_label} Ramp",
            marker_color=color, opacity=0.8, visible=False,
            showlegend=False,
            hovertemplate=f"HE %{{x}}<br>%{{y:+,.0f}} MW/hr<extra></extra>",
        ), row=1, col=col)

    fig.update_layout(
        template=PLOTLY_TEMPLATE, height=380,
        margin=dict(l=50, r=20, t=40, b=60),
        legend=dict(font=dict(size=9), orientation="h", yanchor="top", y=-0.08, x=0),
        hovermode="x unified", barmode="group",
    )
    for col in range(1, 4):
        fig.update_xaxes(
            dtick=1, range=[0.5, 24.5], autorange=False, fixedrange=True,
            title_text="Hour Ending", row=1, col=col,
        )
        fig.update_yaxes(title_text="MW", row=1, col=col)

    chart_html = fig.to_html(
        include_plotlyjs="cdn", full_html=False,
        div_id=chart_id, config=PLOTLY_LOCKED_CONFIG,
    )

    yaxis_map = {"Load": "yaxis", "Solar": "yaxis2", "Wind": "yaxis3"}

    btn_row = '<div style="display:flex;gap:0;padding:4px 8px;">'
    for comp in ["Load", "Solar", "Wind"]:
        btn_id = f"{chart_id}-{comp.lower()}-btn"
        btn_row += (
            f'<div style="flex:1;text-align:center;">'
            f'<button class="ss-toggle" onclick="ssCompToggle(\'{comp}\')"'
            f' id="{btn_id}">Show Ramp</button>'
            f'</div>'
        )
    btn_row += '</div>'

    html = f'<div style="padding:8px;">{btn_row}{chart_html}</div>'

    html += f'''<script>
(function() {{
  var chartId = "{chart_id}";
  var traces  = {json.dumps(comp_traces)};
  var yaxMap  = {json.dumps(yaxis_map)};

  window.ssCompToggle = function(comp) {{
    var el = document.getElementById(chartId);
    if (!el) return;
    var btnId = chartId + "-" + comp.toLowerCase() + "-btn";
    var btn   = document.getElementById(btnId);
    var showingRamp = (btn.textContent === "Show Outright");

    traces[comp].outright.forEach(function(i) {{
      Plotly.restyle(chartId, {{visible: showingRamp}}, [i]);
    }});
    traces[comp].ramp.forEach(function(i) {{
      Plotly.restyle(chartId, {{visible: !showingRamp}}, [i]);
    }});

    btn.textContent = showingRamp ? "Show Ramp" : "Show Outright";

    var ro = {{}};
    ro[yaxMap[comp] + ".autorange"] = true;
    Plotly.relayout(chartId, ro);
  }};
}})();
</script>'''

    return html


# ══════════════════════════════════════════════════════════════════════
# Section 3: Net Load breakdown + ramp
# ══════════════════════════════════════════════════════════════════════


def _build_net_load_row(df: pd.DataFrame) -> str:
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("Net Load Breakdown", "Net Load Ramp"),
        horizontal_spacing=0.08,
    )

    hours = df["hour_ending"]
    net = df["net_load_mw"]
    solar = df["solar_mw"]
    wind = df["wind_mw"]
    load = df["load_mw"]
    ramp = net.diff()

    # ── Left: Stacked area (Net Load + Wind + Solar = Load) ──
    fig.add_trace(go.Scatter(
        x=hours, y=net, mode="lines", name="Net Load",
        stackgroup="stack",
        line=dict(color="#60a5fa", width=1),
        fillcolor="rgba(96, 165, 250, 0.50)",
        hovertemplate="HE %{x}<br>Net Load: %{y:,.0f} MW<extra></extra>",
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=hours, y=wind, mode="lines", name="Wind",
        stackgroup="stack",
        line=dict(color="#34d399", width=1),
        fillcolor="rgba(52, 211, 153, 0.35)",
        hovertemplate="HE %{x}<br>Wind: %{y:,.0f} MW<extra></extra>",
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=hours, y=solar, mode="lines", name="Solar",
        stackgroup="stack",
        line=dict(color="#fbbf24", width=1),
        fillcolor="rgba(251, 191, 36, 0.40)",
        hovertemplate="HE %{x}<br>Solar: %{y:,.0f} MW<extra></extra>",
    ), row=1, col=1)

    # Gross load line on top
    fig.add_trace(go.Scatter(
        x=hours, y=load, mode="lines", name="Load",
        line=dict(color="#f8fafc", width=2),
        hovertemplate="HE %{x}<br>Load: %{y:,.0f} MW<extra></extra>",
    ), row=1, col=1)

    # ── Right: Ramp bars ────────────────────────────────────────────
    bar_colors = ["#34d399" if (pd.notna(v) and v >= 0) else "#f87171" for v in ramp]
    fig.add_trace(go.Bar(
        x=hours, y=ramp, name="Net Load Ramp",
        marker_color=bar_colors, opacity=0.85,
        hovertemplate="HE %{x}<br>Ramp: %{y:+,.0f} MW/hr<extra></extra>",
    ), row=1, col=2)

    fig.add_hline(y=0, line_color="#7f8ea3", line_dash="dash", line_width=1, row=1, col=2)

    fig.update_layout(
        template=PLOTLY_TEMPLATE, height=450,
        margin=dict(l=60, r=40, t=40, b=55),
        legend=dict(orientation="h", yanchor="top", y=-0.12, x=0),
        hovermode="x unified", barmode="relative",
    )
    fig.update_xaxes(title_text="Hour Ending", dtick=1, range=[0.5, 24.5], autorange=False, fixedrange=True)
    fig.update_yaxes(title_text="MW", col=1)
    fig.update_yaxes(title_text="MW/hr", col=2)

    return fig.to_html(include_plotlyjs="cdn", full_html=False, div_id="ss-netload", config=PLOTLY_LOCKED_CONFIG)


# ══════════════════════════════════════════════════════════════════════
# Section 4: Gas Price Profile
# ══════════════════════════════════════════════════════════════════════


def _build_gas_price_chart(df: pd.DataFrame, gas_hub: str | None) -> str:
    hub_label = gas_hub or "auto"
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df["hour_ending"], y=df["gas_price_usd_mmbtu"],
        mode="lines+markers", name=f"Gas ({hub_label})",
        line=dict(color="#f87171", width=2.5), marker=dict(size=5),
        hovertemplate="HE %{x}<br>Gas: $%{y:.2f}/MMBtu<extra></extra>",
    ))

    fig.update_layout(
        title=f"Gas Price — {hub_label}",
        template=PLOTLY_TEMPLATE, height=350,
        margin=dict(l=60, r=40, t=40, b=40),
        hovermode="x unified",
    )
    fig.update_xaxes(dtick=1, range=[0.5, 24.5], autorange=False, fixedrange=True, title_text="Hour Ending")
    fig.update_yaxes(title_text="$/MMBtu")

    return fig.to_html(include_plotlyjs="cdn", full_html=False, div_id="ss-gas", config=PLOTLY_LOCKED_CONFIG)


# ══════════════════════════════════════════════════════════════════════
# Section 5: Outages card
# ══════════════════════════════════════════════════════════════════════


def _build_outages_card(df: pd.DataFrame) -> str:
    outage_mw = float(df["outages_mw"].iloc[0])

    card = (
        "background:#111d31;border:1px solid #253b59;border-radius:10px;"
        "padding:16px 22px;max-width:320px;"
    )
    label_s = (
        "font-size:10px;font-weight:600;color:#6f8db1;"
        "text-transform:uppercase;letter-spacing:0.5px;"
    )
    val_s = (
        "font-size:22px;font-weight:700;color:#dbe7ff;"
        "font-family:'Space Grotesk',monospace;margin-top:4px;"
    )

    return (
        f'<div style="padding:12px;">'
        f'<div style="{card}">'
        f'<div style="{label_s}">Forecast Outages (Flat)</div>'
        f'<div style="{val_s}">{outage_mw:,.0f} MW</div>'
        f'</div>'
        f'</div>'
    )


# ══════════════════════════════════════════════════════════════════════
# Fleet Database sections
# ══════════════════════════════════════════════════════════════════════


def _build_fleet_summary_cards(gen_df: pd.DataFrame, fleet_csv: pd.DataFrame) -> str:
    total_cap = gen_df["effective_capacity_mw"].sum()
    n_generators = len(gen_df)
    n_plants = gen_df["plant_id_eia"].nunique()
    n_states = gen_df["state"].nunique()
    n_fleet_blocks = len(fleet_csv)
    dominant_fuel = gen_df.groupby("fleet_fuel_type")["effective_capacity_mw"].sum().idxmax()

    card = (
        "background:#111d31;border:1px solid #253b59;border-radius:10px;"
        "padding:14px 18px;min-width:130px;flex:1;"
    )
    label_s = (
        "font-size:10px;font-weight:600;color:#6f8db1;"
        "text-transform:uppercase;letter-spacing:0.5px;"
    )
    val_s = (
        "font-size:20px;font-weight:700;color:#dbe7ff;"
        "font-family:'Space Grotesk',monospace;margin-top:2px;"
    )

    cards = [
        ("Total Capacity", f"{total_cap / 1000:,.1f} GW"),
        ("Generators", f"{n_generators:,}"),
        ("Plants", f"{n_plants:,}"),
        ("States", str(n_states)),
        ("Fleet Blocks", str(n_fleet_blocks)),
        ("Dominant Fuel", dominant_fuel),
    ]

    html = '<div style="display:flex;gap:10px;flex-wrap:wrap;padding:12px 8px;">'
    for lbl, val in cards:
        html += (
            f'<div style="{card}">'
            f'<div style="{label_s}">{lbl}</div>'
            f'<div style="{val_s}">{val}</div>'
            f'</div>'
        )
    html += "</div>"
    return html


def _build_fuel_bar(gen_df: pd.DataFrame) -> str:
    fuel_cap = (
        gen_df.groupby("fleet_fuel_type")["effective_capacity_mw"]
        .sum()
        .sort_values(ascending=True)
        .reset_index()
    )
    colors = [FUEL_COLORS.get(f, "#94a3b8") for f in fuel_cap["fleet_fuel_type"]]

    fig = go.Figure(go.Bar(
        y=fuel_cap["fleet_fuel_type"],
        x=fuel_cap["effective_capacity_mw"] / 1000,
        orientation="h",
        marker_color=colors,
        text=[f"{v:,.1f} GW" for v in fuel_cap["effective_capacity_mw"] / 1000],
        textposition="outside",
        hovertemplate="%{y}<br>%{x:.1f} GW<extra></extra>",
    ))
    fig.update_layout(
        title="Capacity by Fuel Type",
        template=PLOTLY_TEMPLATE, height=350,
        margin=dict(l=100, r=80, t=40, b=40),
        xaxis_title="Capacity (GW)",
    )
    return fig.to_html(include_plotlyjs="cdn", full_html=False)


def _build_state_bar(gen_df: pd.DataFrame) -> str:
    state_fuel = (
        gen_df.groupby(["state", "fleet_fuel_type"])["effective_capacity_mw"]
        .sum()
        .reset_index()
    )
    state_order = (
        state_fuel.groupby("state")["effective_capacity_mw"]
        .sum()
        .sort_values(ascending=False)
        .index.tolist()
    )
    fuel_types = sorted(state_fuel["fleet_fuel_type"].unique())

    fig = go.Figure()
    for fuel in fuel_types:
        sub = state_fuel[state_fuel["fleet_fuel_type"] == fuel]
        sub = sub.set_index("state").reindex(state_order).fillna(0)
        fig.add_trace(go.Bar(
            x=sub.index,
            y=sub["effective_capacity_mw"] / 1000,
            name=fuel,
            marker_color=FUEL_COLORS.get(fuel, "#94a3b8"),
            hovertemplate="%{x}<br>" + fuel + ": %{y:.1f} GW<extra></extra>",
        ))

    fig.update_layout(
        title="Capacity by State & Fuel Type",
        template=PLOTLY_TEMPLATE, height=400,
        margin=dict(l=60, r=40, t=40, b=40),
        barmode="stack",
        yaxis_title="Capacity (GW)",
        legend=dict(orientation="h", yanchor="top", y=-0.12, x=0),
    )
    return fig.to_html(include_plotlyjs="cdn", full_html=False)


def _build_heat_rate_box(gen_df: pd.DataFrame) -> str:
    hr_df = gen_df[gen_df["avg_heat_rate"].notna() & (gen_df["avg_heat_rate"] > 0)].copy()
    fuel_types = sorted(hr_df["fleet_fuel_type"].unique())

    fig = go.Figure()
    for fuel in fuel_types:
        sub = hr_df[hr_df["fleet_fuel_type"] == fuel]
        fig.add_trace(go.Box(
            y=sub["avg_heat_rate"],
            name=fuel,
            marker_color=FUEL_COLORS.get(fuel, "#94a3b8"),
            boxpoints="outliers",
            hovertemplate=fuel + "<br>HR: %{y:.2f} MMBtu/MWh<extra></extra>",
        ))

    fig.update_layout(
        title="Heat Rate Distribution by Fuel Type (EIA-923 data)",
        template=PLOTLY_TEMPLATE, height=400,
        margin=dict(l=60, r=40, t=40, b=40),
        yaxis_title="Heat Rate (MMBtu/MWh)",
    )
    return fig.to_html(include_plotlyjs="cdn", full_html=False)


def _build_filterable_table(gen_df: pd.DataFrame) -> str:
    """Interactive HTML table with dropdown filters and capacity bars."""
    # Ensure pjm_zone column exists
    if "pjm_zone" not in gen_df.columns:
        gen_df = gen_df.copy()
        gen_df["pjm_zone"] = ""

    plant_df = (
        gen_df.groupby(
            ["plant_id_eia", "plant_name_eia", "utility_name_eia",
             "technology_description", "fuel_type_code_pudl",
             "prime_mover_code", "state", "pjm_zone", "fleet_fuel_type", "gas_hub"],
            observed=True,
        )
        .agg(
            capacity_mw=("effective_capacity_mw", "sum"),
            heat_rate=("avg_heat_rate", "mean"),
            n_units=("generator_id", "count"),
        )
        .reset_index()
        .sort_values("capacity_mw", ascending=False)
        .reset_index(drop=True)
    )

    max_cap = float(plant_df["capacity_mw"].max()) if len(plant_df) > 0 else 1.0

    technologies = sorted(plant_df["technology_description"].dropna().unique().tolist())
    fuels = sorted(plant_df["fuel_type_code_pudl"].dropna().unique().tolist())
    zones = sorted([z for z in plant_df["pjm_zone"].dropna().unique().tolist() if z])
    states = sorted(plant_df["state"].dropna().unique().tolist())
    fleet_types = sorted(plant_df["fleet_fuel_type"].dropna().unique().tolist())

    tid = "fleet-tbl"

    def _select(col_id: str, options: list[str], label: str) -> str:
        opts = f'<option value="">All {label}</option>'
        for o in options:
            opts += f'<option value="{o}">{o}</option>'
        return (
            f'<select id="{col_id}" onchange="fleetFilter()" '
            f'style="background:#0f1a2b;color:#dbe7ff;border:1px solid #2a3f60;'
            f'border-radius:4px;padding:4px 8px;font-size:11px;font-family:monospace;'
            f'max-width:180px;width:100%;">{opts}</select>'
        )

    filter_bar = '<div style="display:flex;gap:8px;flex-wrap:wrap;padding:8px;align-items:end;">'
    for fid, opts, label in [
        ("flt-tech", technologies, "Technology"),
        ("flt-fuel", fuels, "Fuel"),
        ("flt-zone", zones, "Zone"),
        ("flt-state", states, "State"),
        ("flt-fleet", fleet_types, "Fleet Type"),
    ]:
        filter_bar += (
            f'<div style="flex:1;min-width:120px;">'
            f'<div style="font-size:10px;color:#6f8db1;margin-bottom:2px;">{label}</div>'
            f'{_select(fid, opts, label)}'
            f'</div>'
        )
    filter_bar += (
        '<div style="flex:2;min-width:180px;">'
        '<div style="font-size:10px;color:#6f8db1;margin-bottom:2px;">Search Facility</div>'
        '<input id="flt-search" type="text" placeholder="Type to search..." '
        'oninput="fleetFilter()" '
        'style="background:#0f1a2b;color:#dbe7ff;border:1px solid #2a3f60;'
        'border-radius:4px;padding:4px 8px;font-size:11px;font-family:monospace;'
        'width:100%;box-sizing:border-box;" />'
        '</div>'
    )
    filter_bar += (
        '<div style="min-width:100px;text-align:right;">'
        f'<span id="fleet-count" style="font-size:12px;color:#dbe7ff;font-family:monospace;">'
        f'{len(plant_df)} plants</span>'
        '</div>'
    )
    filter_bar += '</div>'

    columns = [
        ("Facility", "left", "250px"),
        ("Technology", "left", "180px"),
        ("Fuel", "left", "60px"),
        ("Zone", "center", "55px"),
        ("State", "center", "50px"),
        ("Fleet Type", "left", "70px"),
        ("Hub", "left", "90px"),
        ("Units", "right", "45px"),
        ("HR", "right", "55px"),
        ("Capacity (MW)", "right", "200px"),
    ]

    html = filter_bar
    html += '<div style="overflow-x:auto;padding:0 8px 8px 8px;">'
    html += f'<table id="{tid}" style="width:100%;border-collapse:collapse;font-size:11px;font-family:monospace;">'
    html += '<thead><tr>'
    for col_name, align, width in columns:
        html += (
            f'<th style="padding:6px 8px;background:#16263d;color:#e6efff;'
            f'text-align:{align};font-size:11px;position:sticky;top:0;'
            f'min-width:{width};white-space:nowrap;">{col_name}</th>'
        )
    html += '</tr></thead><tbody>'

    for i, (_, row) in enumerate(plant_df.iterrows()):
        bg = "rgba(18, 32, 50, 0.45)" if i % 2 == 0 else "transparent"
        cap = float(row["capacity_mw"])
        bar_pct = (cap / max_cap * 100) if max_cap > 0 else 0
        hr = row["heat_rate"]
        hr_str = f"{hr:.2f}" if pd.notna(hr) and hr > 0 else "\u2014"
        fuel_color = FUEL_COLORS.get(str(row["fleet_fuel_type"]), "#94a3b8")
        hub = row["gas_hub"] if row["gas_hub"] else "\u2014"

        zone = row.get("pjm_zone", "") or ""
        zone_display = zone or "\u2014"

        html += (
            f'<tr style="background:{bg};" '
            f'data-tech="{row["technology_description"]}" '
            f'data-fuel="{row["fuel_type_code_pudl"]}" '
            f'data-zone="{zone}" '
            f'data-state="{row["state"]}" '
            f'data-fleet="{row["fleet_fuel_type"]}" '
            f'data-name="{str(row["plant_name_eia"]).lower()}">'
            f'<td style="padding:5px 8px;border-bottom:1px solid #1f334f;text-align:left;color:#dbe7ff;font-weight:600;">{row["plant_name_eia"]}</td>'
            f'<td style="padding:5px 8px;border-bottom:1px solid #1f334f;text-align:left;color:#a6bad6;">{row["technology_description"]}</td>'
            f'<td style="padding:5px 8px;border-bottom:1px solid #1f334f;text-align:left;color:#a6bad6;">{row["fuel_type_code_pudl"]}</td>'
            f'<td style="padding:5px 8px;border-bottom:1px solid #1f334f;text-align:center;color:#636EFA;font-weight:600;">{zone_display}</td>'
            f'<td style="padding:5px 8px;border-bottom:1px solid #1f334f;text-align:center;color:#a6bad6;">{row["state"]}</td>'
            f'<td style="padding:5px 8px;border-bottom:1px solid #1f334f;text-align:left;color:{fuel_color};font-weight:600;">{row["fleet_fuel_type"]}</td>'
            f'<td style="padding:5px 8px;border-bottom:1px solid #1f334f;text-align:left;color:#a6bad6;">{hub}</td>'
            f'<td style="padding:5px 8px;border-bottom:1px solid #1f334f;text-align:right;color:#a6bad6;">{int(row["n_units"])}</td>'
            f'<td style="padding:5px 8px;border-bottom:1px solid #1f334f;text-align:right;color:#dbe7ff;">{hr_str}</td>'
            f'<td style="padding:5px 8px;border-bottom:1px solid #1f334f;text-align:right;">'
            f'<div style="display:flex;align-items:center;justify-content:flex-end;gap:8px;">'
            f'<div style="width:120px;height:8px;background:#1a2d48;border-radius:4px;overflow:hidden;">'
            f'<div style="width:{bar_pct:.0f}%;height:100%;background:{fuel_color};border-radius:4px;"></div>'
            f'</div>'
            f'<span style="color:#dbe7ff;min-width:60px;text-align:right;">{cap:,.0f}</span>'
            f'</div></td>'
            f'</tr>'
        )

    html += '</tbody></table></div>'

    html += f'''<script>
function fleetFilter() {{
  var tbl = document.getElementById("{tid}");
  var rows = tbl.getElementsByTagName("tbody")[0].getElementsByTagName("tr");
  var tech = document.getElementById("flt-tech").value;
  var fuel = document.getElementById("flt-fuel").value;
  var zone = document.getElementById("flt-zone").value;
  var state = document.getElementById("flt-state").value;
  var fleet = document.getElementById("flt-fleet").value;
  var search = document.getElementById("flt-search").value.toLowerCase();
  var shown = 0;
  for (var i = 0; i < rows.length; i++) {{
    var r = rows[i];
    var show = true;
    if (tech && r.getAttribute("data-tech") !== tech) show = false;
    if (fuel && r.getAttribute("data-fuel") !== fuel) show = false;
    if (zone && r.getAttribute("data-zone") !== zone) show = false;
    if (state && r.getAttribute("data-state") !== state) show = false;
    if (fleet && r.getAttribute("data-fleet") !== fleet) show = false;
    if (search && r.getAttribute("data-name").indexOf(search) < 0) show = false;
    r.style.display = show ? "" : "none";
    if (show) shown++;
  }}
  document.getElementById("fleet-count").textContent = shown + " plants";
}}
</script>'''

    return html


# ══════════════════════════════════════════════════════════════════════
# Nuclear Fleet — merged table + NRC vs fuel mix chart
# ══════════════════════════════════════════════════════════════════════


def _build_nuclear_merged_table(
    eia_df: pd.DataFrame | None,
    nrc_units: pd.DataFrame | None,
    plant_daily: pd.DataFrame | None = None,
) -> str:
    """Plant-level nuclear table matching the outage-dashboard style."""

    # ── Aggregate NRC units to plant level ─────────────────────────
    nrc_by_plant: dict[int, dict] = {}
    plant_names: dict[int, str] = {}
    plant_states: dict[int, str] = {}
    plant_zones: dict[int, str] = {}

    if nrc_units is not None:
        for pid, grp in nrc_units.groupby("plant_id_eia"):
            pid = int(pid)
            cap = float(grp["capacity_mw"].sum())
            eff = float(grp["current_effective_mw"].sum())
            out = cap - eff
            pct = eff / cap * 100 if cap > 0 else 0
            all_off = (grp["current_power_pct"] == 0).all()
            any_off = (grp["current_power_pct"] == 0).any()
            nrc_by_plant[pid] = {
                "cap_mw": cap, "avail_mw": eff, "out_mw": out, "pct_avail": pct,
                "status": "offline" if all_off else ("reduced" if any_off or pct < 95 else "full"),
            }

    # Enrich with EIA names/states/zones
    if eia_df is not None:
        for _, row in eia_df.iterrows():
            pid = int(row["plant_id_eia"])
            plant_names[pid] = str(row.get("plant_name_eia", pid))
            plant_states[pid] = str(row.get("state", ""))

    # Load zones from generators parquet
    if GENERATORS_PARQUET_PATH.exists():
        _gen = pd.read_parquet(GENERATORS_PARQUET_PATH)
        if "pjm_zone" in _gen.columns:
            for pid, grp in _gen[_gen["fleet_fuel_type"] == "nuclear"].groupby("plant_id_eia"):
                zone_val = grp["pjm_zone"].dropna().iloc[0] if len(grp["pjm_zone"].dropna()) > 0 else ""
                plant_zones[int(pid)] = str(zone_val)
    if nrc_units is not None:
        for _, row in nrc_units.iterrows():
            pid = int(row["plant_id_eia"])
            if pid not in plant_names:
                # Derive from unit name (strip trailing " 1", " 2", etc.)
                uname = str(row["unit_name"])
                plant_names[pid] = uname.rsplit(" ", 1)[0] if uname[-1].isdigit() else uname

    # ── Compute DoD and 7D sparkline from plant_daily ──────────────
    dod_by_plant: dict[int, float] = {}
    spark_by_plant: dict[int, list[float]] = {}

    if plant_daily is not None:
        plant_daily = plant_daily.copy()
        plant_daily["date"] = pd.to_datetime(plant_daily["date"]).dt.date
        for pid, grp in plant_daily.groupby("plant_id_eia"):
            pid = int(pid)
            grp = grp.sort_values("date")
            if len(grp) >= 2:
                dod_by_plant[pid] = float(grp["effective_mw"].iloc[-1] - grp["effective_mw"].iloc[-2])
            # Last 7 days for sparkline
            last7 = grp.tail(7)["effective_mw"].tolist()
            spark_by_plant[pid] = last7

    # ── Build sorted plant list ────────────────────────────────────
    all_pids = sorted(nrc_by_plant.keys(), key=lambda p: -nrc_by_plant[p]["cap_mw"])

    # Sort: reduced first, then partial, then full (by cap descending within each group)
    status_order = {"reduced": 0, "offline": 0, "partial": 1, "full": 2}
    all_pids.sort(key=lambda p: (status_order.get(nrc_by_plant[p]["status"], 2), -nrc_by_plant[p]["cap_mw"]))

    # ── Fleet totals ───────────────────────────────────────────────
    total_cap = sum(v["cap_mw"] for v in nrc_by_plant.values())
    total_avail = sum(v["avail_mw"] for v in nrc_by_plant.values())
    total_out = total_cap - total_avail
    total_pct = total_avail / total_cap * 100 if total_cap > 0 else 0
    total_dod = sum(dod_by_plant.values()) if dod_by_plant else 0
    dod_sign = "+" if total_dod >= 0 else ""

    # ── Header bar ─────────────────────────────────────────────────
    bar_color = "#00CC96" if total_pct >= 85 else "#FFA15A" if total_pct >= 70 else "#EF553B"

    html = (
        '<div style="background:#0a1628;border:1px solid #253b59;border-radius:10px;'
        'padding:16px 20px;margin:8px;">'
        # Top line
        '<div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:4px;">'
        f'<div><span style="color:#636EFA;font-size:12px;font-weight:700;">PJM</span>'
        f'<span style="color:#a6bad6;font-size:12px;margin-left:12px;">DoD</span>'
        f'<span style="color:{"#00CC96" if total_dod >= 0 else "#EF553B"};font-size:12px;font-weight:600;margin-left:4px;">'
        f'{dod_sign}{total_dod:,.0f} MW</span></div>'
        f'<div style="text-align:right;"><span style="color:#6f8db1;font-size:10px;">Outage</span></div>'
        '</div>'
        # Big number line
        '<div style="display:flex;justify-content:space-between;align-items:baseline;">'
        f'<div><span style="color:#dbe7ff;font-size:28px;font-weight:800;font-family:monospace;">'
        f'{total_avail:,.0f} MW</span>'
        f'<span style="color:#6f8db1;font-size:13px;margin-left:10px;">available of {total_cap:,.0f} MW</span>'
        f'<span style="color:{bar_color};font-size:13px;font-weight:600;margin-left:8px;">{total_pct:.1f}%</span></div>'
        f'<div><span style="color:#EF553B;font-size:22px;font-weight:800;font-family:monospace;">'
        f'{total_out:,.0f} MW</span></div>'
        '</div>'
        # Utilization bar
        f'<div style="width:100%;height:8px;background:#1a2d48;border-radius:4px;margin-top:8px;overflow:hidden;">'
        f'<div style="width:{total_pct:.0f}%;height:100%;background:{bar_color};border-radius:4px;"></div>'
        f'</div>'
        '</div>'
    )

    # ── Table ──────────────────────────────────────────────────────
    status_badge = {
        "full": ("FULL", "#00CC96"),
        "partial": ("PARTIAL", "#FFA15A"),
        "reduced": ("REDUCED", "#EF553B"),
        "offline": ("OFFLINE", "#EF553B"),
    }

    html += '<div style="overflow-x:auto;padding:8px;">'
    html += '<table style="width:100%;border-collapse:collapse;font-size:12px;font-family:monospace;">'
    html += '<thead><tr>'
    for col, align, width in [
        ("PLANT", "left", "280px"), ("CAP MW", "right", "80px"),
        ("AVAIL MW", "right", "90px"), ("OUT MW", "right", "70px"),
        ("% AVAIL", "right", "120px"), ("DOD", "right", "60px"),
        ("7D TREND", "center", "80px"),
    ]:
        html += (
            f'<th style="padding:8px 10px;background:transparent;color:#6f8db1;'
            f'text-align:{align};font-size:10px;font-weight:600;letter-spacing:0.5px;'
            f'border-bottom:1px solid #253b59;min-width:{width};">{col}</th>'
        )
    html += '</tr></thead><tbody>'

    for pid in all_pids:
        info = nrc_by_plant[pid]
        name = plant_names.get(pid, str(pid))
        state = plant_states.get(pid, "")
        zone = plant_zones.get(pid, "")
        status = info["status"]
        badge_text, badge_color = status_badge.get(status, ("?", "#556"))
        cap = info["cap_mw"]
        avail = info["avail_mw"]
        out = info["out_mw"]
        pct = info["pct_avail"]
        dod = dod_by_plant.get(pid, 0)
        spark = spark_by_plant.get(pid, [])

        pct_color = "#00CC96" if pct >= 95 else "#FFA15A" if pct >= 50 else "#EF553B"

        # DoD display
        if abs(dod) < 0.5:
            dod_str = '<span style="color:#556;">0</span>'
        else:
            dod_color = "#00CC96" if dod > 0 else "#EF553B"
            dod_str = f'<span style="color:{dod_color};font-weight:600;">{dod:+,.0f}</span>'

        # SVG sparkline (7 days)
        spark_svg = ""
        if len(spark) >= 2:
            cap_for_spark = cap if cap > 0 else 1
            # Normalize to 0-1 range based on capacity
            normed = [min(s / cap_for_spark, 1.0) for s in spark]
            w, h = 70, 24
            points = []
            for j, v in enumerate(normed):
                x = j / (len(normed) - 1) * w
                y = h - v * h
                points.append(f"{x:.1f},{y:.1f}")
            polyline = " ".join(points)
            spark_svg = (
                f'<svg width="{w}" height="{h}" style="vertical-align:middle;">'
                f'<polyline points="{polyline}" fill="none" stroke="#636EFA" stroke-width="1.5" />'
                f'</svg>'
            )

        html += (
            f'<tr>'
            # Plant name with badge
            f'<td style="padding:8px 10px;border-bottom:1px solid #1a2d48;text-align:left;">'
            f'<span style="background:{badge_color};color:#0a1628;padding:2px 6px;border-radius:3px;'
            f'font-size:9px;font-weight:800;margin-right:8px;">{badge_text}</span>'
            f'<span style="color:#dbe7ff;font-weight:600;">{name}</span>'
            f'<span style="color:#636EFA;font-size:10px;font-weight:600;margin-left:6px;">{zone}</span>'
            f'<span style="color:#556;font-size:10px;margin-left:4px;">{state}</span></td>'
            # Cap MW
            f'<td style="padding:8px 10px;border-bottom:1px solid #1a2d48;text-align:right;color:#a6bad6;">{cap:,.0f}</td>'
            # Avail MW
            f'<td style="padding:8px 10px;border-bottom:1px solid #1a2d48;text-align:right;color:#dbe7ff;font-weight:700;">{avail:,.0f}</td>'
            # Out MW
            f'<td style="padding:8px 10px;border-bottom:1px solid #1a2d48;text-align:right;color:#a6bad6;">{out:,.0f}</td>'
            # % Avail with bar
            f'<td style="padding:8px 10px;border-bottom:1px solid #1a2d48;text-align:right;">'
            f'<div style="display:flex;align-items:center;justify-content:flex-end;gap:8px;">'
            f'<span style="color:#dbe7ff;min-width:40px;text-align:right;">{pct:.1f}%</span>'
            f'<div style="width:60px;height:6px;background:#1a2d48;border-radius:3px;overflow:hidden;">'
            f'<div style="width:{min(pct, 100):.0f}%;height:100%;background:{pct_color};border-radius:3px;"></div>'
            f'</div></div></td>'
            # DoD
            f'<td style="padding:8px 10px;border-bottom:1px solid #1a2d48;text-align:right;">{dod_str}</td>'
            # 7D sparkline
            f'<td style="padding:8px 10px;border-bottom:1px solid #1a2d48;text-align:center;">{spark_svg}</td>'
            f'</tr>'
        )

    html += '</tbody></table></div>'
    return html


def _build_nrc_fuelmix_overlay(
    nrc_daily: pd.DataFrame,
    df_mix: pd.DataFrame | None,
) -> str:
    """Single-panel chart: NRC effective capacity vs fuel mix nuclear, with stats."""
    nrc = nrc_daily.copy()
    nrc["date"] = pd.to_datetime(nrc["date"])
    nameplate_gw = float(nrc["nameplate_mw"].iloc[0]) / 1000

    fig = go.Figure()

    # NRC effective capacity
    fig.add_trace(go.Scatter(
        x=nrc["date"], y=nrc["effective_mw"] / 1000,
        mode="lines", name="NRC Effective",
        fill="tozeroy", fillcolor="rgba(171, 99, 250, 0.20)",
        line=dict(color="#AB63FA", width=2),
        hovertemplate="%{x|%b %d}<br>NRC: %{y:.1f} GW<extra></extra>",
    ))

    stats_text = ""

    if df_mix is not None and len(df_mix) > 0:
        df_mix = df_mix.copy()
        df_mix["date"] = pd.to_datetime(df_mix["date"])
        fm_daily = (
            df_mix.groupby("date")["nuclear"]
            .agg(["mean", "min", "max"])
            .reset_index()
            .rename(columns={"mean": "fm_avg_mw", "min": "fm_min_mw", "max": "fm_max_mw"})
        )
        merged = nrc.merge(fm_daily, on="date", how="inner").sort_values("date")

        if len(merged) > 0:
            # Fuel mix hourly range band
            fig.add_trace(go.Scatter(
                x=merged["date"], y=merged["fm_max_mw"] / 1000,
                mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip",
            ))
            fig.add_trace(go.Scatter(
                x=merged["date"], y=merged["fm_min_mw"] / 1000,
                mode="lines", line=dict(width=0),
                fill="tonexty", fillcolor="rgba(76, 201, 240, 0.12)",
                name="Fuel Mix (hourly range)", hoverinfo="skip",
            ))

            # Fuel mix daily average
            fig.add_trace(go.Scatter(
                x=merged["date"], y=merged["fm_avg_mw"] / 1000,
                mode="lines", name="Fuel Mix (daily avg)",
                line=dict(color="#4cc9f0", width=2),
                hovertemplate="%{x|%b %d}<br>Fuel Mix: %{y:.1f} GW<extra></extra>",
            ))

            # Compute stats
            corr = float(merged["effective_mw"].corr(merged["fm_avg_mw"]))
            avg_diff = float((merged["effective_mw"] - merged["fm_avg_mw"]).mean()) / 1000
            stats_text = f"Correlation: {corr:.3f} | Avg diff (NRC−FM): {avg_diff:+.2f} GW"

    # Nameplate reference
    fig.add_hline(
        y=nameplate_gw, line_color="#636EFA", line_dash="dot", line_width=1,
        annotation_text=f"Nameplate: {nameplate_gw:.1f} GW",
        annotation_position="top right",
        annotation_font_color="#636EFA",
    )

    if stats_text:
        fig.add_annotation(
            text=stats_text,
            xref="paper", yref="paper", x=0.01, y=1.06,
            showarrow=False, font=dict(size=11, color="#a6bad6"),
        )

    fig.update_layout(
        title="NRC Effective Capacity vs Fuel Mix Nuclear Generation",
        template=PLOTLY_TEMPLATE, height=420,
        margin=dict(l=60, r=40, t=55, b=40),
        legend=dict(orientation="h", yanchor="top", y=-0.10, x=0),
        yaxis_title="GW",
        hovermode="x unified",
    )
    fig.update_yaxes(autorange=True)

    return fig.to_html(include_plotlyjs="cdn", full_html=False)


# ══════════════════════════════════════════════════════════════════════
# Hydro Fleet sections
# ══════════════════════════════════════════════════════════════════════


def _build_hydro_table(hydro_df: pd.DataFrame) -> str:
    """Hydro plant table — unified, sorted pumped-first then conventional by cap."""
    conventional = hydro_df[~hydro_df["is_pumped_storage"]].copy()
    pumped = hydro_df[hydro_df["is_pumped_storage"]].copy()

    conv_cap = float(conventional["fleet_capacity_mw"].sum())
    pump_cap = float(pumped["fleet_capacity_mw"].sum())
    total_cap = conv_cap + pump_cap
    conv_gen = float(conventional["total_gen_gwh"].sum())
    pump_gen = float(pumped["total_gen_gwh"].sum())
    conv_avg_cf = float(conventional["capacity_factor_pct"].mean()) if len(conventional) > 0 else 0

    # ── Header bar (matches nuclear style) ──
    html = (
        '<div style="background:#0a1628;border:1px solid #253b59;border-radius:10px;'
        'padding:16px 20px;margin:8px;">'
        # Top line
        '<div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:4px;">'
        f'<div><span style="color:#00CC96;font-size:12px;font-weight:700;">PJM Hydro</span>'
        f'<span style="color:#a6bad6;font-size:12px;margin-left:12px;">'
        f'{len(conventional)} conventional + {len(pumped)} pumped storage</span></div>'
        f'<div style="text-align:right;"><span style="color:#6f8db1;font-size:10px;">Pumped Storage</span></div>'
        '</div>'
        # Big number line
        '<div style="display:flex;justify-content:space-between;align-items:baseline;">'
        f'<div><span style="color:#dbe7ff;font-size:28px;font-weight:800;font-family:monospace;">'
        f'{total_cap:,.0f} MW</span>'
        f'<span style="color:#6f8db1;font-size:13px;margin-left:10px;">total capacity</span>'
        f'<span style="color:#00CC96;font-size:13px;font-weight:600;margin-left:12px;">'
        f'{conv_cap:,.0f} MW conv</span>'
        f'<span style="color:#6f8db1;font-size:12px;margin-left:4px;">(avg CF {conv_avg_cf:.0f}%)</span></div>'
        f'<div><span style="color:#19D3F3;font-size:22px;font-weight:800;font-family:monospace;">'
        f'{pump_cap:,.0f} MW</span></div>'
        '</div>'
        # Capacity bar (conv green + pumped cyan)
        f'<div style="width:100%;height:8px;background:#1a2d48;border-radius:4px;margin-top:8px;overflow:hidden;display:flex;">'
        f'<div style="width:{conv_cap / total_cap * 100:.0f}%;height:100%;background:#00CC96;"></div>'
        f'<div style="width:{pump_cap / total_cap * 100:.0f}%;height:100%;background:#19D3F3;"></div>'
        f'</div>'
        '</div>'
    )

    # ── Unified table ──
    # Sort: pumped first by cap desc, then conventional by cap desc
    pumped_sorted = pumped.sort_values("fleet_capacity_mw", ascending=False)
    conv_sorted = conventional.sort_values("fleet_capacity_mw", ascending=False).head(25)
    max_cap = float(hydro_df["fleet_capacity_mw"].max()) if len(hydro_df) > 0 else 1

    html += '<div style="overflow-x:auto;padding:8px;">'
    html += '<table style="width:100%;border-collapse:collapse;font-size:12px;font-family:monospace;">'
    html += '<thead><tr>'
    for col, align, width in [
        ("PLANT", "left", "280px"), ("CAP MW", "right", "100px"),
        ("GEN (GWh)", "right", "100px"), ("CF %", "right", "120px"),
    ]:
        html += (
            f'<th style="padding:8px 10px;background:transparent;color:#6f8db1;'
            f'text-align:{align};font-size:10px;font-weight:600;letter-spacing:0.5px;'
            f'border-bottom:1px solid #253b59;min-width:{width};">{col}</th>'
        )
    html += '</tr></thead><tbody>'

    def _render_rows(df: pd.DataFrame, is_pumped: bool) -> str:
        rows_html = ""
        for _, row in df.iterrows():
            cap = float(row.get("fleet_capacity_mw", 0) or 0)
            gen = float(row.get("total_gen_gwh", 0) or 0)
            cf = float(row.get("capacity_factor_pct", 0) or 0)
            zone = str(row.get("pjm_zone", "") or "")
            state = str(row.get("state", ""))
            name = str(row.get("plant_name_eia", ""))

            if is_pumped:
                badge_text, badge_color = "PUMPED", "#19D3F3"
            else:
                badge_text, badge_color = "CONV", "#00CC96"

            gen_color = "#EF553B" if gen < 0 else "#a6bad6"
            cap_bar_pct = cap / max_cap * 100 if max_cap > 0 else 0

            # CF bar (only meaningful for conventional)
            if is_pumped:
                cf_cell = '<span style="color:#556;">\u2014</span>'
            else:
                cf_color = "#00CC96" if cf >= 30 else "#FFA15A" if cf >= 10 else "#556"
                cf_cell = (
                    f'<div style="display:flex;align-items:center;justify-content:flex-end;gap:6px;">'
                    f'<span style="color:{cf_color};min-width:40px;text-align:right;">{cf:.1f}%</span>'
                    f'<div style="width:50px;height:6px;background:#1a2d48;border-radius:3px;overflow:hidden;">'
                    f'<div style="width:{min(cf, 100):.0f}%;height:100%;background:{cf_color};border-radius:3px;"></div>'
                    f'</div></div>'
                )

            rows_html += (
                f'<tr>'
                # Plant with badge + zone + state
                f'<td style="padding:8px 10px;border-bottom:1px solid #1a2d48;text-align:left;">'
                f'<span style="background:{badge_color};color:#0a1628;padding:2px 6px;border-radius:3px;'
                f'font-size:9px;font-weight:800;margin-right:8px;">{badge_text}</span>'
                f'<span style="color:#dbe7ff;font-weight:600;">{name}</span>'
                f'<span style="color:#636EFA;font-size:10px;font-weight:600;margin-left:6px;">{zone}</span>'
                f'<span style="color:#556;font-size:10px;margin-left:4px;">{state}</span></td>'
                # Cap MW with bar
                f'<td style="padding:8px 10px;border-bottom:1px solid #1a2d48;text-align:right;">'
                f'<div style="display:flex;align-items:center;justify-content:flex-end;gap:6px;">'
                f'<div style="width:60px;height:6px;background:#1a2d48;border-radius:3px;overflow:hidden;">'
                f'<div style="width:{cap_bar_pct:.0f}%;height:100%;background:{badge_color};border-radius:3px;opacity:0.6;"></div>'
                f'</div>'
                f'<span style="color:#dbe7ff;font-weight:700;min-width:50px;text-align:right;">{cap:,.0f}</span>'
                f'</div></td>'
                # Gen
                f'<td style="padding:8px 10px;border-bottom:1px solid #1a2d48;text-align:right;color:{gen_color};font-weight:600;">{gen:,.1f}</td>'
                # CF
                f'<td style="padding:8px 10px;border-bottom:1px solid #1a2d48;text-align:right;">{cf_cell}</td>'
                f'</tr>'
            )
        return rows_html

    # Pumped storage rows
    if len(pumped_sorted) > 0:
        html += (
            f'<tr><td colspan="4" style="padding:10px 10px 4px 10px;border-bottom:none;'
            f'color:#19D3F3;font-size:10px;font-weight:700;letter-spacing:0.5px;">'
            f'PUMPED STORAGE ({len(pumped_sorted)} plants, {pump_cap:,.0f} MW)</td></tr>'
        )
        html += _render_rows(pumped_sorted, is_pumped=True)

    # Divider
    html += (
        f'<tr><td colspan="4" style="padding:0;height:2px;'
        f'background:linear-gradient(90deg,#253b59,#253b5940);border:none;"></td></tr>'
    )

    # Conventional rows
    if len(conv_sorted) > 0:
        html += (
            f'<tr><td colspan="4" style="padding:10px 10px 4px 10px;border-bottom:none;'
            f'color:#00CC96;font-size:10px;font-weight:700;letter-spacing:0.5px;">'
            f'CONVENTIONAL (top {len(conv_sorted)} by capacity, {conv_cap:,.0f} MW)</td></tr>'
        )
        html += _render_rows(conv_sorted, is_pumped=False)

    html += '</tbody></table></div>'
    return html


def _build_hydro_monthly_chart(
    df_mix: pd.DataFrame,
    hydro_df: pd.DataFrame,
) -> str:
    """Monthly average hydro MW: fuel mix vs EIA-923, split conv/pumped."""
    # ── Fuel mix: monthly average hydro MW ──
    df = df_mix.copy()
    df["date"] = pd.to_datetime(df["date"])

    if "hydro" not in df.columns:
        return '<div style="padding:14px;color:#f87171;">No hydro column in fuel mix data.</div>'

    df["month"] = df["date"].dt.to_period("M")
    fm_monthly = df.groupby("month").agg(
        hydro_avg_mw=("hydro", "mean"),
        storage_avg_mw=("storage", "mean") if "storage" in df.columns else ("hydro", lambda x: np.nan),
    ).reset_index()
    fm_monthly["month_start"] = fm_monthly["month"].dt.to_timestamp()

    # ── EIA-923: monthly generation → avg MW ──
    # hydro_df has per-plant annual data; we need monthly.
    # Re-derive monthly from the validation parquet's source data.
    # For now, compute fleet-level monthly avg MW from total_gen_gwh / 12 months.
    # Better: pull EIA monthly directly. But since we already have plant-level annual,
    # use the fleet total as a single annual reference bar.

    # Split EIA into conventional vs pumped
    conv = hydro_df[~hydro_df["is_pumped_storage"]]
    pump = hydro_df[hydro_df["is_pumped_storage"]]

    eia_conv_annual_avg_mw = float(conv["total_gen_gwh"].sum()) * 1000 / 8760 if len(conv) > 0 else 0
    eia_pump_annual_avg_mw = float(pump["total_gen_gwh"].sum()) * 1000 / 8760 if len(pump) > 0 else 0

    fig = go.Figure()

    # Fuel mix monthly hydro
    fig.add_trace(go.Bar(
        x=fm_monthly["month_start"], y=fm_monthly["hydro_avg_mw"] / 1000,
        name="Fuel Mix Hydro (monthly avg)",
        marker_color="#00CC96", opacity=0.7,
        hovertemplate="%{x|%b %Y}<br>Fuel Mix: %{y:.2f} GW<extra></extra>",
    ))

    # Fuel mix monthly storage (if available)
    if "storage" in df.columns:
        fig.add_trace(go.Bar(
            x=fm_monthly["month_start"], y=fm_monthly["storage_avg_mw"] / 1000,
            name="Fuel Mix Storage (monthly avg)",
            marker_color="#19D3F3", opacity=0.5,
            hovertemplate="%{x|%b %Y}<br>Storage: %{y:.2f} GW<extra></extra>",
        ))

    # EIA-923 conventional annual average as reference line
    if eia_conv_annual_avg_mw > 0:
        fig.add_hline(
            y=eia_conv_annual_avg_mw / 1000,
            line_color="#FFA15A", line_dash="dash", line_width=2,
            annotation_text=f"EIA Conv Avg: {eia_conv_annual_avg_mw / 1000:.2f} GW",
            annotation_position="top right",
            annotation_font_color="#FFA15A",
        )

    # EIA-923 pumped storage annual average
    if eia_pump_annual_avg_mw != 0:
        fig.add_hline(
            y=eia_pump_annual_avg_mw / 1000,
            line_color="#AB63FA", line_dash="dot", line_width=1.5,
            annotation_text=f"EIA Pumped Net Avg: {eia_pump_annual_avg_mw / 1000:.2f} GW",
            annotation_position="bottom right",
            annotation_font_color="#AB63FA",
        )

    # Fleet nameplate reference
    total_cap_gw = float(hydro_df["fleet_capacity_mw"].sum()) / 1000
    fig.add_hline(
        y=total_cap_gw, line_color="#636EFA", line_dash="dot", line_width=1,
        annotation_text=f"Nameplate: {total_cap_gw:.1f} GW",
        annotation_position="top left",
        annotation_font_color="#636EFA",
    )

    fig.update_layout(
        title="Hydro Monthly Avg Generation: Fuel Mix vs EIA-923",
        template=PLOTLY_TEMPLATE, height=420,
        margin=dict(l=60, r=40, t=55, b=40),
        yaxis_title="GW (avg MW / 1000)",
        legend=dict(orientation="h", yanchor="top", y=-0.10, x=0),
        hovermode="x unified",
        barmode="group",
    )
    fig.update_yaxes(autorange=True)

    return fig.to_html(include_plotlyjs="cdn", full_html=False)


# ══════════════════════════════════════════════════════════════════════
# Gas Fleet sections
# ══════════════════════════════════════════════════════════════════════


def _build_gas_table(gas_val: pd.DataFrame) -> str:
    """Gas plant table split CC/CT, matching nuclear/hydro style."""
    cc = gas_val[gas_val["fleet_fuel_type"] == "cc_gas"].copy()
    ct = gas_val[gas_val["fleet_fuel_type"] == "ct_gas"].copy()

    cc_cap = float(cc["fleet_capacity_mw"].sum())
    ct_cap = float(ct["fleet_capacity_mw"].sum())
    total_cap = cc_cap + ct_cap

    # Weighted avg heat rates
    def _wavg_hr(df):
        valid = df[df["cems_heat_rate"].notna() & (df["cems_heat_rate"] > 0) & (df["cems_heat_rate"] < 30)]
        if len(valid) == 0 or valid["total_gen_gwh"].sum() <= 0:
            return 0
        return float(np.average(valid["cems_heat_rate"], weights=valid["total_gen_gwh"]))

    cc_hr = _wavg_hr(cc)
    ct_hr = _wavg_hr(ct)
    cc_cf = float(cc["avg_mw"].sum() / cc_cap * 100) if cc_cap > 0 else 0
    ct_cf = float(ct["avg_mw"].sum() / ct_cap * 100) if ct_cap > 0 else 0

    # Header bar
    html = (
        '<div style="background:#0a1628;border:1px solid #253b59;border-radius:10px;'
        'padding:16px 20px;margin:8px;">'
        '<div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:4px;">'
        f'<div><span style="color:#636EFA;font-size:12px;font-weight:700;">PJM Gas Fleet (CEMS)</span>'
        f'<span style="color:#a6bad6;font-size:12px;margin-left:12px;">'
        f'{len(cc)} CC + {len(ct)} CT plants with CEMS data</span></div>'
        '</div>'
        '<div style="display:flex;justify-content:space-between;align-items:baseline;">'
        f'<div><span style="color:#dbe7ff;font-size:28px;font-weight:800;font-family:monospace;">'
        f'{total_cap / 1000:,.1f} GW</span>'
        f'<span style="color:#6f8db1;font-size:13px;margin-left:10px;">CEMS-covered capacity</span></div>'
        '</div>'
        '<div style="display:flex;gap:20px;margin-top:6px;">'
        f'<div><span style="color:#636EFA;font-weight:600;">CC:</span>'
        f'<span style="color:#dbe7ff;margin-left:4px;">{cc_cap / 1000:,.1f} GW</span>'
        f'<span style="color:#6f8db1;margin-left:4px;">CF {cc_cf:.0f}%</span>'
        f'<span style="color:#6f8db1;margin-left:4px;">HR {cc_hr:.2f}</span></div>'
        f'<div><span style="color:#FFA15A;font-weight:600;">CT:</span>'
        f'<span style="color:#dbe7ff;margin-left:4px;">{ct_cap / 1000:,.1f} GW</span>'
        f'<span style="color:#6f8db1;margin-left:4px;">CF {ct_cf:.0f}%</span>'
        f'<span style="color:#6f8db1;margin-left:4px;">HR {ct_hr:.2f}</span></div>'
        '</div>'
        # Capacity bar
        f'<div style="width:100%;height:8px;background:#1a2d48;border-radius:4px;margin-top:8px;overflow:hidden;display:flex;">'
        f'<div style="width:{cc_cap / total_cap * 100:.0f}%;height:100%;background:#636EFA;"></div>'
        f'<div style="width:{ct_cap / total_cap * 100:.0f}%;height:100%;background:#FFA15A;"></div>'
        f'</div>'
        '</div>'
    )

    # Table
    max_cap = float(gas_val["fleet_capacity_mw"].max()) if len(gas_val) > 0 else 1

    html += '<div style="overflow-x:auto;padding:8px;">'
    html += '<table style="width:100%;border-collapse:collapse;font-size:12px;font-family:monospace;">'
    html += '<thead><tr>'
    for col, align, width in [
        ("PLANT", "left", "280px"), ("CAP MW", "right", "90px"),
        ("CF %", "right", "110px"), ("CEMS HR", "right", "70px"),
        ("GEN (GWh)", "right", "80px"),
    ]:
        html += (
            f'<th style="padding:8px 10px;background:transparent;color:#6f8db1;'
            f'text-align:{align};font-size:10px;font-weight:600;letter-spacing:0.5px;'
            f'border-bottom:1px solid #253b59;min-width:{width};">{col}</th>'
        )
    html += '</tr></thead><tbody>'

    def _render_gas_rows(df: pd.DataFrame, fuel_type: str, label: str) -> str:
        if len(df) == 0:
            return ""
        badge_color = "#636EFA" if fuel_type == "cc_gas" else "#FFA15A"
        badge_text = "CC" if fuel_type == "cc_gas" else "CT"
        total_mw = df["fleet_capacity_mw"].sum()

        s = (
            f'<tr><td colspan="5" style="padding:10px 10px 4px 10px;border-bottom:none;'
            f'color:{badge_color};font-size:10px;font-weight:700;letter-spacing:0.5px;">'
            f'{label} ({len(df)} plants, {total_mw:,.0f} MW)</td></tr>'
        )
        for _, row in df.head(15).iterrows():
            cap = float(row.get("fleet_capacity_mw", 0) or 0)
            cf = float(row.get("capacity_factor_pct", 0) or 0)
            hr = row.get("cems_heat_rate")
            gen = float(row.get("total_gen_gwh", 0) or 0)
            zone = str(row.get("pjm_zone", "") or "")
            state = str(row.get("state", "") or "")
            name = str(row.get("plant_name", "") or "")
            cap_bar = cap / max_cap * 100 if max_cap > 0 else 0

            cf_color = "#00CC96" if cf >= 40 else "#FFA15A" if cf >= 15 else "#556"
            hr_str = f"{hr:.2f}" if pd.notna(hr) and 0 < hr < 30 else "\u2014"
            hr_color = "#00CC96" if pd.notna(hr) and hr < 7.5 else "#FFA15A" if pd.notna(hr) and hr < 10 else "#EF553B" if pd.notna(hr) else "#556"

            s += (
                f'<tr>'
                f'<td style="padding:8px 10px;border-bottom:1px solid #1a2d48;text-align:left;">'
                f'<span style="background:{badge_color};color:#0a1628;padding:2px 6px;border-radius:3px;'
                f'font-size:9px;font-weight:800;margin-right:8px;">{badge_text}</span>'
                f'<span style="color:#dbe7ff;font-weight:600;">{name}</span>'
                f'<span style="color:#636EFA;font-size:10px;font-weight:600;margin-left:6px;">{zone}</span>'
                f'<span style="color:#556;font-size:10px;margin-left:4px;">{state}</span></td>'
                f'<td style="padding:8px 10px;border-bottom:1px solid #1a2d48;text-align:right;">'
                f'<div style="display:flex;align-items:center;justify-content:flex-end;gap:6px;">'
                f'<div style="width:50px;height:6px;background:#1a2d48;border-radius:3px;overflow:hidden;">'
                f'<div style="width:{cap_bar:.0f}%;height:100%;background:{badge_color};border-radius:3px;opacity:0.5;"></div>'
                f'</div>'
                f'<span style="color:#dbe7ff;font-weight:700;min-width:50px;text-align:right;">{cap:,.0f}</span>'
                f'</div></td>'
                f'<td style="padding:8px 10px;border-bottom:1px solid #1a2d48;text-align:right;">'
                f'<div style="display:flex;align-items:center;justify-content:flex-end;gap:6px;">'
                f'<span style="color:{cf_color};min-width:35px;text-align:right;">{cf:.1f}%</span>'
                f'<div style="width:40px;height:6px;background:#1a2d48;border-radius:3px;overflow:hidden;">'
                f'<div style="width:{min(cf, 100):.0f}%;height:100%;background:{cf_color};border-radius:3px;"></div>'
                f'</div></div></td>'
                f'<td style="padding:8px 10px;border-bottom:1px solid #1a2d48;text-align:right;color:{hr_color};font-weight:600;">{hr_str}</td>'
                f'<td style="padding:8px 10px;border-bottom:1px solid #1a2d48;text-align:right;color:#a6bad6;">{gen:,.0f}</td>'
                f'</tr>'
            )
        return s

    html += _render_gas_rows(cc, "cc_gas", "COMBINED CYCLE")
    html += (
        f'<tr><td colspan="5" style="padding:0;height:2px;'
        f'background:linear-gradient(90deg,#253b59,#253b5940);border:none;"></td></tr>'
    )
    html += _render_gas_rows(ct, "ct_gas", "COMBUSTION TURBINE")
    html += '</tbody></table></div>'
    return html


def _build_gas_monthly_chart(gas_monthly: pd.DataFrame, df_mix: pd.DataFrame) -> str:
    """Monthly CEMS gas generation (CC+CT) vs fuel mix gas column."""
    # CEMS monthly by type
    fig = go.Figure()

    for fuel, color, name in [("cc_gas", "#636EFA", "CC (CEMS)"), ("ct_gas", "#FFA15A", "CT (CEMS)")]:
        sub = gas_monthly[gas_monthly["fleet_fuel_type"] == fuel].sort_values("month_start")
        if len(sub) > 0:
            fig.add_trace(go.Bar(
                x=sub["month_start"], y=sub["avg_mw"] / 1000,
                name=name, marker_color=color, opacity=0.7,
                hovertemplate=f"%{{x|%b %Y}}<br>{name}: %{{y:.1f}} GW<extra></extra>",
            ))

    # Fuel mix monthly gas
    df = df_mix.copy()
    df["date"] = pd.to_datetime(df["date"])
    if "gas" in df.columns:
        fm_monthly = df.groupby(df["date"].dt.to_period("M"))["gas"].mean().reset_index()
        fm_monthly["month_start"] = fm_monthly["date"].dt.to_timestamp()
        fig.add_trace(go.Scatter(
            x=fm_monthly["month_start"], y=fm_monthly["gas"] / 1000,
            mode="lines+markers", name="Fuel Mix Gas (monthly avg)",
            line=dict(color="#4cc9f0", width=2.5), marker=dict(size=5),
            hovertemplate="%{x|%b %Y}<br>Fuel Mix: %{y:.1f} GW<extra></extra>",
        ))

    # CEMS monthly implied heat rate as secondary trace
    cc_monthly = gas_monthly[gas_monthly["fleet_fuel_type"] == "cc_gas"].sort_values("month_start")
    ct_monthly = gas_monthly[gas_monthly["fleet_fuel_type"] == "ct_gas"].sort_values("month_start")

    fig.update_layout(
        title="Gas Monthly: CEMS Generation (CC+CT) vs Fuel Mix",
        template=PLOTLY_TEMPLATE, height=420,
        margin=dict(l=60, r=40, t=55, b=40),
        yaxis_title="GW (avg)",
        legend=dict(orientation="h", yanchor="top", y=-0.10, x=0),
        hovermode="x unified",
        barmode="stack",
    )
    fig.update_yaxes(autorange=True)

    return fig.to_html(include_plotlyjs="cdn", full_html=False)


# ══════════════════════════════════════════════════════════════════════
# Table helpers
# ══════════════════════════════════════════════════════════════════════


def _tbl_row(
    metric: str,
    unit: str,
    values: pd.Series,
    signed: bool = False,
    sign_colors: bool = False,
    decimals: int = 0,
) -> str:
    s = values.copy()
    s.index = range(1, len(s) + 1)
    html = f'<tr><td class="metric">{metric}</td><td class="unit">{unit}</td>'
    for h in range(1, 25):
        v = s.get(h, pd.NA)
        cls = _cell_class(v, sign_colors)
        html += f'<td class="{cls}">{_fmt(v, signed, decimals)}</td>'

    for hours in [ONPEAK_HOURS, OFFPEAK_HOURS, list(range(1, 25))]:
        vals = pd.to_numeric(s.reindex(hours), errors="coerce").dropna()
        v = float(vals.mean()) if not vals.empty else pd.NA
        cls = _cell_class(v, sign_colors)
        html += f'<td class="{cls}">{_fmt(v, signed, decimals)}</td>'

    html += "</tr>"
    return html


def _tbl_body(component_rows: list[str], net_rows: list[str]) -> str:
    divider = (
        f'<tr><td colspan="{2 + 24 + 3}" style="padding:0;height:3px;'
        f'background:linear-gradient(90deg,#4a6a8a,#4a6a8a60);border:none;"></td></tr>'
    )
    return "".join(component_rows) + divider + "".join(net_rows)


def _cell_class(val, sign_colors: bool) -> str:
    if not sign_colors or pd.isna(val):
        return ""
    return "pos" if val > 0 else "neg" if val < 0 else "zero"


def _fmt(val, signed: bool = False, decimals: int = 0) -> str:
    if pd.isna(val):
        return "\u2014"
    if decimals > 0:
        return f"{float(val):+,.{decimals}f}" if signed else f"{float(val):,.{decimals}f}"
    return f"{float(val):+,.0f}" if signed else f"{float(val):,.0f}"


# ══════════════════════════════════════════════════════════════════════
# CSS
# ══════════════════════════════════════════════════════════════════════


_STYLE = """
<style>
.ss-wrap { padding: 8px; }
.ss-meta { margin-bottom: 10px; color: #9eb4d3; font-size: 11px; font-family: monospace; }
.ss-tw { overflow-x: auto; border: 1px solid #2a3f60; border-radius: 8px; }
.ss-t {
  width: 100%; border-collapse: collapse;
  font-size: 11px; font-family: monospace;
}
.ss-t th {
  position: sticky; top: 0; background: #16263d; color: #e6efff;
  border-bottom: 1px solid #2a3f60; padding: 6px 8px;
  text-align: right; white-space: nowrap;
}
.ss-t th.metric, .ss-t th.unit { text-align: left; }
.ss-t td {
  padding: 5px 8px; border-bottom: 1px solid #1f334f;
  text-align: right; color: #dbe7ff; white-space: nowrap;
}
.ss-t td.metric { text-align: left; color: #cfe0ff; font-weight: 700; }
.ss-t td.unit { text-align: left; color: #8aa5ca; }
.ss-t tr:nth-child(even) td { background: rgba(18, 32, 50, 0.45); }
.ss-t td.pos { color: #34d399; }
.ss-t td.neg { color: #f87171; }
.ss-t td.zero { color: #9eb4d3; }
.ss-toggle {
  padding: 4px 12px; font-size: 11px; font-weight: 600;
  background: #101d31; color: #9eb4d3; border: 1px solid #2a3f60;
  border-radius: 4px; cursor: pointer; font-family: inherit;
  margin-bottom: 6px;
}
.ss-toggle:hover { background: #1a2b44; color: #dbe7ff; }
</style>
"""


# ══════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate supply stack input data validation report",
    )
    parser.add_argument(
        "--date", dest="forecast_date", default=None,
        help="Delivery date (YYYY-MM-DD). Defaults to tomorrow.",
    )
    parser.add_argument(
        "--region", default="RTO",
        help="PJM region (used when --region-preset is not provided).",
    )
    parser.add_argument(
        "--region-preset", choices=sorted(REGION_PRESETS), default=None,
        help="Preconfigured zonal scope: rto, south, dominion.",
    )
    parser.add_argument(
        "--gas-hub", dest="gas_hub_col", default=None,
        help="Gas hub override column name.",
    )
    parser.add_argument(
        "--output-dir", default=None, type=Path,
        help=f"Output directory (default: {REPORT_OUTPUT_DIR})",
    )
    return parser.parse_args()


def main():
    import src.settings  # noqa: F401

    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    path = generate(
        forecast_date=args.forecast_date,
        region=args.region,
        region_preset=args.region_preset,
        gas_hub_col=args.gas_hub_col,
        output_dir=args.output_dir,
    )
    print(f"Report saved to: {path}")


if __name__ == "__main__":
    main()
