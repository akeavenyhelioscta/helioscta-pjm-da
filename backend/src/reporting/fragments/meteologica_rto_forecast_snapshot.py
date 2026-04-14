"""Meteologica RTO Forecast Snapshot.

Sections:
  1. All-dates summary table (Load, Wind, Solar, Net Load for every available date)
  2. Per-date sections — each date is one section containing:
     a. Table with Load/Wind/Solar + divider + Net Load (JS toggle: outright / ramp)
     b. Row 1: Load, Solar, Wind charts (each with outright/ramp toggle, all vintages)
     c. Row 2: Net Load profile (with solar+wind overlay) + Net Load ramp
"""

from __future__ import annotations

import json
import logging
from datetime import timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.data import (
    load_forecast_vintages,
    meteologica_generation_forecast_hourly,
)
from src.like_day_forecast import configs
from src.reporting.fragments.like_day_forecast_chart_utils import (
    build_vintage_chart,
)
from src.utils.cache_utils import pull_with_cache

logger = logging.getLogger(__name__)

PLOTLY_TEMPLATE = "plotly_dark"
ET = ZoneInfo("America/New_York")
HE_COLS = [f"HE{h}" for h in range(1, 25)]
ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = [h for h in range(1, 25) if h not in ONPEAK_HOURS]
SUMMARY_COLS = ["OnPeak", "OffPeak", "Flat"]
CUTOFF_PRIORITY = ["DA Cutoff", "DA -12h", "DA -24h", "DA -48h"]

VINTAGE_COLORS = {
    "Latest": "#60a5fa",
    "DA Cutoff": "#f87171",
    "DA -12h": "#a78bfa",
    "DA -24h": "#34d399",
    "DA -48h": "#fbbf24",
}

Section = tuple[str, Any, str | None]

PLOTLY_LOCKED_CONFIG = {
    "displaylogo": False,
    "scrollZoom": False,
    "doubleClick": False,
    "modeBarButtonsToRemove": [
        "zoom2d",
        "pan2d",
        "select2d",
        "lasso2d",
        "zoomIn2d",
        "zoomOut2d",
        "autoScale2d",
        "resetScale2d",
    ],
}


# ══════════════════════════════════════════════════════════════════════
# Public entry point
# ══════════════════════════════════════════════════════════════════════


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list[Section]:
    del schema

    logger.info("Building Meteologica RTO forecast snapshot fragments...")

    ck = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    load_raw = _pull_load(**ck)
    solar_raw = _pull_generation(kind="solar", **ck)
    wind_raw = _pull_generation(kind="wind", **ck)

    if load_raw.empty or solar_raw.empty or wind_raw.empty:
        return [("Meteologica RTO Forecast Snapshot", _empty_html("Missing load/solar/wind forecast data."), None)]

    cutoff_label = _pick_cutoff_label(load_raw, solar_raw, wind_raw)
    latest_df = _build_hourly_frame(load_raw, solar_raw, wind_raw, "Latest")
    cutoff_df = _build_hourly_frame(load_raw, solar_raw, wind_raw, cutoff_label) if cutoff_label else pd.DataFrame()

    if latest_df.empty:
        return [("Meteologica RTO Forecast Snapshot", _empty_html("No hourly rows available."), None)]

    available_dates = sorted(latest_df["forecast_date"].unique())
    if not available_dates:
        return [("Meteologica RTO Forecast Snapshot", _empty_html("No forecast dates available."), None)]

    meta = _build_meta_line(cutoff_label, latest_df, cutoff_df)

    sections: list[Section] = []

    # Section 1: All Forecast Dates — single section with both charts
    all_fcst_html = _render_all_forecasts(load_raw, solar_raw, wind_raw, available_dates)
    sections.append(("All Forecast Dates", all_fcst_html, None))

    # Per-date sections with Day +N label
    today = (pd.Timestamp.now(ET)).date()
    for dt in available_dates:
        day_offset = (dt - today).days
        dt_label = pd.Timestamp(dt).strftime("%a %b %d")
        section_name = f"{dt_label} (Day +{day_offset})" if day_offset >= 0 else f"{dt_label} (Day {day_offset})"

        latest_day = _slice_day(latest_df, dt)
        cutoff_day = _slice_day(cutoff_df, dt) if not cutoff_df.empty else pd.DataFrame()
        dt_key = _date_key(dt)

        # Extract per-component vintage series for this date
        load_vintages = _component_vintages(load_raw, dt)
        solar_vintages = _component_vintages(solar_raw, dt)
        wind_vintages = _component_vintages(wind_raw, dt)

        content = _STYLE
        content += _render_snapshot_table(latest_day, meta, dt_key)
        content += _render_component_charts_row(load_vintages, solar_vintages, wind_vintages, dt_key)
        content += _render_net_load_row(latest_day, cutoff_day, cutoff_label, dt_key)

        sections.append((section_name, content, None))

    return sections


# ══════════════════════════════════════════════════════════════════════
# Regional entry points (full load/solar/wind for Meteologica regions)
# ══════════════════════════════════════════════════════════════════════

_REGION_LABELS = {"WEST": "Western", "MIDATL": "Mid-Atlantic", "SOUTH": "Southern"}


def _build_fragments_for_region(
    region: str,
    region_label: str,
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list[Section]:
    """Build full forecast snapshot for a Meteologica region."""
    del schema

    logger.info(f"Building Meteologica {region_label} forecast snapshot fragments...")

    ck = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    load_raw = _pull_load(region=region, **ck)
    solar_raw = _pull_generation(kind="solar", region=region, **ck)
    wind_raw = _pull_generation(kind="wind", region=region, **ck)

    if load_raw.empty and solar_raw.empty and wind_raw.empty:
        return [(f"Meteologica {region_label} Forecast Snapshot",
                 _empty_html(f"No forecast data for {region_label}."), None)]

    has_all = not (load_raw.empty or solar_raw.empty or wind_raw.empty)
    cutoff_label = _pick_cutoff_label(load_raw, solar_raw, wind_raw) if has_all else None
    latest_df = _build_hourly_frame(load_raw, solar_raw, wind_raw, "Latest")
    cutoff_df = (
        _build_hourly_frame(load_raw, solar_raw, wind_raw, cutoff_label)
        if cutoff_label else pd.DataFrame()
    )

    if latest_df.empty:
        return [(f"Meteologica {region_label} Forecast Snapshot",
                 _empty_html(f"No hourly rows for {region_label}."), None)]

    available_dates = sorted(latest_df["forecast_date"].unique())
    if not available_dates:
        return [(f"Meteologica {region_label} Forecast Snapshot",
                 _empty_html("No forecast dates available."), None)]

    meta = _build_meta_line(cutoff_label, latest_df, cutoff_df)

    sections: list[Section] = []

    # Section 1: All Forecast Dates — load vintage chart + net load area
    all_fcst_html = _render_all_forecasts_regional(
        load_raw, solar_raw, wind_raw, available_dates, region, region_label,
    )
    sections.append(("All Forecast Dates", all_fcst_html, None))

    # Per-date sections with Day +N label
    today = (pd.Timestamp.now(ET)).date()
    for dt in available_dates:
        day_offset = (dt - today).days
        dt_label = pd.Timestamp(dt).strftime("%a %b %d")
        section_name = (f"{dt_label} (Day +{day_offset})"
                        if day_offset >= 0
                        else f"{dt_label} (Day {day_offset})")

        latest_day = _slice_day(latest_df, dt)
        cutoff_day = _slice_day(cutoff_df, dt) if not cutoff_df.empty else pd.DataFrame()
        dt_key = f"{region.lower()}-{_date_key(dt)}"

        load_vintages = _component_vintages(load_raw, dt)
        solar_vintages = _component_vintages(solar_raw, dt)
        wind_vintages = _component_vintages(wind_raw, dt)

        content = _STYLE
        content += _render_snapshot_table(latest_day, meta, dt_key)
        content += _render_component_charts_row(load_vintages, solar_vintages, wind_vintages, dt_key)
        content += _render_net_load_row(latest_day, cutoff_day, cutoff_label, dt_key)

        sections.append((section_name, content, None))

    return sections


def build_fragments_west(**kwargs) -> list[Section]:
    return _build_fragments_for_region("WEST", "Western", **kwargs)


def build_fragments_midatl(**kwargs) -> list[Section]:
    return _build_fragments_for_region("MIDATL", "Mid-Atlantic", **kwargs)


def build_fragments_south(**kwargs) -> list[Section]:
    return _build_fragments_for_region("SOUTH", "Southern", **kwargs)


# ══════════════════════════════════════════════════════════════════════
# All Forecasts — single net load overlay, one line per date
# ══════════════════════════════════════════════════════════════════════


_AF_PREFIX = "meteoAllFcst"


def _render_all_forecasts(
    load_raw: pd.DataFrame,
    solar_raw: pd.DataFrame,
    wind_raw: pd.DataFrame,
    dates: list,
) -> str:
    """Single HTML block: Load vintage chart + net load stacked area with ramp."""
    html = ""

    # ── Load forecast vintage chart (no date pills / vintage toggles) ──
    load_chart_id = f"{_AF_PREFIX}Load"
    load_chart_df = load_raw.copy()
    load_chart_df["forecast_date"] = pd.to_datetime(load_chart_df["forecast_date"])
    if not load_chart_df.empty:
        html += build_vintage_chart(
            chart_id=load_chart_id,
            df=load_chart_df,
            title="Load Forecast — RTO (Meteologica)",
            value_col="forecast_mw",
            y_title="MW",
            prefix=_AF_PREFIX,
        )

    # ── Net load stacked area + ramp — all dates on datetime x-axis ──
    html += _build_net_load_area_chart(load_raw, solar_raw, wind_raw, dates, div_id="meteo-all-netload")

    return html


def _render_all_forecasts_regional(
    load_raw: pd.DataFrame,
    solar_raw: pd.DataFrame,
    wind_raw: pd.DataFrame,
    dates: list,
    region: str,
    region_label: str,
) -> str:
    """All-dates overview for a regional snapshot: load vintage + net load area."""
    html = ""
    af_prefix = f"meteo{region}AllFcst"

    load_chart_df = load_raw.copy()
    load_chart_df["forecast_date"] = pd.to_datetime(load_chart_df["forecast_date"])
    if not load_chart_df.empty:
        html += build_vintage_chart(
            chart_id=f"{af_prefix}Load",
            df=load_chart_df,
            title=f"Load Forecast — {region_label} (Meteologica)",
            value_col="forecast_mw",
            y_title="MW",
            prefix=af_prefix,
        )

    html += _build_net_load_area_chart(
        load_raw, solar_raw, wind_raw, dates,
        div_id=f"meteo-{region.lower()}-all-netload",
    )
    return html


def _build_net_load_area_chart(
    load_raw: pd.DataFrame,
    solar_raw: pd.DataFrame,
    wind_raw: pd.DataFrame,
    dates: list,
    div_id: str = "meteo-all-netload",
) -> str:
    """Stacked area (left) + grouped ramp bars (right), datetime x-axis."""
    rows = []
    for dt in dates:
        load_s = _day_latest_series(load_raw, dt)
        solar_s = _day_latest_series(solar_raw, dt)
        wind_s = _day_latest_series(wind_raw, dt)
        if load_s.empty:
            continue
        for h in range(1, 25):
            ld = load_s.get(h)
            sl = solar_s.get(h)
            wn = wind_s.get(h)
            if pd.isna(ld) or pd.isna(sl) or pd.isna(wn):
                continue
            rows.append({
                "datetime": pd.Timestamp(dt) + pd.Timedelta(hours=h),
                "date_label": pd.Timestamp(dt).strftime("%a %b-%d"),
                "he": h,
                "load": ld,
                "solar": sl,
                "wind": wn,
                "net_load": ld - sl - wn,
            })

    if not rows:
        return _empty_html("No net load data.")

    df = pd.DataFrame(rows).sort_values("datetime")

    # Compute ramps
    df["load_ramp"] = df["load"].diff()
    df["solar_ramp"] = df["solar"].diff()
    df["wind_ramp"] = df["wind"].diff()
    df["net_load_ramp"] = df["net_load"].diff()
    # NaN out first hour of each day (ramp across midnight is meaningless)
    day_boundaries = df["date_label"] != df["date_label"].shift(1)
    for col in ["load_ramp", "solar_ramp", "wind_ramp", "net_load_ramp"]:
        df.loc[day_boundaries, col] = None

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("Net Load Breakdown", "Hourly Ramp"),
        horizontal_spacing=0.06,
        column_widths=[0.55, 0.45],
    )

    cd = df[["date_label", "he"]].values

    # ── Left: Stacked area ───────────────────────────────────────
    fig.add_trace(go.Scatter(
        x=df["datetime"], y=df["net_load"],
        mode="lines", name="Net Load",
        stackgroup="stack",
        line=dict(color="#60a5fa", width=1),
        fillcolor="rgba(96, 165, 250, 0.50)",
        customdata=cd,
        hovertemplate="<b>%{customdata[0]}</b> HE %{customdata[1]}<br>Net Load: %{y:,.0f} MW<extra></extra>",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=df["datetime"], y=df["solar"],
        mode="lines", name="Solar",
        stackgroup="stack",
        line=dict(color="#fbbf24", width=1),
        fillcolor="rgba(251, 191, 36, 0.40)",
        customdata=cd,
        hovertemplate="<b>%{customdata[0]}</b> HE %{customdata[1]}<br>Solar: %{y:,.0f} MW<extra></extra>",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=df["datetime"], y=df["wind"],
        mode="lines", name="Wind",
        stackgroup="stack",
        line=dict(color="#34d399", width=1),
        fillcolor="rgba(52, 211, 153, 0.35)",
        customdata=cd,
        hovertemplate="<b>%{customdata[0]}</b> HE %{customdata[1]}<br>Wind: %{y:,.0f} MW<extra></extra>",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=df["datetime"], y=df["load"],
        mode="lines", name="Load",
        line=dict(color="#f8fafc", width=2),
        customdata=cd,
        hovertemplate="<b>%{customdata[0]}</b> HE %{customdata[1]}<br>Load: %{y:,.0f} MW<extra></extra>",
    ), row=1, col=1)

    # ── Right: Grouped ramp bars ─────────────────────────────────
    ramp_traces = [
        ("Load Ramp", "load_ramp", "#f8fafc"),
        ("Net Load Ramp", "net_load_ramp", "#60a5fa"),
        ("Solar Ramp", "solar_ramp", "#fbbf24"),
        ("Wind Ramp", "wind_ramp", "#34d399"),
    ]
    for name, col, color in ramp_traces:
        fig.add_trace(go.Bar(
            x=df["datetime"], y=df[col],
            name=name,
            marker_color=color,
            opacity=0.8,
            customdata=cd,
            legend="legend2",
            hovertemplate=f"<b>%{{customdata[0]}}</b> HE %{{customdata[1]}}<br>{name}: %{{y:+,.0f}} MW/hr<extra></extra>",
        ), row=1, col=2)

    fig.add_hline(y=0, line_color="#7f8ea3", line_dash="dash", line_width=1, row=1, col=2)

    fig.update_layout(
        height=500,
        template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="top", y=-0.08, xanchor="left", x=0),
        legend2=dict(orientation="h", yanchor="top", y=-0.08, xanchor="left", x=0.55),
        margin=dict(l=60, r=40, t=40, b=60),
        hovermode="x unified",
        barmode="group",
        bargap=0.15,
    )
    fig.update_xaxes(tickformat="%a %b-%d %I %p", gridcolor="rgba(99,110,250,0.08)")
    fig.update_yaxes(title_text="MW", tickformat=".1s", gridcolor="rgba(99,110,250,0.1)", col=1)
    fig.update_yaxes(title_text="MW/hr", tickformat=".1s", gridcolor="rgba(99,110,250,0.1)", col=2)

    return fig.to_html(include_plotlyjs="cdn", full_html=False, div_id=div_id)


def _day_latest_series(raw_df: pd.DataFrame, target_date) -> pd.Series:
    """Extract Latest vintage hourly series for a single date."""
    if raw_df.empty:
        return pd.Series(dtype=float)
    sub = raw_df[(raw_df["forecast_date"] == target_date) & (raw_df["vintage_label"] == "Latest")]
    if sub.empty:
        return pd.Series(dtype=float)
    sub = sub.drop_duplicates("hour_ending", keep="last")
    return sub.set_index("hour_ending")["forecast_mw"].sort_index()


# ══════════════════════════════════════════════════════════════════════
# Snapshot table with outright/ramp toggle
# ══════════════════════════════════════════════════════════════════════


def _render_snapshot_table(day_df: pd.DataFrame, meta: str, dt_key: str) -> str:
    if day_df.empty:
        return _empty_html("No rows.")

    tid = f"ms-tbl-{dt_key}"

    # Outright rows
    outright_rows = [
        _tbl_row("Load", "MW", day_df["load_mw"]),
        _tbl_row("Wind", "MW", day_df["wind_mw"]),
        _tbl_row("Solar", "MW", day_df["solar_mw"]),
    ]
    outright_net = [
        _tbl_row("Net Load", "MW", day_df["net_load_mw"]),
    ]

    # Ramp rows
    ramp_rows = [
        _tbl_row("Load Ramp", "MW/hr", day_df["load_mw"].diff(), signed=True, sign_colors=True),
        _tbl_row("Wind Ramp", "MW/hr", day_df["wind_mw"].diff(), signed=True, sign_colors=True),
        _tbl_row("Solar Ramp", "MW/hr", day_df["solar_mw"].diff(), signed=True, sign_colors=True),
    ]
    ramp_net = [
        _tbl_row("Net Load Ramp", "MW/hr", day_df["net_load_mw"].diff(), signed=True, sign_colors=True),
    ]

    # Build both table bodies
    outright_body = _tbl_body(outright_rows, outright_net)
    ramp_body = _tbl_body(ramp_rows, ramp_net)

    cols = ["Metric", "Unit"] + HE_COLS + SUMMARY_COLS
    header = '<thead><tr>'
    for col in cols:
        cls = ' class="metric"' if col == "Metric" else ' class="unit"' if col == "Unit" else ""
        header += f'<th{cls}>{col}</th>'
    header += '</tr></thead>'

    toggle_btn = (
        f'<button class="ms-toggle" onclick="msToggle(\'{tid}\')" id="{tid}-btn">Show Ramp</button>'
    )

    return (
        f'<div class="ms-wrap"><div class="ms-meta">{meta}</div>'
        f'{toggle_btn}'
        f'<div class="ms-tw"><table class="ms-t" id="{tid}">{header}'
        f'<tbody class="ms-tbody-active" id="{tid}-outright">{outright_body}</tbody>'
        f'<tbody style="display:none;" id="{tid}-ramp">{ramp_body}</tbody>'
        f'</table></div></div>'
        f'''<script>
function msToggle(tid) {{
  var o = document.getElementById(tid + '-outright');
  var r = document.getElementById(tid + '-ramp');
  var b = document.getElementById(tid + '-btn');
  if (o.style.display === 'none') {{
    o.style.display = ''; r.style.display = 'none'; b.textContent = 'Show Ramp';
  }} else {{
    o.style.display = 'none'; r.style.display = ''; b.textContent = 'Show Outright';
  }}
}}
</script>'''
    )


def _tbl_row(metric: str, unit: str, values: pd.Series, signed: bool = False, sign_colors: bool = False) -> str:
    s = values.copy()
    s.index = range(1, len(s) + 1)
    html = f'<tr><td class="metric">{metric}</td><td class="unit">{unit}</td>'
    for h in range(1, 25):
        v = s.get(h, pd.NA)
        cls = _cell_class(v, sign_colors)
        html += f'<td class="{cls}">{_fmt(v, signed)}</td>'

    # Summaries
    for hours in [ONPEAK_HOURS, OFFPEAK_HOURS, list(range(1, 25))]:
        vals = pd.to_numeric(s.reindex(hours), errors="coerce").dropna()
        v = float(vals.mean()) if not vals.empty else pd.NA
        cls = _cell_class(v, sign_colors)
        html += f'<td class="{cls}">{_fmt(v, signed)}</td>'

    html += '</tr>'
    return html


def _tbl_body(component_rows: list[str], net_rows: list[str]) -> str:
    """Component rows + divider + net load rows."""
    divider = (
        f'<tr><td colspan="{2 + 24 + 3}" style="padding:0;height:3px;'
        f'background:linear-gradient(90deg,#4a6a8a,#4a6a8a60);border:none;"></td></tr>'
    )
    return "".join(component_rows) + divider + "".join(net_rows)


# ══════════════════════════════════════════════════════════════════════
# Row 1: Load / Solar / Wind component charts
# ══════════════════════════════════════════════════════════════════════


def _component_vintages(raw_df: pd.DataFrame, target_date) -> dict[str, pd.Series]:
    """Extract {vintage_label: Series(hour_ending -> MW)} for a given date."""
    if raw_df.empty:
        return {}
    day = raw_df[raw_df["forecast_date"] == target_date]
    out: dict[str, pd.Series] = {}
    for label in day["vintage_label"].unique():
        sub = day[day["vintage_label"] == label].drop_duplicates("hour_ending", keep="last")
        out[label] = sub.set_index("hour_ending")["forecast_mw"].sort_index()
    return out


def _render_component_charts_row(
    load_v: dict[str, pd.Series],
    solar_v: dict[str, pd.Series],
    wind_v: dict[str, pd.Series],
    dt_key: str,
) -> str:
    """Three subplots side by side: Load, Solar, Wind — with outright/ramp toggle.

    Uses a single ``make_subplots`` figure so Plotly handles column layout
    internally, avoiding flex-container sizing issues that can truncate charts.

    Meteologica has full vintage support for all three components, so all
    vintages are shown in every subplot (unlike PJM which only has Latest
    for solar/wind).
    """
    chart_id = f"ms-comp-{dt_key}"

    fig = make_subplots(
        rows=1, cols=3,
        subplot_titles=("Load Forecast", "Solar Forecast", "Wind Forecast"),
        horizontal_spacing=0.06,
    )

    hours = list(range(1, 25))
    components = [
        ("Load", load_v, 1),
        ("Solar", solar_v, 2),
        ("Wind", wind_v, 3),
    ]

    # Track trace indices for the toggle JS
    all_outright: list[int] = []
    all_ramp: list[int] = []
    # Indices that carry a legend entry (first subplot / Load only)
    legend_outright: list[int] = []
    legend_ramp: list[int] = []

    seen_vints: set[str] = set()  # track which vintages already have a legend entry

    for comp_label, vintages, col in components:
        vintage_order = [v for v in ["Latest"] + CUTOFF_PRIORITY if v in vintages]

        for vint in vintage_order:
            s = vintages[vint].reindex(hours)
            color = VINTAGE_COLORS.get(vint, "#94a3b8")
            ramp = s.diff()
            first_for_vint = vint not in seen_vints
            seen_vints.add(vint)

            # Outright trace — visible
            idx = len(fig.data)
            all_outright.append(idx)
            if first_for_vint:
                legend_outright.append(idx)
            fig.add_trace(go.Scatter(
                x=hours, y=s.values,
                mode="lines+markers", name=vint,
                legendgroup=vint,
                line=dict(color=color, width=2.2 if vint == "Latest" else 1.5,
                          dash="solid" if vint == "Latest" else "dash"),
                marker=dict(size=4 if vint == "Latest" else 3),
                showlegend=first_for_vint,
                hovertemplate=f"{vint}<br>HE %{{x}}<br>%{{y:,.0f}} MW<extra></extra>",
            ), row=1, col=col)

            # Ramp trace — hidden initially
            idx = len(fig.data)
            all_ramp.append(idx)
            if first_for_vint:
                legend_ramp.append(idx)
            fig.add_trace(go.Bar(
                x=hours, y=ramp.values,
                name=vint,
                legendgroup=vint,
                marker_color=color,
                opacity=0.8,
                visible=False,
                showlegend=False,
                hovertemplate=f"{vint}<br>HE %{{x}}<br>%{{y:+,.0f}} MW/hr<extra></extra>",
            ), row=1, col=col)

    fig.update_layout(
        template=PLOTLY_TEMPLATE,
        height=400,
        margin=dict(l=50, r=20, t=80, b=40),
        legend=dict(font=dict(size=9), orientation="h", yanchor="bottom", y=1.08, x=0),
        hovermode="x unified",
        barmode="group",
    )
    for col in range(1, 4):
        fig.update_xaxes(
            dtick=1,
            range=[0.5, 24.5],
            autorange=False,
            fixedrange=True,
            title_text="Hour Ending",
            row=1, col=col,
        )
        fig.update_yaxes(title_text="MW", row=1, col=col)

    chart_html = fig.to_html(
        include_plotlyjs="cdn",
        full_html=False,
        div_id=chart_id,
        config=PLOTLY_LOCKED_CONFIG,
    )

    js_key = dt_key.replace("-", "_")
    btn_id = f"{chart_id}-ramp-btn"
    toggle_btn = (
        f'<button class="ms-toggle" onclick="msCompToggle_{js_key}()"'
        f' id="{btn_id}">Show Ramp</button>'
    )

    html = f'<div style="padding:8px;">{toggle_btn}{chart_html}</div>'

    html += f'''<script>
(function() {{
  var chartId    = "{chart_id}";
  var btnId      = "{btn_id}";
  var outright   = {json.dumps(all_outright)};
  var ramp       = {json.dumps(all_ramp)};
  var legendOut  = {json.dumps(legend_outright)};
  var legendRamp = {json.dumps(legend_ramp)};

  window.msCompToggle_{js_key} = function() {{
    var btn = document.getElementById(btnId);
    var toRamp = (btn.textContent === "Show Ramp");

    // Hide outright, show ramp (or vice versa)
    outright.forEach(function(i) {{ Plotly.restyle(chartId, {{visible: !toRamp}}, [i]); }});
    ramp.forEach(function(i)     {{ Plotly.restyle(chartId, {{visible:  toRamp}}, [i]); }});

    // Swap legend entries (Load subplot only)
    legendOut.forEach(function(i)  {{ Plotly.restyle(chartId, {{showlegend: !toRamp}}, [i]); }});
    legendRamp.forEach(function(i) {{ Plotly.restyle(chartId, {{showlegend:  toRamp}}, [i]); }});

    btn.textContent = toRamp ? "Show Outright" : "Show Ramp";
    Plotly.relayout(chartId, {{
      "yaxis.autorange": true, "yaxis2.autorange": true, "yaxis3.autorange": true
    }});
  }};
}})();
</script>'''

    return html


# ══════════════════════════════════════════════════════════════════════
# Row 2: Net Load profile + ramp
# ══════════════════════════════════════════════════════════════════════


def _render_net_load_row(
    latest_day: pd.DataFrame,
    cutoff_day: pd.DataFrame,
    cutoff_label: str | None,
    dt_key: str,
) -> str:
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("Net Load Breakdown", "Net Load Ramp"),
        horizontal_spacing=0.08,
    )

    hours = latest_day["hour_ending"]
    net = latest_day["net_load_mw"]
    solar = latest_day["solar_mw"]
    wind = latest_day["wind_mw"]
    load = latest_day["load_mw"] if "load_mw" in latest_day.columns else net + solar + wind
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

    # Load line on top
    fig.add_trace(go.Scatter(
        x=hours, y=load, mode="lines", name="Load",
        line=dict(color="#f8fafc", width=2),
        hovertemplate="HE %{x}<br>Load: %{y:,.0f} MW<extra></extra>",
    ), row=1, col=1)

    # Cutoff net load
    if not cutoff_day.empty:
        fig.add_trace(go.Scatter(
            x=cutoff_day["hour_ending"], y=cutoff_day["net_load_mw"],
            mode="lines", name=f"Net Load ({cutoff_label})",
            line=dict(color="#f87171", width=1.6, dash="dash"),
            hovertemplate=f"HE %{{x}}<br>Net ({cutoff_label}): %{{y:,.0f}} MW<extra></extra>",
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
        template=PLOTLY_TEMPLATE, height=470,
        margin=dict(l=60, r=40, t=80, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.08, x=0),
        hovermode="x unified", barmode="relative",
    )
    fig.update_xaxes(
        title_text="Hour Ending",
        dtick=1,
        range=[0.5, 24.5],
        autorange=False,
        fixedrange=True,
    )
    fig.update_yaxes(title_text="MW", col=1)
    fig.update_yaxes(title_text="MW/hr", col=2)

    return fig.to_html(
        include_plotlyjs="cdn",
        full_html=False,
        div_id=f"ms-netload-{dt_key}",
        config=PLOTLY_LOCKED_CONFIG,
    )


# ══════════════════════════════════════════════════════════════════════
# Data pull / prep helpers
# ══════════════════════════════════════════════════════════════════════


def _pull_load(region: str = "RTO", **cache_kwargs: Any) -> pd.DataFrame:
    df = _safe_pull(f"meteo_load_forecast_vintages_{region.lower()}_snapshot",
                    load_forecast_vintages.pull_source_vintages,
                    {"source": "meteologica", "region": region}, **cache_kwargs)
    if df.empty:
        return df
    out = df.copy()
    out["forecast_date"] = pd.to_datetime(out["forecast_date"]).dt.date
    out["hour_ending"] = pd.to_numeric(out["hour_ending"], errors="coerce").astype("Int64")
    out["forecast_mw"] = pd.to_numeric(out["forecast_load_mw"], errors="coerce")
    out["forecast_execution_datetime"] = pd.to_datetime(out["forecast_execution_datetime"], errors="coerce")
    out = out.dropna(subset=["forecast_date", "hour_ending", "forecast_mw", "vintage_label"]).copy()
    out["hour_ending"] = out["hour_ending"].astype(int)
    return out[["forecast_date", "hour_ending", "vintage_label", "forecast_mw", "forecast_execution_datetime"]]


def _pull_generation(kind: str, region: str = "RTO", **cache_kwargs: Any) -> pd.DataFrame:
    latest_fn = meteologica_generation_forecast_hourly.pull
    da_fn = meteologica_generation_forecast_hourly.pull_da_cutoff_vintages
    val_col = "forecast_generation_mw"
    src_l = f"meteo_{kind}_{region.lower()}_snap_l"
    src_d = f"meteo_{kind}_{region.lower()}_snap_da"

    latest = _safe_pull(src_l, latest_fn, {"source": kind, "region": region}, **cache_kwargs)
    da = _safe_pull(src_d, da_fn, {"source": kind, "region": region}, **cache_kwargs)

    frames: list[pd.DataFrame] = []
    if not latest.empty:
        df_l = latest.copy()
        df_l["vintage_label"] = "Latest"
        frames.append(df_l)
    if not da.empty:
        frames.append(da.copy())
    if not frames:
        return pd.DataFrame(columns=["forecast_date", "hour_ending", "vintage_label", "forecast_mw", "forecast_execution_datetime"])

    out = pd.concat(frames, ignore_index=True)
    out["forecast_date"] = pd.to_datetime(out["forecast_date"]).dt.date
    out["hour_ending"] = pd.to_numeric(out["hour_ending"], errors="coerce").astype("Int64")
    out["forecast_mw"] = pd.to_numeric(out[val_col], errors="coerce")
    out["forecast_execution_datetime"] = pd.to_datetime(out["forecast_execution_datetime"], errors="coerce")
    out = out.dropna(subset=["forecast_date", "hour_ending", "forecast_mw", "vintage_label"]).copy()
    out["hour_ending"] = out["hour_ending"].astype(int)
    return out[["forecast_date", "hour_ending", "vintage_label", "forecast_mw", "forecast_execution_datetime"]]


def _pick_cutoff_label(load, solar, wind) -> str | None:
    labels = [set(df["vintage_label"].dropna().unique()) for df in (load, solar, wind)]
    for l in CUTOFF_PRIORITY:
        if all(l in s for s in labels):
            return l
    union = set().union(*labels)
    for l in CUTOFF_PRIORITY:
        if l in union:
            return l
    return None


def _build_hourly_frame(load_df, solar_df, wind_df, vintage_label: str) -> pd.DataFrame:
    load_s = _series_from(load_df, vintage_label)
    solar_s = _series_from(solar_df, vintage_label)
    wind_s = _series_from(wind_df, vintage_label)
    if load_s.empty and solar_s.empty and wind_s.empty:
        return pd.DataFrame()
    idx = load_s.index.union(solar_s.index).union(wind_s.index)
    out = pd.DataFrame(index=idx).reset_index()
    out.columns = ["forecast_date", "hour_ending"]
    out["load_mw"] = out.set_index(["forecast_date", "hour_ending"]).index.map(load_s)
    out["solar_mw"] = out.set_index(["forecast_date", "hour_ending"]).index.map(solar_s)
    out["wind_mw"] = out.set_index(["forecast_date", "hour_ending"]).index.map(wind_s)
    out["renewables_mw"] = out["solar_mw"] + out["wind_mw"]
    out["net_load_mw"] = out["load_mw"] - out["renewables_mw"]
    out["vintage_label"] = vintage_label
    return out.sort_values(["forecast_date", "hour_ending"]).reset_index(drop=True)


def _series_from(df, vintage_label: str) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=float)
    sub = df[df["vintage_label"] == vintage_label].drop_duplicates(["forecast_date", "hour_ending"], keep="last")
    if sub.empty:
        return pd.Series(dtype=float)
    return sub.set_index(["forecast_date", "hour_ending"])["forecast_mw"]


def _slice_day(df, target_date) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=df.columns)
    day = df[df["forecast_date"] == target_date].copy()
    if day.empty:
        return pd.DataFrame(columns=df.columns)
    full = pd.DataFrame({"hour_ending": list(range(1, 25))})
    m = full.merge(day, on="hour_ending", how="left")
    m["forecast_date"] = target_date
    return m.sort_values("hour_ending").reset_index(drop=True)


def _date_key(dt) -> str:
    return dt.isoformat() if hasattr(dt, "isoformat") and not isinstance(dt, str) else str(dt)


def _build_meta_line(cutoff_label, latest_df, cutoff_df) -> str:
    le = _max_exec(latest_df, "Latest")
    ce = _max_exec(cutoff_df, cutoff_label) if cutoff_label else None
    lt = le.strftime("%Y-%m-%d %H:%M ET") if le else "N/A"
    ct = ce.strftime("%Y-%m-%d %H:%M ET") if ce else "N/A"
    cn = cutoff_label or "N/A"
    return f"Latest exec: {lt} | {cn} exec: {ct}"


def _max_exec(df, label) -> pd.Timestamp | None:
    if df.empty or "forecast_execution_datetime" not in df.columns:
        return None
    sub = df[df["vintage_label"] == label] if label and "vintage_label" in df.columns else df
    if sub.empty:
        return None
    ts = pd.to_datetime(sub["forecast_execution_datetime"], errors="coerce").dropna()
    return ts.max() if not ts.empty else None


def _safe_pull(name, fn, kwargs, **ck) -> pd.DataFrame:
    try:
        out = pull_with_cache(source_name=name, pull_fn=fn, pull_kwargs=kwargs, **ck)
        return out if out is not None else pd.DataFrame()
    except Exception as e:
        logger.warning("%s pull failed: %s", name, e)
        return pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════
# Formatting helpers
# ══════════════════════════════════════════════════════════════════════


def _cell_class(val, sign_colors: bool) -> str:
    if not sign_colors or pd.isna(val):
        return ""
    return "pos" if val > 0 else "neg" if val < 0 else "zero"


def _fmt(val, signed: bool = False) -> str:
    if pd.isna(val):
        return "\u2014"
    return f"{float(val):+,.0f}" if signed else f"{float(val):,.0f}"


def _empty_html(msg: str) -> str:
    return f"<div style='padding:14px;color:#f87171;font-family:monospace;'>{msg}</div>"


# ══════════════════════════════════════════════════════════════════════
# CSS
# ══════════════════════════════════════════════════════════════════════


_STYLE = """
<style>
.ms-wrap { padding: 8px; }
.ms-meta { margin-bottom: 10px; color: #9eb4d3; font-size: 11px; font-family: monospace; }
.ms-tw { overflow-x: auto; border: 1px solid #2a3f60; border-radius: 8px; }
.ms-t {
  width: 100%; border-collapse: collapse;
  font-size: 11px; font-family: monospace;
}
.ms-t th {
  position: sticky; top: 0; background: #16263d; color: #e6efff;
  border-bottom: 1px solid #2a3f60; padding: 6px 8px;
  text-align: right; white-space: nowrap;
}
.ms-t th.metric, .ms-t th.unit { text-align: left; }
.ms-t td {
  padding: 5px 8px; border-bottom: 1px solid #1f334f;
  text-align: right; color: #dbe7ff; white-space: nowrap;
}
.ms-t td.metric { text-align: left; color: #cfe0ff; font-weight: 700; }
.ms-t td.unit { text-align: left; color: #8aa5ca; }
.ms-t tr:nth-child(even) td { background: rgba(18, 32, 50, 0.45); }
.ms-t td.pos { color: #34d399; }
.ms-t td.neg { color: #f87171; }
.ms-t td.zero { color: #9eb4d3; }
.ms-toggle {
  padding: 4px 12px; font-size: 11px; font-weight: 600;
  background: #101d31; color: #9eb4d3; border: 1px solid #2a3f60;
  border-radius: 4px; cursor: pointer; font-family: inherit;
  margin-bottom: 6px;
}
.ms-toggle:hover { background: #1a2b44; color: #dbe7ff; }
</style>
"""
