"""Data inspection report — model inputs, data freshness, analog details, features, filtering.

Sections (incrementally enabled):
  1. Data Freshness      — Cache metadata: source, rows, age, staleness flags
  -- Future sections (Target Inputs, Analog Profiles, Distance Breakdown,
     Filtering Funnel, Feature Matrix) are implemented but disabled until
     the Data Freshness section is validated. --
"""
import json
import logging
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from src.like_day_forecast import configs

logger = logging.getLogger(__name__)

PLOTLY_TEMPLATE = "plotly_dark"
Section = tuple[str, Any, str | None]

# ── Shared styling constants ───────────────────────────────────────

CARD_STYLE = (
    "background:#111d31;border:1px solid #253b59;border-radius:10px;"
    "padding:16px 20px;min-width:180px;flex:1;"
)
LABEL_STYLE = (
    "font-size:11px;font-weight:600;color:#6f8db1;"
    "text-transform:uppercase;letter-spacing:0.5px;"
)
VALUE_STYLE = (
    "font-size:22px;font-weight:700;color:#dbe7ff;"
    "font-family:'Space Grotesk',monospace;"
)
TABLE_STYLE = (
    "width:100%;border-collapse:collapse;font-size:12px;font-family:monospace;"
)
TH_STYLE = (
    "padding:6px 10px;background:#16263d;color:#e6efff;text-align:left;"
    "font-size:11px;font-weight:600;text-transform:uppercase;position:sticky;top:0;"
)
TD_STYLE = "padding:5px 10px;border-bottom:1px solid #1e3350;color:#dbe7ff;"

GROUP_LABELS = {
    "lmp_profile": "LMP Profile",
    "lmp_level": "LMP Level",
    "lmp_volatility": "LMP Volatility",
    "load_level": "Load Level",
    "load_shape": "Load Shape",
    "gas_price": "Gas Price",
    "gas_momentum": "Gas Momentum",
    "calendar_dow": "Day of Week",
    "calendar_season": "Season",
    "weather_level": "Weather Level",
    "weather_hdd_cdd": "HDD/CDD",
    "weather_wind": "Wind Speed",
    "composite_heat_rate": "Heat Rate",
    "renewable_level": "Renewables Level",
    "renewable_shape": "Renewables Shape",
    "outage_level": "Outage Level",
    "outage_composition": "Outage Composition",
    "target_renewable_level": "Tgt Renewables",
    "target_outage_level": "Tgt Outages",
    "target_weather_level": "Tgt Weather",
    "target_weather_hdd_cdd": "Tgt HDD/CDD",
    "target_load_level": "Tgt Load Level",
    "target_load_shape": "Tgt Load Shape",
}


# ── Public entry point ─────────────────────────────────────────────


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list[Section]:
    """Build data inspection report sections.

    Currently only Data Source Freshness is enabled. Additional sections
    (Target Inputs, Analog Profiles, Distance Breakdown, Filtering Funnel,
    Feature Matrix) are implemented below and can be enabled by uncommenting.
    """
    logger.info("Building data inspection report...")

    sections: list[Section] = []

    # 1. Data Source Freshness (lightweight — just reads cache metadata)
    sections.append((
        "Data Source Freshness",
        _data_freshness_html(cache_dir, cache_ttl_hours),
        None,
    ))

    # ── Future sections (uncomment to enable) ────────────────────
    # Requires: from src.like_day_forecast.features.builder import build_daily_features
    #           from src.like_day_forecast.similarity import engine, filtering
    #           from src.like_day_forecast.similarity.engine import FEATURE_GROUPS
    #           from src.data import lmps_hourly
    #           from src.utils.cache_utils import pull_with_cache
    #
    # cache_kwargs = dict(cache_dir=cache_dir, cache_enabled=cache_enabled,
    #                     cache_ttl_hours=cache_ttl_hours, force_refresh=force_refresh)
    # pull_cache_kwargs = dict(cache_dir=cache_dir, cache_enabled=cache_enabled,
    #                          ttl_hours=cache_ttl_hours, force_refresh=force_refresh)
    # target_date = date.today() + timedelta(days=1)
    # reference_date = date.today()
    # df_features = build_daily_features(schema=schema, hub=configs.HUB, ...)
    #
    # sections.append(("Target Inputs", _target_inputs_html(...), None))
    # sections.append(("Analog LMP Profiles", _analog_overlay_chart_html(...), None))
    # sections.append(("Distance Breakdown", _analog_distance_html(...), None))
    # sections.append(("Filtering Funnel", _filtering_funnel_html(...), None))
    # sections.append(("Feature Matrix", _feature_matrix_html(...), None))

    return sections


# ── Section 1: Target Inputs ──────────────────────────────────────


def _target_inputs_html(
    df_features: pd.DataFrame,
    target_date: date,
    reference_date: date,
) -> str:
    """D+1 forecast inputs and reference-date context as metric cards."""
    ref_row = df_features[df_features["date"] == reference_date]
    if ref_row.empty:
        return _error_html(f"Reference date {reference_date} not in feature matrix")
    row = ref_row.iloc[0]

    html = '<div style="padding:12px;">'

    # -- Weather --
    html += _card_group(f"Weather Forecast \u2014 {target_date}", [
        ("Temp Avg", row.get("tgt_temp_daily_avg"), ".1f", "\u00b0F",
         row.get("tgt_temp_change_vs_ref"), "vs ref"),
        ("Temp Max", row.get("tgt_temp_daily_max"), ".1f", "\u00b0F", None, None),
        ("Temp Min", row.get("tgt_temp_daily_min"), ".1f", "\u00b0F", None, None),
        ("HDD", row.get("tgt_hdd"), ".0f", "", None, None),
        ("CDD", row.get("tgt_cdd"), ".0f", "", None, None),
    ])

    # -- Load --
    html += _card_group(f"Load Forecast \u2014 {target_date}", [
        ("DA Load Avg", row.get("tgt_load_daily_avg"), ",.0f", " MW",
         row.get("tgt_load_change_vs_ref"), "MW vs ref"),
        ("DA Load Peak", row.get("tgt_load_daily_peak"), ",.0f", " MW", None, None),
        ("DA Load Valley", row.get("tgt_load_daily_valley"), ",.0f", " MW", None, None),
    ])

    # -- Renewables --
    html += _card_group(f"Renewable Forecast \u2014 {target_date}", [
        ("Solar Avg", row.get("tgt_solar_daily_avg"), ",.0f", " MW", None, None),
        ("Wind Avg", row.get("tgt_wind_daily_avg"), ",.0f", " MW", None, None),
        ("Total Renewable", row.get("tgt_renewable_daily_avg"), ",.0f", " MW",
         row.get("tgt_renewable_change_vs_ref"), "MW vs ref"),
    ])

    # -- Outages --
    html += _card_group(f"Outage Forecast \u2014 {target_date}", [
        ("Total Outages", row.get("tgt_outage_total_mw"), ",.0f", " MW",
         row.get("tgt_outage_change_vs_ref"), "MW vs ref"),
        ("Forced Outages", row.get("tgt_outage_forced_mw"), ",.0f", " MW", None, None),
    ])

    # -- Reference context --
    html += _card_group(f"Reference Date Context \u2014 {reference_date}", [
        ("DA LMP Flat", row.get("lmp_daily_flat"), ".2f", " $/MWh", None, None),
        ("Gas (M3)", row.get("gas_m3_price"), ".2f", " $/MMBtu", None, None),
        ("Gas (HH)", row.get("gas_hh_price"), ".2f", " $/MMBtu", None, None),
        ("Temp Avg", row.get("temp_daily_avg"), ".1f", "\u00b0F", None, None),
        ("RT Load Avg", row.get("load_daily_avg"), ",.0f", " MW", None, None),
    ])

    html += "</div>"
    return html


def _card_group(title: str, cards: list) -> str:
    """Render a labeled row of metric cards."""
    html = '<div style="margin-bottom:16px;">'
    html += (
        f'<div style="font-size:13px;font-weight:700;color:#8dd9ff;'
        f'margin-bottom:8px;">{title}</div>'
    )
    html += '<div style="display:flex;gap:12px;flex-wrap:wrap;">'
    for label, value, fmt, unit, change, change_label in cards:
        html += _metric_card(label, value, fmt, unit, change, change_label)
    html += "</div></div>"
    return html


def _metric_card(
    label: str,
    value,
    fmt: str = ".1f",
    unit: str = "",
    change=None,
    change_label: str = "vs ref",
) -> str:
    """Single metric card with value and optional change indicator."""
    missing = pd.isna(value) if value is not None else True
    v_str = f"{float(value):{fmt}}" if not missing else "N/A"
    na_tag = (' <span style="color:#f87171;font-size:10px;">MISSING</span>' if missing else "")

    change_html = ""
    if change is not None and not pd.isna(change):
        sign = "+" if float(change) > 0 else ""
        ch_color = "#34d399" if abs(float(change)) < 5 else "#fbbf24"
        change_html = (
            f'<div style="font-size:12px;color:{ch_color};font-family:monospace;">'
            f"{sign}{float(change):{fmt}} {change_label}</div>"
        )

    return (
        f'<div style="{CARD_STYLE}">'
        f'<div style="{LABEL_STYLE}">{label}{na_tag}</div>'
        f'<div style="{VALUE_STYLE}">{v_str}'
        f'<span style="font-size:14px;color:#6f8db1;">{unit}</span></div>'
        f"{change_html}</div>"
    )


# ── Section 2: Data Freshness ─────────────────────────────────────


def _data_freshness_html(cache_dir: Path | None, ttl_hours: float = 4.0) -> str:
    """Table of cached data sources with age and status indicators.

    Deduplicates by source name — only the freshest entry per source is
    shown.  A *Params* column displays the pull_kwargs so callers can
    distinguish e.g. ``lmps_hourly_da(hub=WESTERN HUB)`` from a 7-day
    lookback variant.
    """
    if cache_dir is None or not cache_dir.exists():
        return _error_html("Cache directory not available")

    now = time.time()

    # ── Collect all entries ───────────────────────────────────────
    all_entries: list[dict] = []
    for meta_file in sorted(cache_dir.glob("*.meta.json")):
        try:
            meta = json.loads(meta_file.read_text())
        except (json.JSONDecodeError, OSError):
            continue

        source = meta.get("source_name", meta_file.stem)
        n_rows = meta.get("rows")
        cached_epoch = meta.get("cached_at_epoch", 0)
        age_hours = (now - cached_epoch) / 3600
        cached_at = meta.get("cached_at", "?")

        # Build compact param string from pull_kwargs
        raw_kwargs = meta.get("pull_kwargs", {})
        params = ", ".join(f"{k}={v}" for k, v in raw_kwargs.items()) if raw_kwargs else ""

        # Unique key = source + params (so different param sets are separate)
        dedup_key = f"{source}|{params}"

        all_entries.append(dict(
            dedup_key=dedup_key,
            source=source,
            params=params,
            rows=n_rows,
            age_hours=age_hours,
            cached_at=cached_at,
        ))

    if not all_entries:
        return _error_html("No cached data sources found")

    # ── Deduplicate: keep freshest per (source, params) ──────────
    best: dict[str, dict] = {}
    for entry in all_entries:
        key = entry["dedup_key"]
        if key not in best or entry["age_hours"] < best[key]["age_hours"]:
            best[key] = entry

    rows = list(best.values())
    rows.sort(key=lambda r: r["source"])

    # ── Assign status ────────────────────────────────────────────
    for r in rows:
        age = r["age_hours"]
        if age < 1:
            r["status"], r["color"] = "FRESH", "#34d399"
        elif age < ttl_hours:
            r["status"], r["color"] = "OK", "#fbbf24"
        else:
            r["status"], r["color"] = "STALE", "#f87171"

    # ── Summary badges ───────────────────────────────────────────
    n_fresh = sum(1 for r in rows if r["status"] == "FRESH")
    n_ok = sum(1 for r in rows if r["status"] == "OK")
    n_stale = sum(1 for r in rows if r["status"] == "STALE")
    total_rows = sum(r["rows"] for r in rows if isinstance(r["rows"], (int, float)))
    oldest = max(r["age_hours"] for r in rows) if rows else 0

    html = '<div style="padding:8px 12px;">'
    html += (
        '<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:12px;">'
    )
    for label, count, color in [
        ("Fresh", n_fresh, "#34d399"),
        ("OK", n_ok, "#fbbf24"),
        ("Stale", n_stale, "#f87171"),
    ]:
        html += (
            f'<div style="background:#111d31;border:1px solid #253b59;'
            f'border-radius:8px;padding:8px 16px;display:flex;align-items:center;gap:8px;">'
            f'<span style="display:inline-block;width:10px;height:10px;border-radius:50%;'
            f'background:{color};"></span>'
            f'<span style="font-size:12px;font-weight:700;color:#dbe7ff;">{count}</span>'
            f'<span style="font-size:11px;color:#6f8db1;text-transform:uppercase;">{label}</span>'
            f'</div>'
        )
    html += (
        f'<div style="background:#111d31;border:1px solid #253b59;'
        f'border-radius:8px;padding:8px 16px;font-size:12px;color:#8ea8c4;">'
        f'{len(rows)} sources | {total_rows:,} total rows | oldest: {oldest:.1f}h'
        f'</div>'
    )
    html += '</div>'

    # ── Table ────────────────────────────────────────────────────
    html += '<div style="overflow-x:auto;">'
    html += f'<table style="{TABLE_STYLE}">'
    html += "<thead><tr>"
    for col in ["Status", "Source", "Parameters", "Rows", "Age (hrs)", "Cached At"]:
        html += f'<th style="{TH_STYLE}">{col}</th>'
    html += "</tr></thead><tbody>"

    for r in rows:
        rows_str = (
            f"{r['rows']:,}" if isinstance(r["rows"], (int, float)) else str(r["rows"] or "?")
        )
        params_html = (
            f'<span style="color:#6f8db1;font-size:11px;">{r["params"]}</span>'
            if r["params"] else '<span style="color:#3a4f6f;">\u2014</span>'
        )
        html += (
            f"<tr>"
            f'<td style="{TD_STYLE}">'
            f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;'
            f'background:{r["color"]};margin-right:6px;vertical-align:middle;"></span>'
            f'{r["status"]}</td>'
            f'<td style="{TD_STYLE}font-weight:600;">{r["source"]}</td>'
            f'<td style="{TD_STYLE}">{params_html}</td>'
            f'<td style="{TD_STYLE}text-align:right;">{rows_str}</td>'
            f'<td style="{TD_STYLE}text-align:right;color:{r["color"]};">'
            f'{r["age_hours"]:.1f}</td>'
            f'<td style="{TD_STYLE}font-size:11px;">{r["cached_at"]}</td>'
            f"</tr>"
        )

    html += "</tbody></table></div></div>"
    return html


# ── Section 3: Analog LMP Overlay ─────────────────────────────────


def _analog_overlay_chart_html(
    analogs_df: pd.DataFrame,
    df_lmp: pd.DataFrame,
    target_date: date,
) -> str:
    """Plotly chart overlaying top-N analog D+1 LMP profiles + actual."""
    fig = go.Figure()
    top_n = min(10, len(analogs_df))
    top_analogs = analogs_df.head(top_n)

    colors = [
        "#60a5fa", "#a78bfa", "#34d399", "#fbbf24", "#f87171",
        "#818cf8", "#4ade80", "#fb923c", "#c084fc", "#94a3b8",
    ]

    for i, (_, analog) in enumerate(top_analogs.iterrows()):
        next_date = analog["date"] + timedelta(days=1)
        day_lmp = df_lmp[df_lmp["date"] == next_date].sort_values("hour_ending")
        if day_lmp.empty:
            continue

        color = colors[i % len(colors)]
        fig.add_trace(go.Scatter(
            x=day_lmp["hour_ending"].values,
            y=day_lmp["lmp_total"].values,
            mode="lines",
            name=f"#{int(analog['rank'])} {analog['date']} (w={analog['weight']:.1%})",
            line=dict(color=color, width=2.0 if i < 5 else 1.2),
            opacity=max(0.35, 1.0 - i * 0.065),
            hovertemplate=(
                f"<b>Analog #{int(analog['rank'])}: {analog['date']}</b><br>"
                "HE %{x}<br>LMP: $%{y:.1f}/MWh<br>"
                f"Distance: {analog['distance']:.3f} | Weight: {analog['weight']:.2%}"
                "<extra></extra>"
            ),
        ))

    # Actual LMP for target date
    actual_lmp = df_lmp[df_lmp["date"] == target_date].sort_values("hour_ending")
    if len(actual_lmp) >= 24:
        fig.add_trace(go.Scatter(
            x=actual_lmp["hour_ending"].values,
            y=actual_lmp["lmp_total"].values,
            mode="lines+markers",
            name=f"Actual ({target_date})",
            line=dict(color="#4cc9f0", width=3),
            marker=dict(size=5),
            hovertemplate="HE %{x}<br>Actual: $%{y:.1f}/MWh<extra></extra>",
        ))

    fig.update_layout(
        title=f"Analog D+1 LMP Profiles \u2014 Top {top_n}",
        xaxis_title="Hour Ending",
        yaxis_title="$/MWh",
        height=500,
        template=PLOTLY_TEMPLATE,
        legend=dict(
            orientation="v", yanchor="top", y=1, xanchor="left", x=1.02,
            font=dict(size=10),
        ),
        margin=dict(l=60, r=220, t=40, b=60),
        hovermode="x unified",
    )
    fig.update_xaxes(dtick=1, range=[0.5, 24.5])

    return fig.to_html(include_plotlyjs="cdn", full_html=False)


# ── Section 4: Distance Breakdown ─────────────────────────────────


def _resolve_group_cols(df: pd.DataFrame, group_def: dict) -> list[str]:
    """Resolve feature columns for a group definition."""
    if "columns_pattern" in group_def:
        return [c for c in df.columns if c.startswith(group_def["columns_pattern"])]
    return [c for c in group_def.get("columns", []) if c in df.columns]


def _analog_distance_html(
    analogs_df: pd.DataFrame,
    df_features: pd.DataFrame,
    df_pool: pd.DataFrame,
    reference_date: date,
) -> str:
    """Stacked bar chart showing per-group weighted distance for top analogs."""
    feature_weights = configs.FEATURE_GROUP_WEIGHTS.copy()
    target_mask = df_features["date"] == reference_date
    if not target_mask.any():
        return _error_html("Reference date not found")
    target_row = df_features[target_mask].iloc[0]

    # Resolve active groups (same skip-NaN logic as engine)
    group_columns: dict[str, list[str]] = {}
    for gn, gdef in FEATURE_GROUPS.items():
        if gn not in feature_weights or feature_weights[gn] == 0:
            continue
        cols = _resolve_group_cols(df_features, gdef)
        if not cols:
            continue
        if np.all(np.isnan(target_row[cols].values.astype(float))):
            continue
        group_columns[gn] = cols

    if not group_columns:
        return _error_html("No active feature groups")

    # Z-score normalize across pool + target (same as engine)
    all_dates = pd.concat([df_features[target_mask], df_pool]).drop_duplicates(subset=["date"])
    df_norm = all_dates.copy()
    for cols in group_columns.values():
        vals = df_norm[cols].values.astype(float)
        means = np.nanmean(vals, axis=0)
        stds = np.nanstd(vals, axis=0)
        stds[stds == 0] = 1.0
        df_norm[cols] = (vals - means) / stds

    target_norm = df_norm[df_norm["date"] == reference_date].iloc[0]

    # Per-group distances for top analogs
    top_n = min(5, len(analogs_df))
    top_analogs = analogs_df.head(top_n)
    group_names = list(group_columns.keys())
    per_analog: dict[str, dict[str, float]] = {}

    for _, analog in top_analogs.iterrows():
        an = df_norm[df_norm["date"] == analog["date"]]
        if an.empty:
            continue
        an = an.iloc[0]
        dists: dict[str, float] = {}
        for gn, cols in group_columns.items():
            t = np.nan_to_num(target_norm[cols].values.astype(float), nan=0.0)
            a = np.nan_to_num(an[cols].values.astype(float), nan=0.0)
            dists[gn] = float(np.sqrt(np.sum((t - a) ** 2))) * feature_weights.get(gn, 1.0)
        per_analog[str(analog["date"])] = dists

    if not per_analog:
        return _error_html("No analog distances computed")

    # Build stacked horizontal bar chart
    fig = go.Figure()
    analog_labels = [
        f"#{int(top_analogs.iloc[i]['rank'])} {d}"
        for i, d in enumerate(per_analog.keys())
    ][::-1]
    analog_dates = list(per_analog.keys())[::-1]

    for gn in group_names:
        fig.add_trace(go.Bar(
            y=analog_labels,
            x=[per_analog[d].get(gn, 0) for d in analog_dates],
            name=GROUP_LABELS.get(gn, gn),
            orientation="h",
            hovertemplate=(
                f"{GROUP_LABELS.get(gn, gn)}<br>"
                "Weighted distance: %{x:.3f}<extra></extra>"
            ),
        ))

    fig.update_layout(
        title=f"Per-Group Weighted Distance \u2014 Top {top_n} Analogs",
        xaxis_title="Weighted Distance",
        barmode="stack",
        height=max(300, 60 * top_n + 140),
        template=PLOTLY_TEMPLATE,
        legend=dict(
            orientation="h", yanchor="top", y=-0.18, xanchor="left", x=0,
            font=dict(size=10),
        ),
        margin=dict(l=160, r=40, t=40, b=120),
    )

    return fig.to_html(include_plotlyjs="cdn", full_html=False)


# ── Section 5: Filtering Funnel ───────────────────────────────────


def _filtering_funnel_html(
    n_universe: int,
    n_after_calendar: int,
    n_after_regime: int,
    n_pool: int,
) -> str:
    """Horizontal bar funnel showing candidate reduction at each filter stage."""
    stages = [
        ("Universe", n_universe, "#60a5fa"),
        ("Calendar Filter", n_after_calendar, "#a78bfa"),
        ("Regime Filter", n_after_regime, "#34d399"),
        ("Final Pool", n_pool, "#fbbf24"),
    ]
    max_val = max(s[1] for s in stages) or 1

    html = '<div style="padding:16px;">'
    for name, count, color in stages:
        pct = count / n_universe * 100 if n_universe > 0 else 0
        bar_w = count / max_val * 100 if max_val > 0 else 0
        html += (
            f'<div style="margin:8px 0;display:flex;align-items:center;gap:12px;">'
            f'<div style="width:130px;font-size:12px;font-weight:600;'
            f'color:#9eb4d3;text-align:right;">{name}</div>'
            f'<div style="flex:1;background:#0b1220;border-radius:4px;'
            f'height:28px;position:relative;">'
            f'<div style="width:{bar_w:.1f}%;background:{color};height:100%;'
            f'border-radius:4px;opacity:0.7;"></div>'
            f'<span style="position:absolute;left:8px;top:5px;font-size:12px;'
            f'font-weight:700;color:#dbe7ff;font-family:monospace;">'
            f'{count:,} ({pct:.0f}%)</span>'
            f'</div></div>'
        )

    # Summary box
    cal_removed = n_universe - n_after_calendar
    regime_removed = n_after_calendar - n_after_regime
    html += (
        f'<div style="margin-top:16px;padding:12px;background:#111d31;'
        f'border-radius:8px;border:1px solid #253b59;font-size:12px;color:#8ea8c4;">'
        f'<div style="margin-bottom:4px;font-weight:600;color:#9eb4d3;">Filter Summary</div>'
        f'<div>Calendar (DOW={configs.FILTER_SAME_DOW_GROUP}, '
        f'\u00b1{configs.FILTER_SEASON_WINDOW_DAYS}d): '
        f'removed {cal_removed:,} candidates</div>'
        f'<div>Regime (LMP/Gas z-score): removed {regime_removed:,} candidates</div>'
        f'<div style="margin-top:4px;font-weight:600;color:#dbe7ff;">'
        f'Final pool: {n_pool:,} candidates for {configs.DEFAULT_N_ANALOGS} analogs</div>'
        f'</div>'
    )
    html += "</div>"
    return html


# ── Section 6: Feature Matrix Summary ─────────────────────────────


def _feature_matrix_html(
    df_features: pd.DataFrame,
    reference_date: date,
) -> str:
    """Per-group summary: status, weight, NaN count, historical percentile."""
    feature_weights = configs.FEATURE_GROUP_WEIGHTS.copy()
    target_mask = df_features["date"] == reference_date
    if not target_mask.any():
        return _error_html(f"Reference date {reference_date} not found")

    target_row = df_features[target_mask].iloc[0]
    historical = df_features[df_features["date"] < reference_date]

    rows: list[dict] = []
    for gn, gdef in FEATURE_GROUPS.items():
        cols = _resolve_group_cols(df_features, gdef)
        if not cols:
            continue

        target_vals = target_row[cols].values.astype(float)
        n_nan = int(np.sum(np.isnan(target_vals)))
        all_nan = np.all(np.isnan(target_vals))
        weight = feature_weights.get(gn, 0)

        if all_nan:
            status, status_color = "SKIP", "#f87171"
        elif n_nan > 0:
            status, status_color = "PARTIAL", "#fbbf24"
        else:
            status, status_color = "ACTIVE", "#34d399"

        # Historical percentile of group mean
        percentile = None
        target_mean = None
        if not all_nan and len(historical) > 0:
            target_mean = float(np.nanmean(target_vals))
            hist_means = np.nanmean(historical[cols].values.astype(float), axis=1)
            hist_means = hist_means[~np.isnan(hist_means)]
            if len(hist_means) > 0:
                percentile = float(
                    np.searchsorted(np.sort(hist_means), target_mean) / len(hist_means) * 100
                )

        rows.append(dict(
            gn=gn,
            label=GROUP_LABELS.get(gn, gn),
            n_features=len(cols),
            weight=weight,
            n_nan=n_nan,
            status=status,
            status_color=status_color,
            percentile=percentile,
            target_mean=target_mean,
        ))

    # Build table
    html = '<div style="overflow-x:auto;padding:8px;">'
    html += f'<table style="{TABLE_STYLE}">'
    html += "<thead><tr>"
    for col in ["Status", "Feature Group", "# Feat", "Weight", "NaN", "Hist. Pctile"]:
        html += f'<th style="{TH_STYLE}">{col}</th>'
    html += "</tr></thead><tbody>"

    for r in rows:
        # Percentile indicator
        if r["percentile"] is not None:
            pct = r["percentile"]
            if 25 <= pct <= 75:
                bar_color = "#34d399"
            elif 10 <= pct <= 90:
                bar_color = "#fbbf24"
            else:
                bar_color = "#f87171"
            pctile_html = (
                f'<div style="display:flex;align-items:center;gap:6px;">'
                f'<div style="width:60px;height:8px;background:#0b1220;'
                f'border-radius:4px;position:relative;">'
                f'<div style="position:absolute;left:{pct:.0f}%;top:0;'
                f'width:3px;height:8px;background:{bar_color};border-radius:2px;"></div>'
                f'</div>'
                f'<span style="font-size:11px;color:{bar_color};">P{pct:.0f}</span>'
                f'</div>'
            )
        else:
            pctile_html = '<span style="color:#556;">\u2014</span>'

        nan_color = "#f87171" if r["n_nan"] > 0 else "#6f8db1"
        html += (
            f"<tr>"
            f'<td style="{TD_STYLE}">'
            f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;'
            f'background:{r["status_color"]};margin-right:6px;'
            f'vertical-align:middle;"></span>{r["status"]}</td>'
            f'<td style="{TD_STYLE}font-weight:600;">{r["label"]}</td>'
            f'<td style="{TD_STYLE}text-align:right;">{r["n_features"]}</td>'
            f'<td style="{TD_STYLE}text-align:right;">{r["weight"]:.1f}</td>'
            f'<td style="{TD_STYLE}text-align:right;color:{nan_color};">{r["n_nan"]}</td>'
            f'<td style="{TD_STYLE}">{pctile_html}</td>'
            f"</tr>"
        )

    html += "</tbody></table></div>"

    # Summary footer
    active = sum(1 for r in rows if r["status"] == "ACTIVE")
    skipped = sum(1 for r in rows if r["status"] == "SKIP")
    n_feat = sum(r["n_features"] for r in rows if r["status"] != "SKIP")
    html += (
        f'<div style="padding:8px 12px;font-size:12px;color:#8ea8c4;">'
        f"{active} active groups ({n_feat} features) | "
        f'{skipped} skipped (all NaN)</div>'
    )

    return html


# ── Helpers ────────────────────────────────────────────────────────


def _error_html(msg: str) -> str:
    return f'<div style="padding:16px;color:#e74c3c;font-size:14px;">{msg}</div>'
