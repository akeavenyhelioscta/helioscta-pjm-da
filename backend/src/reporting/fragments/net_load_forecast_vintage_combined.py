"""Net Load forecast vintage dashboard — Load − (Solar + Wind).

PJM Net Load: available for RTO only (solar/wind have no regional data).
Meteologica Net Load: available for all 4 regions.
PJM vs Meteologica diff: RTO only.

Vintages: Latest, DA Cutoff, DA -12h, DA -24h, DA -48h.
Global controls: date pills + vintage toggle pills.
"""
import logging
from pathlib import Path

import pandas as pd

from src.like_day_forecast import configs
from src.data import load_forecast_vintages, solar_forecast_vintages, wind_forecast_vintages
from src.utils.cache_utils import pull_with_cache
from src.reporting.fragments.like_day_forecast_chart_utils import (
    REGIONS, REGION_LABELS,
    vintage_order, empty, filter_common_intervals,
    build_vintage_badges, build_vintage_chart, build_diff_chart,
    build_global_controls,
)

logger = logging.getLogger(__name__)

_PREFIX = "netLoadVint"


# ── Public entry point ──────────────────────────────────────────────


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list:
    """Compute net load (Load − Solar − Wind) per vintage and build charts."""
    logger.info("Building combined vintage net load forecast report...")

    cache_kwargs = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    # ── Pull load data (one cached pull per source, split by region) ──
    pjm_load_all = _safe_pull(
        "pjm_load_forecast_vintages",
        load_forecast_vintages.pull_combined_vintages,
        {"source": "pjm"},
        **cache_kwargs,
    )
    meteo_load_all = _safe_pull(
        "meteologica_load_forecast_vintages",
        load_forecast_vintages.pull_combined_vintages,
        {"source": "meteologica"},
        **cache_kwargs,
    )
    pjm_load: dict[str, pd.DataFrame | None] = {}
    meteo_load: dict[str, pd.DataFrame | None] = {}
    for region in REGIONS:
        for src_key, df_all, target in [
            ("pjm", pjm_load_all, pjm_load),
            ("meteologica", meteo_load_all, meteo_load),
        ]:
            if df_all is not None and len(df_all) > 0 and "region" in df_all.columns:
                sub = df_all[df_all["region"] == region]
                target[region] = sub if len(sub) > 0 else None
            else:
                target[region] = None

    # ── Pull solar data (separate cache per provider) ───────
    pjm_solar_all = _safe_pull(
        "pjm_solar_forecast_vintages",
        solar_forecast_vintages.pull_pjm_vintages,
        {},
        **cache_kwargs,
    )
    meteo_solar_all = _safe_pull(
        "meteologica_solar_forecast_vintages",
        solar_forecast_vintages.pull_meteologica_vintages,
        {},
        **cache_kwargs,
    )
    solar_all = pd.concat([df for df in [pjm_solar_all, meteo_solar_all] if df is not None], ignore_index=True) if any(df is not None for df in [pjm_solar_all, meteo_solar_all]) else None
    pjm_solar: dict[str, pd.DataFrame | None] = {}
    meteo_solar: dict[str, pd.DataFrame | None] = {}
    _split_by_source_region(solar_all, pjm_solar, meteo_solar)

    # ── Pull wind data (separate cache per provider) ─────────
    pjm_wind_all = _safe_pull(
        "pjm_wind_forecast_vintages",
        wind_forecast_vintages.pull_pjm_vintages,
        {},
        **cache_kwargs,
    )
    meteo_wind_all = _safe_pull(
        "meteologica_wind_forecast_vintages",
        wind_forecast_vintages.pull_meteologica_vintages,
        {},
        **cache_kwargs,
    )
    wind_all = pd.concat([df for df in [pjm_wind_all, meteo_wind_all] if df is not None], ignore_index=True) if any(df is not None for df in [pjm_wind_all, meteo_wind_all]) else None
    pjm_wind: dict[str, pd.DataFrame | None] = {}
    meteo_wind: dict[str, pd.DataFrame | None] = {}
    _split_by_source_region(wind_all, pjm_wind, meteo_wind)

    # ── Compute net load ─────────────────────────────────────────
    # PJM net load: RTO only (solar/wind are RTO only)
    pjm_net: dict[str, pd.DataFrame | None] = {}
    pjm_net["RTO"] = _compute_net_load(
        pjm_load.get("RTO"), pjm_solar.get("RTO"), pjm_wind.get("RTO"),
    )

    # Meteologica net load: all regions
    meteo_net: dict[str, pd.DataFrame | None] = {}
    for region in REGIONS:
        meteo_net[region] = _compute_net_load(
            meteo_load.get(region),
            meteo_solar.get(region),
            meteo_wind.get(region),
        )

    # ── Collect all dates ────────────────────────────────────────
    all_dates: set = set()
    for d in [pjm_net, meteo_net]:
        for df in d.values():
            if df is not None and len(df) > 0:
                all_dates |= set(pd.to_datetime(df["forecast_date"]).dt.date.unique())
    all_dates_sorted = sorted(all_dates)

    # ── Build chart ID list ──────────────────────────────────────
    chart_ids: list[str] = []
    chart_ids.append(f"{_PREFIX}Pjm_RTO")
    for region in REGIONS:
        chart_ids.append(f"{_PREFIX}Meteo_{region}")
    chart_ids.append(f"{_PREFIX}Diff_RTO")

    fragments: list = []

    # 1. Global controls
    filter_html = build_global_controls(
        f"{_PREFIX}Filter", chart_ids, all_dates_sorted, _PREFIX,
    )
    fragments.append(("", filter_html, None))

    # 2. Vintage badges
    badges_df = pjm_net.get("RTO")
    if badges_df is not None and len(badges_df) > 0:
        order = vintage_order(badges_df)
        badges_html = build_vintage_badges(badges_df, order)
        fragments.append(("Vintage Info", badges_html, None))

    # 3. RTO section — PJM Net Load + Meteologica Net Load + Diff
    fragments.append("Net Load Forecast Vintages — RTO")

    df_pjm = pjm_net.get("RTO")
    if df_pjm is not None and len(df_pjm) > 0:
        df_common = filter_common_intervals(df_pjm)
        if len(df_common) > 0:
            chart = build_vintage_chart(
                f"{_PREFIX}Pjm_RTO", df_common,
                "PJM RTO — Net Load Vintage Overlay",
                value_col="forecast_mw", y_title="Net Load (MW)", prefix=_PREFIX,
            )
            fragments.append(("PJM RTO Net Load Vintage Overlay", chart, None))
        else:
            fragments.append(("PJM RTO Net Load Vintage Overlay",
                              empty("No common intervals for PJM Net Load RTO."), None))
    else:
        fragments.append(("PJM RTO Net Load Vintage Overlay",
                          empty("No PJM net load data for RTO (requires load, solar, and wind)."), None))

    df_meteo_rto = meteo_net.get("RTO")
    if df_meteo_rto is not None and len(df_meteo_rto) > 0:
        df_common = filter_common_intervals(df_meteo_rto)
        if len(df_common) > 0:
            chart = build_vintage_chart(
                f"{_PREFIX}Meteo_RTO", df_common,
                "Meteologica RTO — Net Load Vintage Overlay",
                value_col="forecast_mw", y_title="Net Load (MW)", prefix=_PREFIX,
            )
            fragments.append(("Meteologica RTO Net Load Vintage Overlay", chart, None))
        else:
            fragments.append(("Meteologica RTO Net Load Vintage Overlay",
                              empty("No common intervals for Meteologica Net Load RTO."), None))
    else:
        fragments.append(("Meteologica RTO Net Load Vintage Overlay",
                          empty("No Meteologica net load data for RTO."), None))

    # Diff chart for RTO
    has_both = (
        df_pjm is not None and len(df_pjm) > 0
        and df_meteo_rto is not None and len(df_meteo_rto) > 0
    )
    if has_both:
        chart = build_diff_chart(
            f"{_PREFIX}Diff_RTO", df_pjm, df_meteo_rto,
            "PJM vs Meteologica RTO — Net Load Diff by Vintage",
            value_col="forecast_mw", label_a="PJM", label_b="Meteo", prefix=_PREFIX,
        )
        if chart is not None:
            fragments.append(("PJM vs Meteologica RTO Net Load Diff", chart, None))
        else:
            fragments.append(("PJM vs Meteologica RTO Net Load Diff",
                              empty("No common intervals for net load diff in RTO."), None))
    else:
        fragments.append(("PJM vs Meteologica RTO Net Load Diff",
                          empty("Need both PJM and Meteologica net load for RTO diff."), None))

    # 4. Regional sections — Meteologica only
    for region in ["WEST", "MIDATL", "SOUTH"]:
        label = REGION_LABELS[region]
        fragments.append(f"Net Load Forecast Vintages — {label}")

        df_meteo = meteo_net.get(region)
        if df_meteo is not None and len(df_meteo) > 0:
            df_common = filter_common_intervals(df_meteo)
            if len(df_common) > 0:
                chart = build_vintage_chart(
                    f"{_PREFIX}Meteo_{region}", df_common,
                    f"Meteologica {label} — Net Load Vintage Overlay",
                    value_col="forecast_mw", y_title="Net Load (MW)", prefix=_PREFIX,
                )
                fragments.append((f"Meteologica {label} Net Load Vintage Overlay", chart, None))
            else:
                fragments.append((f"Meteologica {label} Net Load Vintage Overlay",
                                  empty(f"No common intervals for Meteologica Net Load {label}."), None))
        else:
            fragments.append((f"Meteologica {label} Net Load Vintage Overlay",
                              empty(f"No Meteologica net load data for {label}."), None))

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


def _split_by_source_region(
    df_all: pd.DataFrame | None,
    pjm_target: dict[str, pd.DataFrame | None],
    meteo_target: dict[str, pd.DataFrame | None],
) -> None:
    """Split a combined vintage DataFrame by source and region into dicts."""
    if df_all is not None and len(df_all) > 0:
        for src_key, target in [("pjm", pjm_target), ("meteologica", meteo_target)]:
            src_df = df_all[df_all["source"] == src_key] if "source" in df_all.columns else pd.DataFrame()
            for region in REGIONS:
                if len(src_df) > 0 and "region" in src_df.columns:
                    sub = src_df[src_df["region"] == region]
                    target[region] = sub if len(sub) > 0 else None
                else:
                    target[region] = None
    else:
        for region in REGIONS:
            pjm_target[region] = None
            meteo_target[region] = None


def _compute_net_load(
    df_load: pd.DataFrame | None,
    df_solar: pd.DataFrame | None,
    df_wind: pd.DataFrame | None,
) -> pd.DataFrame | None:
    """Compute net load = load − solar − wind per vintage.

    Uses inner join so net load only covers the date/hour range where
    all three sources (load, solar, wind) have data.  Returns None if
    any source is missing entirely.
    """
    if df_load is None or len(df_load) == 0:
        return None
    if df_solar is None or len(df_solar) == 0:
        return None
    if df_wind is None or len(df_wind) == 0:
        return None

    _join = ["vintage_label", "forecast_date", "hour_ending"]

    # Normalize load column name
    load = df_load.copy()
    if "forecast_load_mw" in load.columns:
        load["load_mw"] = pd.to_numeric(load["forecast_load_mw"], errors="coerce")
    elif "forecast_mw" in load.columns:
        load["load_mw"] = pd.to_numeric(load["forecast_mw"], errors="coerce")
    else:
        logger.warning("No load MW column found")
        return None

    load["forecast_date"] = pd.to_datetime(load["forecast_date"])
    merged = load[_join + ["load_mw", "forecast_execution_datetime",
                           "vintage_anchor_execution_datetime"]].copy()

    # Inner-join solar
    solar = df_solar.copy()
    solar["forecast_date"] = pd.to_datetime(solar["forecast_date"])
    solar["solar_mw"] = pd.to_numeric(solar["forecast_mw"], errors="coerce")
    merged = merged.merge(solar[_join + ["solar_mw"]], on=_join, how="inner")

    # Inner-join wind
    wind = df_wind.copy()
    wind["forecast_date"] = pd.to_datetime(wind["forecast_date"])
    wind["wind_mw"] = pd.to_numeric(wind["forecast_mw"], errors="coerce")
    merged = merged.merge(wind[_join + ["wind_mw"]], on=_join, how="inner")

    if len(merged) == 0:
        return None

    merged["forecast_mw"] = merged["load_mw"] - merged["solar_mw"] - merged["wind_mw"]
    merged["hour_ending"] = merged["hour_ending"].astype(int)

    return merged[["forecast_date", "hour_ending", "forecast_mw",
                    "forecast_execution_datetime", "vintage_label",
                    "vintage_anchor_execution_datetime"]]
