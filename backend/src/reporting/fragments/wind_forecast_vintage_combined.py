"""Wind forecast vintage dashboard.

For RTO: PJM Wind, Meteologica Wind, PJM vs Meteologica diff.
For WEST/MIDATL/SOUTH: Meteologica Wind only (PJM wind has no regional data).

Vintages: Latest, DA Cutoff, DA -12h, DA -24h, DA -48h.
Global controls: date pills + vintage toggle pills.
"""
import logging
from pathlib import Path

import pandas as pd

from src.like_day_forecast import configs
from src.data import wind_forecast_vintages
from src.utils.cache_utils import pull_with_cache
from src.reporting.fragments.forecast_chart_utils import (
    REGIONS, REGION_LABELS, VINTAGE_ORDER,
    vintage_order, empty,
    build_vintage_badges, build_vintage_chart, build_diff_chart,
    build_global_controls,
)

logger = logging.getLogger(__name__)

_PREFIX = "windVint"


# ── Public entry point ──────────────────────────────────────────────


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list:
    """Pull vintage data for wind forecasts, return combined fragments."""
    logger.info("Building combined vintage wind forecast report...")

    cache_kwargs = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    # ── Pull data (separate cache per provider) ──
    df_pjm_all = _safe_pull(
        "pjm_wind_forecast_vintages",
        wind_forecast_vintages.pull_pjm_vintages,
        {},
        **cache_kwargs,
    )
    df_meteo_all = _safe_pull(
        "meteologica_wind_forecast_vintages",
        wind_forecast_vintages.pull_meteologica_vintages,
        {},
        **cache_kwargs,
    )

    pjm_data: dict[str, pd.DataFrame | None] = {}
    meteo_data: dict[str, pd.DataFrame | None] = {}
    for src_key, src_df_raw, target in [("pjm", df_pjm_all, pjm_data), ("meteologica", df_meteo_all, meteo_data)]:
        src_df = src_df_raw if src_df_raw is not None and len(src_df_raw) > 0 else pd.DataFrame()
        for region in REGIONS:
            if len(src_df) > 0 and "region" in src_df.columns:
                sub = src_df[src_df["region"] == region]
                target[region] = sub if len(sub) > 0 else None
            else:
                target[region] = None

    # ── Collect all dates for the global filter ──────────────────
    all_dates: set = set()
    if pjm_data["RTO"] is not None and len(pjm_data["RTO"]) > 0:
        all_dates |= set(pd.to_datetime(pjm_data["RTO"]["forecast_date"]).dt.date.unique())
    for df in meteo_data.values():
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

    # 2. Vintage badges (using PJM RTO data)
    badges_df = pjm_data.get("RTO")
    if badges_df is not None and len(badges_df) > 0:
        order = vintage_order(badges_df)
        badges_html = build_vintage_badges(badges_df, order)
        fragments.append(("Vintage Info", badges_html, None))

    # 3. RTO section — PJM + Meteologica + Diff
    fragments.append("Wind Forecast Vintages — RTO")

    df_pjm = pjm_data.get("RTO")
    if df_pjm is not None and len(df_pjm) > 0:
        chart = build_vintage_chart(
            f"{_PREFIX}Pjm_RTO", df_pjm,
            "PJM RTO — Wind Vintage Overlay",
            value_col="forecast_mw", y_title="Wind (MW)", prefix=_PREFIX,
        )
        fragments.append(("PJM RTO Wind Vintage Overlay", chart, None))
    else:
        fragments.append(("PJM RTO Wind Vintage Overlay",
                          empty("No PJM wind vintage data for RTO."), None))

    df_meteo_rto = meteo_data.get("RTO")
    if df_meteo_rto is not None and len(df_meteo_rto) > 0:
        chart = build_vintage_chart(
            f"{_PREFIX}Meteo_RTO", df_meteo_rto,
            "Meteologica RTO — Wind Vintage Overlay",
            value_col="forecast_mw", y_title="Wind (MW)", prefix=_PREFIX,
        )
        fragments.append(("Meteologica RTO Wind Vintage Overlay", chart, None))
    else:
        fragments.append(("Meteologica RTO Wind Vintage Overlay",
                          empty("No Meteologica wind vintage data for RTO."), None))

    # Diff chart for RTO
    has_both = (
        df_pjm is not None and len(df_pjm) > 0
        and df_meteo_rto is not None and len(df_meteo_rto) > 0
    )
    if has_both:
        chart = build_diff_chart(
            f"{_PREFIX}Diff_RTO", df_pjm, df_meteo_rto,
            "PJM vs Meteologica RTO — Wind Diff by Vintage",
            value_col="forecast_mw", label_a="PJM", label_b="Meteo", prefix=_PREFIX,
        )
        if chart is not None:
            fragments.append(("PJM vs Meteologica RTO Wind Diff", chart, None))
        else:
            fragments.append(("PJM vs Meteologica RTO Wind Diff",
                              empty("No common intervals for Wind diff in RTO."), None))
    else:
        fragments.append(("PJM vs Meteologica RTO Wind Diff",
                          empty("Need both PJM and Meteologica wind data for RTO diff."), None))

    # 4. Regional sections — Meteologica only
    for region in ["WEST", "MIDATL", "SOUTH"]:
        label = REGION_LABELS[region]
        fragments.append(f"Wind Forecast Vintages — {label}")

        df_meteo = meteo_data.get(region)
        if df_meteo is not None and len(df_meteo) > 0:
            chart = build_vintage_chart(
                f"{_PREFIX}Meteo_{region}", df_meteo,
                f"Meteologica {label} — Wind Vintage Overlay",
                value_col="forecast_mw", y_title="Wind (MW)", prefix=_PREFIX,
            )
            fragments.append((f"Meteologica {label} Wind Vintage Overlay", chart, None))
        else:
            fragments.append((f"Meteologica {label} Wind Vintage Overlay",
                              empty(f"No Meteologica wind vintage data for {label}."), None))

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


