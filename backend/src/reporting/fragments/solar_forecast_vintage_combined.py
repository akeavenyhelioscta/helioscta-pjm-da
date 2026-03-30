"""Solar forecast vintage dashboard.

For RTO: PJM Solar, Meteologica Solar, PJM vs Meteologica diff.
For WEST/MIDATL/SOUTH: Meteologica Solar only (PJM solar has no regional data).

Vintages: Latest, DA Cutoff, DA -12h, DA -24h, DA -48h.
Global controls: date pills + vintage toggle pills.
"""
import logging
from pathlib import Path

import pandas as pd

from src.like_day_forecast import configs
from src.data import solar_forecast_vintages
from src.utils.cache_utils import pull_with_cache
from src.reporting.fragments.forecast_chart_utils import (
    REGIONS, REGION_LABELS, VINTAGE_ORDER,
    vintage_order, empty,
    build_vintage_badges, build_vintage_chart, build_diff_chart,
    build_global_controls,
)

logger = logging.getLogger(__name__)

_PREFIX = "solarVint"


# ── Public entry point ──────────────────────────────────────────────


def build_fragments(
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> list:
    """Pull vintage data for solar forecasts, return combined fragments."""
    logger.info("Building combined vintage solar forecast report...")

    cache_kwargs = dict(
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    # ── Pull data (one cache file, split by source+region in-memory) ──
    df_all = _safe_pull(
        "solar_vintage_combined",
        solar_forecast_vintages.pull_combined_vintages,
        {},
        **cache_kwargs,
    )

    pjm_data: dict[str, pd.DataFrame | None] = {}
    meteo_data: dict[str, pd.DataFrame | None] = {}
    if df_all is not None and len(df_all) > 0:
        for src_key, target in [("pjm", pjm_data), ("meteologica", meteo_data)]:
            src_df = df_all[df_all["source"] == src_key] if "source" in df_all.columns else pd.DataFrame()
            for region in REGIONS:
                if len(src_df) > 0 and "region" in src_df.columns:
                    sub = src_df[src_df["region"] == region]
                    target[region] = sub if len(sub) > 0 else None
                else:
                    target[region] = None
    else:
        for region in REGIONS:
            pjm_data[region] = None
            meteo_data[region] = None

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
    fragments.append("Solar Forecast Vintages — RTO")

    df_pjm = pjm_data.get("RTO")
    if df_pjm is not None and len(df_pjm) > 0:
        chart = build_vintage_chart(
            f"{_PREFIX}Pjm_RTO", df_pjm,
            "PJM RTO — Solar Vintage Overlay",
            value_col="forecast_mw", y_title="Solar (MW)", prefix=_PREFIX,
        )
        fragments.append(("PJM RTO Solar Vintage Overlay", chart, None))
    else:
        fragments.append(("PJM RTO Solar Vintage Overlay",
                          empty("No PJM solar vintage data for RTO."), None))

    df_meteo_rto = meteo_data.get("RTO")
    if df_meteo_rto is not None and len(df_meteo_rto) > 0:
        chart = build_vintage_chart(
            f"{_PREFIX}Meteo_RTO", df_meteo_rto,
            "Meteologica RTO — Solar Vintage Overlay",
            value_col="forecast_mw", y_title="Solar (MW)", prefix=_PREFIX,
        )
        fragments.append(("Meteologica RTO Solar Vintage Overlay", chart, None))
    else:
        fragments.append(("Meteologica RTO Solar Vintage Overlay",
                          empty("No Meteologica solar vintage data for RTO."), None))

    # Diff chart for RTO
    has_both = (
        df_pjm is not None and len(df_pjm) > 0
        and df_meteo_rto is not None and len(df_meteo_rto) > 0
    )
    if has_both:
        chart = build_diff_chart(
            f"{_PREFIX}Diff_RTO", df_pjm, df_meteo_rto,
            "PJM vs Meteologica RTO — Solar Diff by Vintage",
            value_col="forecast_mw", label_a="PJM", label_b="Meteo", prefix=_PREFIX,
        )
        if chart is not None:
            fragments.append(("PJM vs Meteologica RTO Solar Diff", chart, None))
        else:
            fragments.append(("PJM vs Meteologica RTO Solar Diff",
                              empty("No common intervals for Solar diff in RTO."), None))
    else:
        fragments.append(("PJM vs Meteologica RTO Solar Diff",
                          empty("Need both PJM and Meteologica solar data for RTO diff."), None))

    # 4. Regional sections — Meteologica only
    for region in ["WEST", "MIDATL", "SOUTH"]:
        label = REGION_LABELS[region]
        fragments.append(f"Solar Forecast Vintages — {label}")

        df_meteo = meteo_data.get(region)
        if df_meteo is not None and len(df_meteo) > 0:
            chart = build_vintage_chart(
                f"{_PREFIX}Meteo_{region}", df_meteo,
                f"Meteologica {label} — Solar Vintage Overlay",
                value_col="forecast_mw", y_title="Solar (MW)", prefix=_PREFIX,
            )
            fragments.append((f"Meteologica {label} Solar Vintage Overlay", chart, None))
        else:
            fragments.append((f"Meteologica {label} Solar Vintage Overlay",
                              empty(f"No Meteologica solar vintage data for {label}."), None))

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


