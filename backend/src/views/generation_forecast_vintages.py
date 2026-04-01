"""View model for generation forecast vintages (solar & wind).

Takes an all-region, all-source DataFrame and produces structured context:
per-region per-source vintage summaries, vintage deltas, PJM vs Meteologica
spread, and cross-region highlights.

Identical data contract for solar and wind — the ``forecast_type`` label
is the only difference.

Input DataFrame columns (from solar_forecast_vintages / wind_forecast_vintages):
    source, region, forecast_date, hour_ending, forecast_mw,
    forecast_execution_datetime, vintage_label, vintage_anchor_execution_datetime

Consumed by:
  - API endpoints (JSON)
  - Agent (structured context for cross-region scanning)
"""
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

ONPEAK_HOURS = list(range(8, 24))   # HE8–HE23
OFFPEAK_HOURS = list(range(1, 8)) + [24]  # HE1–HE7, HE24

REGIONS = ["RTO", "WEST", "MIDATL", "SOUTH"]
VINTAGE_ORDER = ["Latest", "DA Cutoff", "DA -12h", "DA -24h", "DA -48h"]
_DELTA_PAIRS = [
    ("Latest", "DA Cutoff"),
    ("DA Cutoff", "DA -12h"),
    ("DA -12h", "DA -24h"),
    ("DA -24h", "DA -48h"),
]


def build_view_model(
    df: pd.DataFrame | None,
    forecast_type: str = "solar",
) -> dict:
    """Transform generation forecast vintage data into a structured view model.

    Args:
        df: Combined DataFrame with ``source`` column ("pjm"/"meteologica")
            and ``forecast_mw`` values. All regions included.
        forecast_type: "solar" or "wind" — used for labeling only.

    Returns:
        Structured dict with per-region breakdowns. The agent is responsible
        for interpreting the data and highlighting notable changes.
    """
    if df is None or len(df) == 0:
        return {"error": f"No {forecast_type} forecast data available",
                "forecast_type": forecast_type}

    df = _normalize(df)

    # Split by source
    df_pjm = df[df["source"] == "pjm"] if "source" in df.columns else pd.DataFrame()
    df_meteo = df[df["source"] == "meteologica"] if "source" in df.columns else pd.DataFrame()
    pjm_ok = len(df_pjm) > 0
    meteo_ok = len(df_meteo) > 0

    # Regions and vintages present
    region_set: set[str] = set(df["region"].unique()) if "region" in df.columns else set()
    regions_present = [r for r in REGIONS if r in region_set]
    vintage_set: set[str] = set(df["vintage_label"].unique())

    # Build per-region view models
    by_region: dict[str, dict] = {}
    for region in regions_present:
        pjm_r = df_pjm[df_pjm["region"] == region] if pjm_ok else pd.DataFrame()
        meteo_r = df_meteo[df_meteo["region"] == region] if meteo_ok else pd.DataFrame()

        pjm_r_ok = len(pjm_r) > 0
        meteo_r_ok = len(meteo_r) > 0

        if not pjm_r_ok and not meteo_r_ok:
            continue

        rdates: set = set()
        if pjm_r_ok:
            rdates |= set(pjm_r["forecast_date"].unique())
        if meteo_r_ok:
            rdates |= set(meteo_r["forecast_date"].unique())

        sources: dict = {}
        if pjm_r_ok:
            sources["pjm"] = _build_source_summary(pjm_r)
        if meteo_r_ok:
            sources["meteologica"] = _build_source_summary(meteo_r)

        vintage_deltas: dict = {}
        if pjm_r_ok:
            vintage_deltas["pjm"] = _build_vintage_deltas(pjm_r)
        if meteo_r_ok:
            vintage_deltas["meteologica"] = _build_vintage_deltas(meteo_r)

        pjm_vs_meteo: dict = {}
        if pjm_r_ok and meteo_r_ok:
            pjm_vs_meteo = _build_source_spread(pjm_r, meteo_r)

        by_region[region] = {
            "forecast_dates": sorted(str(d) for d in rdates),
            "sources": sources,
            "vintage_deltas": vintage_deltas,
            "pjm_vs_meteologica": pjm_vs_meteo,
        }

    return {
        "forecast_type": forecast_type,
        "regions": regions_present,
        "vintage_order": [v for v in VINTAGE_ORDER if v in vintage_set],
        "by_region": by_region,
    }


# ── Data normalization ─────────────────────────────────────────────


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure consistent dtypes."""
    df = df.copy()
    df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date
    df["hour_ending"] = pd.to_numeric(df["hour_ending"], errors="coerce")
    df["forecast_mw"] = pd.to_numeric(df["forecast_mw"], errors="coerce")
    df["forecast_execution_datetime"] = pd.to_datetime(
        df["forecast_execution_datetime"], errors="coerce",
    )
    df = df.dropna(subset=["forecast_date", "hour_ending", "forecast_mw"])
    df["hour_ending"] = df["hour_ending"].astype(int)
    return df


# ── Source summary ─────────────────────────────────────────────────


def _build_source_summary(df: pd.DataFrame) -> dict:
    """Per-vintage execution timestamps and period averages by forecast date."""
    present = [v for v in VINTAGE_ORDER if v in df["vintage_label"].unique()]

    exec_ts: dict[str, str | None] = {}
    for v in present:
        ts = df.loc[df["vintage_label"] == v, "forecast_execution_datetime"].max()
        exec_ts[v] = str(ts) if pd.notna(ts) else None

    by_date: dict[str, dict] = {}
    for (fdate, vlabel), grp in df.groupby(["forecast_date", "vintage_label"]):
        ds = str(fdate)
        if ds not in by_date:
            by_date[ds] = {}
        by_date[ds][vlabel] = _period_averages(
            grp["hour_ending"], grp["forecast_mw"],
        )

    return {
        "vintages_present": present,
        "execution_timestamps": exec_ts,
        "by_date": by_date,
    }


# ── Vintage deltas ────────────────────────────────────────────────


def _build_vintage_deltas(df: pd.DataFrame) -> dict:
    """MW change between consecutive vintages (newer minus older)."""
    present = set(df["vintage_label"].unique())
    deltas: dict = {}

    for newer, older in _DELTA_PAIRS:
        if newer not in present or older not in present:
            continue

        df_new = df.loc[
            df["vintage_label"] == newer,
            ["forecast_date", "hour_ending", "forecast_mw"],
        ]
        df_old = df.loc[
            df["vintage_label"] == older,
            ["forecast_date", "hour_ending", "forecast_mw"],
        ]

        merged = df_new.merge(
            df_old, on=["forecast_date", "hour_ending"],
            suffixes=("_new", "_old"), how="inner",
        )
        if len(merged) == 0:
            continue

        merged["delta_mw"] = merged["forecast_mw_new"] - merged["forecast_mw_old"]

        by_date: dict[str, dict] = {}
        for fdate, grp in merged.groupby("forecast_date"):
            by_date[str(fdate)] = _delta_summary(grp)

        pair_key = f"{newer} vs {older}"
        deltas[pair_key] = {
            "by_date": by_date,
            "overall": _delta_summary(merged),
        }

    return deltas


def _delta_summary(df: pd.DataFrame) -> dict:
    """Summarize MW deltas by period, with the peak-change hour."""
    on = df[df["hour_ending"].isin(ONPEAK_HOURS)]["delta_mw"]
    off = df[df["hour_ending"].isin(OFFPEAK_HOURS)]["delta_mw"]

    peak_hour = None
    if len(df) > 0:
        peak_idx = df["delta_mw"].abs().idxmax()
        row = df.loc[peak_idx]
        peak_hour = {
            "hour_ending": int(row["hour_ending"]),
            "delta_mw": _sr(row["delta_mw"]),
        }

    return {
        "on_peak_mw": _sr(on.mean()),
        "off_peak_mw": _sr(off.mean()),
        "flat_mw": _sr(df["delta_mw"].mean()),
        "peak_hour_change": peak_hour,
    }


# ── Source spread ─────────────────────────────────────────────────


def _build_source_spread(
    df_pjm: pd.DataFrame, df_meteo: pd.DataFrame,
) -> dict:
    """PJM minus Meteologica MW spread per shared vintage."""
    common = (
        set(df_pjm["vintage_label"].unique())
        & set(df_meteo["vintage_label"].unique())
    )
    spread: dict = {}

    for v in [vv for vv in VINTAGE_ORDER if vv in common]:
        pjm_v = df_pjm.loc[
            df_pjm["vintage_label"] == v,
            ["forecast_date", "hour_ending", "forecast_mw"],
        ]
        meteo_v = df_meteo.loc[
            df_meteo["vintage_label"] == v,
            ["forecast_date", "hour_ending", "forecast_mw"],
        ]

        merged = pjm_v.merge(
            meteo_v, on=["forecast_date", "hour_ending"],
            suffixes=("_pjm", "_meteo"), how="inner",
        )
        if len(merged) == 0:
            continue

        merged["diff_mw"] = merged["forecast_mw_pjm"] - merged["forecast_mw_meteo"]

        by_date: dict[str, dict] = {}
        for fdate, grp in merged.groupby("forecast_date"):
            by_date[str(fdate)] = _spread_summary(grp)

        spread[v] = {
            "by_date": by_date,
            "overall": _spread_summary(merged),
        }

    return spread


def _spread_summary(df: pd.DataFrame) -> dict:
    """Period averages of PJM - Meteologica diff."""
    on = df[df["hour_ending"].isin(ONPEAK_HOURS)]["diff_mw"]
    off = df[df["hour_ending"].isin(OFFPEAK_HOURS)]["diff_mw"]
    return {
        "on_peak_mw": _sr(on.mean()),
        "off_peak_mw": _sr(off.mean()),
        "flat_mw": _sr(df["diff_mw"].mean()),
    }


# ── Helpers ───────────────────────────────────────────────────────


def _period_averages(hours: pd.Series, values: pd.Series) -> dict:
    """Compute on-peak, off-peak, and flat average MW."""
    mask_on = hours.isin(ONPEAK_HOURS)
    mask_off = hours.isin(OFFPEAK_HOURS)
    return {
        "on_peak": _sr(values[mask_on].mean()),
        "off_peak": _sr(values[mask_off].mean()),
        "flat": _sr(values.mean()),
    }


def _sr(val, decimals: int = 0) -> float | None:
    """Safe round — return None for NaN/None."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if np.isnan(f) else round(f, decimals)
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    import json
    import src.settings  # noqa: F401 — load env vars

    from src.like_day_forecast import configs
    from src.data import solar_forecast_vintages, wind_forecast_vintages
    from src.utils.cache_utils import pull_with_cache

    logging.basicConfig(level=logging.INFO)

    CACHE = dict(
        cache_dir=configs.CACHE_DIR,
        cache_enabled=configs.CACHE_ENABLED,
        ttl_hours=configs.CACHE_TTL_HOURS,
        force_refresh=configs.FORCE_CACHE_REFRESH,
    )

    for ftype, mod in [("solar", solar_forecast_vintages), ("wind", wind_forecast_vintages)]:
        df_pjm = pull_with_cache(
            source_name=f"pjm_{ftype}_forecast_vintages",
            pull_fn=mod.pull_pjm_vintages,
            pull_kwargs={},
            **CACHE,
        )
        df_meteo = pull_with_cache(
            source_name=f"meteologica_{ftype}_forecast_vintages",
            pull_fn=mod.pull_meteologica_vintages,
            pull_kwargs={},
            **CACHE,
        )
        df = pd.concat([df_pjm, df_meteo], ignore_index=True)
        vm = build_view_model(df, forecast_type=ftype)
        print(f"\n{'='*60}")
        print(f"  {ftype.upper()} FORECAST VINTAGES")
        print(f"{'='*60}")
        print(json.dumps(vm, indent=2, default=str))
