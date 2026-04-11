"""View model: Regional congestion — DA/RT congestion component across PJM hubs.

Compares congestion pricing at the four PJM aggregate hubs over a 7-day
lookback to surface which regions have active transmission constraints and
how those constraints are evolving.

Consumed by:
  - API endpoints (JSON / markdown)
  - Agent (cross-reference with transmission outages)
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

ONPEAK_HOURS = list(range(8, 24))   # HE8-HE23
OFFPEAK_HOURS = list(range(1, 8)) + [24]  # HE1-HE7, HE24

# PJM aggregate pricing hubs — ordered west to east
HUB_ORDER = ["WESTERN HUB", "AEP GEN HUB", "DOMINION HUB", "EASTERN HUB"]

HUB_SHORT = {
    "WESTERN HUB": "West",
    "AEP GEN HUB": "AEP",
    "DOMINION HUB": "Dom",
    "EASTERN HUB": "East",
}


def build_view_model(
    df_da: pd.DataFrame,
    df_rt: pd.DataFrame,
    lookback_days: int = 7,
) -> dict:
    """Build regional congestion view model.

    Args:
        df_da: DA LMP data from lmps_hourly.pull(market="da") — all hubs.
        df_rt: RT LMP data from lmps_hourly.pull(market="rt") — all hubs.
        lookback_days: Number of days to look back (default 7).

    Returns:
        Structured dict with cross-hub congestion summaries.
    """
    da_ok = df_da is not None and len(df_da) > 0
    rt_ok = df_rt is not None and len(df_rt) > 0

    if not da_ok and not rt_ok:
        return {"error": "No LMP data available"}

    cutoff = date.today() - timedelta(days=lookback_days)

    df_da = _normalize(df_da, cutoff) if da_ok else pd.DataFrame()
    df_rt = _normalize(df_rt, cutoff) if rt_ok else pd.DataFrame()

    da_ok = len(df_da) > 0
    rt_ok = len(df_rt) > 0

    # Filter to known aggregate hubs only
    if da_ok:
        df_da = df_da[df_da["hub"].isin(HUB_ORDER)]
    if rt_ok:
        df_rt = df_rt[df_rt["hub"].isin(HUB_ORDER)]

    result: dict = {
        "date_range": {
            "start": str(cutoff),
            "end": str(date.today()),
        },
        "hubs": [h for h in HUB_ORDER if
                 (da_ok and h in df_da["hub"].values) or
                 (rt_ok and h in df_rt["hub"].values)],
    }

    # Daily congestion summary — one row per date, columns per hub/market
    result["daily_congestion"] = _build_daily_congestion(
        df_da if da_ok else pd.DataFrame(),
        df_rt if rt_ok else pd.DataFrame(),
    )

    # Hub-level congestion profiles — hourly detail per hub
    result["hub_profiles"] = _build_hub_profiles(
        df_da if da_ok else pd.DataFrame(),
        df_rt if rt_ok else pd.DataFrame(),
    )

    # Cross-hub spread — congestion differentials between hubs
    if da_ok:
        result["da_congestion_spread"] = _build_congestion_spread(df_da)
    if rt_ok:
        result["rt_congestion_spread"] = _build_congestion_spread(df_rt)

    return result


# -- Normalization --------------------------------------------------------


def _normalize(df: pd.DataFrame, cutoff: date) -> pd.DataFrame:
    """Ensure consistent dtypes and apply lookback filter."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["hour_ending"] = pd.to_numeric(df["hour_ending"], errors="coerce").astype(int)
    for col in ["lmp_total", "lmp_system_energy_price", "lmp_congestion_price"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["date", "hour_ending", "lmp_congestion_price"])
    df = df[df["date"] >= cutoff]
    return df


# -- Daily congestion summary --------------------------------------------


def _build_daily_congestion(df_da: pd.DataFrame, df_rt: pd.DataFrame) -> list[dict]:
    """One row per date with DA/RT congestion by hub (on-peak, off-peak, flat)."""
    all_dates: set[date] = set()
    if len(df_da) > 0:
        all_dates.update(df_da["date"].unique())
    if len(df_rt) > 0:
        all_dates.update(df_rt["date"].unique())

    rows = []
    for d in sorted(all_dates):
        row: dict = {"date": str(d)}
        for hub in HUB_ORDER:
            short = HUB_SHORT[hub]
            for mkt_label, df_mkt in [("da", df_da), ("rt", df_rt)]:
                if len(df_mkt) == 0:
                    continue
                grp = df_mkt[(df_mkt["date"] == d) & (df_mkt["hub"] == hub)]
                if grp.empty:
                    continue
                mask_on = grp["hour_ending"].isin(ONPEAK_HOURS)
                mask_off = grp["hour_ending"].isin(OFFPEAK_HOURS)
                row[f"{short}_{mkt_label}_onpk"] = _sr(grp.loc[mask_on, "lmp_congestion_price"].mean())
                row[f"{short}_{mkt_label}_offpk"] = _sr(grp.loc[mask_off, "lmp_congestion_price"].mean())
                row[f"{short}_{mkt_label}_flat"] = _sr(grp["lmp_congestion_price"].mean())
        rows.append(row)
    return rows


# -- Hub profiles ---------------------------------------------------------


def _build_hub_profiles(df_da: pd.DataFrame, df_rt: pd.DataFrame) -> list[dict]:
    """Per-hub congestion profile: daily DA/RT congestion + period stats."""
    profiles = []
    for hub in HUB_ORDER:
        profile: dict = {"hub": hub, "short": HUB_SHORT[hub]}

        # DA daily
        if len(df_da) > 0:
            hub_da = df_da[df_da["hub"] == hub]
            if not hub_da.empty:
                profile["da_daily"] = _daily_period_stats(hub_da)
                profile["da_hourly"] = _hourly_records(hub_da)

        # RT daily
        if len(df_rt) > 0:
            hub_rt = df_rt[df_rt["hub"] == hub]
            if not hub_rt.empty:
                profile["rt_daily"] = _daily_period_stats(hub_rt)
                profile["rt_hourly"] = _hourly_records(hub_rt)

        profiles.append(profile)
    return profiles


def _daily_period_stats(df: pd.DataFrame) -> list[dict]:
    """Daily on-peak/off-peak/flat congestion averages."""
    rows = []
    for d, grp in df.groupby("date"):
        mask_on = grp["hour_ending"].isin(ONPEAK_HOURS)
        mask_off = grp["hour_ending"].isin(OFFPEAK_HOURS)
        rows.append({
            "date": str(d),
            "onpk": _sr(grp.loc[mask_on, "lmp_congestion_price"].mean()),
            "offpk": _sr(grp.loc[mask_off, "lmp_congestion_price"].mean()),
            "flat": _sr(grp["lmp_congestion_price"].mean()),
            "max_he": _sr(grp["lmp_congestion_price"].max()),
        })
    return rows


def _hourly_records(df: pd.DataFrame) -> list[dict]:
    """Hourly congestion records sorted by date/hour."""
    records = []
    for _, row in df.sort_values(["date", "hour_ending"]).iterrows():
        records.append({
            "date": str(row["date"]),
            "hour_ending": int(row["hour_ending"]),
            "cong": _sr(row["lmp_congestion_price"]),
            "total": _sr(row["lmp_total"]),
        })
    return records


# -- Cross-hub congestion spread ------------------------------------------


def _build_congestion_spread(df: pd.DataFrame) -> list[dict]:
    """Daily congestion spread between hubs — shows regional constraint asymmetry.

    Computes East-West and Dom-AEP differentials.
    """
    rows = []
    for d, grp in df.groupby("date"):
        hub_cong: dict[str, float] = {}
        for hub in HUB_ORDER:
            hdf = grp[grp["hub"] == hub]
            if not hdf.empty:
                mask_on = hdf["hour_ending"].isin(ONPEAK_HOURS)
                hub_cong[hub] = _sr(hdf.loc[mask_on, "lmp_congestion_price"].mean())

        row: dict = {"date": str(d)}
        # East - West spread (positive = eastern congestion premium)
        if "EASTERN HUB" in hub_cong and "WESTERN HUB" in hub_cong:
            e = hub_cong["EASTERN HUB"]
            w = hub_cong["WESTERN HUB"]
            row["east_west_onpk"] = _sr(e - w) if e is not None and w is not None else None
        # Dominion - AEP spread
        if "DOMINION HUB" in hub_cong and "AEP GEN HUB" in hub_cong:
            d_val = hub_cong["DOMINION HUB"]
            a_val = hub_cong["AEP GEN HUB"]
            row["dom_aep_onpk"] = _sr(d_val - a_val) if d_val is not None and a_val is not None else None
        # Eastern - Dominion spread
        if "EASTERN HUB" in hub_cong and "DOMINION HUB" in hub_cong:
            e = hub_cong["EASTERN HUB"]
            d_val = hub_cong["DOMINION HUB"]
            row["east_dom_onpk"] = _sr(e - d_val) if e is not None and d_val is not None else None

        for hub in HUB_ORDER:
            row[HUB_SHORT[hub] + "_onpk"] = hub_cong.get(hub)

        rows.append(row)
    return rows


# -- Helpers --------------------------------------------------------------


def _sr(val, decimals: int = 2) -> float | None:
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
    from src.data import lmps_hourly
    from src.utils.cache_utils import pull_with_cache

    logging.basicConfig(level=logging.INFO)

    CACHE = dict(
        cache_dir=configs.CACHE_DIR,
        cache_enabled=configs.CACHE_ENABLED,
        ttl_hours=configs.CACHE_TTL_HOURS,
        force_refresh=configs.FORCE_CACHE_REFRESH,
    )

    df_da = pull_with_cache(
        source_name="pjm_lmps_hourly_da",
        pull_fn=lmps_hourly.pull,
        pull_kwargs={"schema": configs.SCHEMA, "market": "da"},
        **CACHE,
    )
    df_rt = pull_with_cache(
        source_name="pjm_lmps_hourly_rt",
        pull_fn=lmps_hourly.pull,
        pull_kwargs={"schema": configs.SCHEMA, "market": "rt"},
        **CACHE,
    )

    vm = build_view_model(df_da, df_rt)
    print(json.dumps(vm, indent=2, default=str))
