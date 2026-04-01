"""View model for DA / RT / DART LMP history — Western Hub, last 7 days.

Merges DA and RT LMP pulls on (hub, date, hour_ending), computes DART
(DA minus RT), and produces structured summaries with on-peak / off-peak /
flat averages for each market section.

Consumed by:
  - API endpoints (JSON)
  - Agent (structured context for price inspection)
"""
import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

ONPEAK_HOURS = list(range(8, 24))   # HE8–HE23
OFFPEAK_HOURS = list(range(1, 8)) + [24]  # HE1–HE7, HE24

_PRICE_COLS = ["lmp_total", "lmp_system_energy_price", "lmp_congestion_price"]

_MERGE_KEYS = ["hub", "date", "hour_ending"]


def build_view_model(df_da: pd.DataFrame, df_rt: pd.DataFrame) -> dict:
    """Build DA / RT / DART LMP view model.

    Args:
        df_da: DA market LMP data from lmps.pull(market="da").
        df_rt: RT market LMP data from lmps.pull(market="rt").

    Returns:
        Structured dict with da, rt, and dart sections.
    """
    da_ok = df_da is not None and len(df_da) > 0
    rt_ok = df_rt is not None and len(df_rt) > 0

    if not da_ok and not rt_ok:
        return {"error": "No LMP data available"}

    cutoff = date.today() - timedelta(days=7)

    df_da = _normalize(df_da, cutoff) if da_ok else pd.DataFrame()
    df_rt = _normalize(df_rt, cutoff) if rt_ok else pd.DataFrame()

    da_ok = len(df_da) > 0
    rt_ok = len(df_rt) > 0

    result: dict = {
        "hub": df_da["hub"].iloc[0] if da_ok else (df_rt["hub"].iloc[0] if rt_ok else None),
        "date_range": {
            "start": str(cutoff),
            "end": str(date.today()),
        },
    }

    if da_ok:
        result["da"] = _build_market_section(df_da)
    if rt_ok:
        result["rt"] = _build_market_section(df_rt)

    if da_ok and rt_ok:
        result["dart"] = _build_dart_section(df_da, df_rt)

    return result


# ── Normalization ────────────────────────────────────────────────


def _normalize(df: pd.DataFrame, cutoff: date) -> pd.DataFrame:
    """Ensure consistent dtypes and apply the 7-day lookback filter."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["hour_ending"] = pd.to_numeric(df["hour_ending"], errors="coerce").astype(int)
    for col in _PRICE_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["date", "hour_ending", "lmp_total"])
    df = df[df["date"] >= cutoff]
    return df


# ── Market section (DA or RT) ───────────────────────────────────


def _build_market_section(df: pd.DataFrame) -> dict:
    """Hourly detail + daily period summaries for a single market."""
    hourly = (
        df[["date", "hour_ending"] + _PRICE_COLS]
        .sort_values(["date", "hour_ending"])
        .to_dict(orient="records")
    )
    # Make dates JSON-serializable
    for row in hourly:
        row["date"] = str(row["date"])
        for col in _PRICE_COLS:
            row[col] = _sr(row[col], decimals=2)

    daily = {}
    for d, grp in df.groupby("date"):
        daily[str(d)] = _period_summaries(grp)

    return {
        "hourly": hourly,
        "daily_summary": daily,
    }


# ── DART section ─────────────────────────────────────────────────


def _build_dart_section(df_da: pd.DataFrame, df_rt: pd.DataFrame) -> dict:
    """Merge DA and RT, compute DART = DA - RT, return same structure."""
    merged = df_da.merge(df_rt, on=_MERGE_KEYS, suffixes=("_da", "_rt"), how="inner")

    dart_rows = []
    for _, row in merged.iterrows():
        dart_rows.append({
            "hub": row["hub"],
            "date": row["date"],
            "hour_ending": row["hour_ending"],
            "lmp_total": row["lmp_total_da"] - row["lmp_total_rt"],
            "lmp_system_energy_price": row["lmp_system_energy_price_da"] - row["lmp_system_energy_price_rt"],
            "lmp_congestion_price": row["lmp_congestion_price_da"] - row["lmp_congestion_price_rt"],
        })

    df_dart = pd.DataFrame(dart_rows)
    if len(df_dart) == 0:
        return {"hourly": [], "daily_summary": {}}

    return _build_market_section(df_dart)


# ── Helpers ──────────────────────────────────────────────────────


def _period_summaries(grp: pd.DataFrame) -> dict:
    """On-peak, off-peak, flat averages for each price component."""
    mask_on = grp["hour_ending"].isin(ONPEAK_HOURS)
    mask_off = grp["hour_ending"].isin(OFFPEAK_HOURS)

    summary = {}
    for col in _PRICE_COLS:
        summary[col] = {
            "on_peak": _sr(grp.loc[mask_on, col].mean(), decimals=2),
            "off_peak": _sr(grp.loc[mask_off, col].mean(), decimals=2),
            "flat": _sr(grp[col].mean(), decimals=2),
        }
    return summary


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
