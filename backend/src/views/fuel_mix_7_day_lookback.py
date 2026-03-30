"""View model: Hourly fuel mix (generation by type) — last 7 days.

Mirrors the structure of ``lmp_7_day_lookback_western_hub`` but for
generation data.  Includes daily period summaries, full hourly detail,
and hour-over-hour ramps for dispatchable fuels (gas, coal).
"""
from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd

FUEL_COLS = [
    "gas", "coal", "nuclear", "solar", "wind",
    "hydro", "oil", "storage", "other_renewables", "other", "multiple_fuels",
]

# On-peak = HE 8-23, off-peak = HE 1-7 + HE 24
ON_PEAK = set(range(8, 24))
RAMP_FUELS = ["gas", "coal"]


def _sr(val, decimals: int = 0):
    """Safe round — returns None for NaN/None, else rounded value."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    return round(float(val), decimals)


def build_view_model(df: pd.DataFrame) -> dict:
    """Build the fuel mix 7-day lookback view model.

    Parameters
    ----------
    df : pd.DataFrame
        Output of ``fuel_mix_hourly.pull()`` with columns
        ``date``, ``hour_ending``, and one column per fuel type.
    """
    if df is None or df.empty:
        return {"error": "No fuel mix data available."}

    df = df.copy()

    # ── Normalize ────────────────────────────────────────────────
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["hour_ending"] = df["hour_ending"].astype(int)
    # hour 0 → hour 24 (end-of-day convention)
    df.loc[df["hour_ending"] == 0, "hour_ending"] = 24

    # Filter to last 7 days
    max_date = df["date"].max()
    min_date = max_date - dt.timedelta(days=6)
    df = df[(df["date"] >= min_date) & (df["date"] <= max_date)]

    if df.empty:
        return {"error": "No fuel mix data in the last 7 days."}

    # Ensure fuel columns are numeric
    for col in FUEL_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Total generation
    available_fuels = [c for c in FUEL_COLS if c in df.columns]
    df["total"] = df[available_fuels].sum(axis=1)

    all_cols = available_fuels + ["total"]

    # ── Hourly records ───────────────────────────────────────────
    hourly = []
    for _, row in df.sort_values(["date", "hour_ending"]).iterrows():
        rec = {
            "date": str(row["date"]),
            "hour_ending": int(row["hour_ending"]),
        }
        for col in all_cols:
            rec[col] = _sr(row.get(col))
        hourly.append(rec)

    # ── Daily summary (on-peak / off-peak / flat) ────────────────
    df["period"] = df["hour_ending"].apply(lambda h: "on_peak" if h in ON_PEAK else "off_peak")

    daily_summary = {}
    for d, dg in df.groupby("date"):
        day_entry = {}
        for col in all_cols:
            on = dg.loc[dg["period"] == "on_peak", col]
            off = dg.loc[dg["period"] == "off_peak", col]
            day_entry[col] = {
                "on_peak": _sr(on.mean()),
                "off_peak": _sr(off.mean()),
                "flat": _sr(dg[col].mean()),
            }
        daily_summary[str(d)] = day_entry

    # ── Ramps (hour-over-hour change for dispatchable fuels) ─────
    ramp_cols = [c for c in RAMP_FUELS if c in df.columns]
    ramps = []
    for d, dg in df.sort_values(["date", "hour_ending"]).groupby("date"):
        diffs = dg[ramp_cols].diff()
        for i, (idx, row) in enumerate(diffs.iterrows()):
            he = int(dg.loc[idx, "hour_ending"])
            rec = {"date": str(d), "hour_ending": he}
            for col in ramp_cols:
                rec[col] = _sr(row[col]) if i > 0 else None  # first hour has no prior
            ramps.append(rec)

    return {
        "date_range": {
            "start": str(min_date),
            "end": str(max_date),
        },
        "fuel_types": available_fuels,
        "hourly": hourly,
        "daily_summary": daily_summary,
        "ramps": ramps,
    }


if __name__ == "__main__":
    import json
    import logging

    import src.settings  # noqa: F401 — load env vars

    from src.like_day_forecast import configs
    from src.data import fuel_mix_hourly
    from src.utils.cache_utils import pull_with_cache

    logging.basicConfig(level=logging.INFO)

    CACHE = dict(
        cache_dir=configs.CACHE_DIR,
        cache_enabled=configs.CACHE_ENABLED,
        ttl_hours=configs.CACHE_TTL_HOURS,
        force_refresh=configs.FORCE_CACHE_REFRESH,
    )

    df = pull_with_cache(
        source_name="fuel_mix_hourly",
        pull_fn=fuel_mix_hourly.pull,
        pull_kwargs={},
        **CACHE,
    )

    vm = build_view_model(df)
    print(json.dumps(vm, indent=2, default=str))
