"""View model: Generation outage forecast vintages.

Rows = forecast execution dates (vintages), most recent first.
Columns = forecast dates (next ~7 days).
Values = MW by outage type (total, forced, planned, maintenance).

Mirrors the HTML heatmap from outages_forecast_rto.py but as structured
data for API / agent consumption.

Consumed by:
  - API endpoints (JSON / markdown)
  - Agent (outage trend context — are outages rising or falling?)
"""
from __future__ import annotations

import logging
from datetime import date

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

OUTAGE_TYPES = [
    ("total_outages_mw", "Total Outages"),
    ("forced_outages_mw", "Forced Outages"),
    ("planned_outages_mw", "Planned Outages"),
    ("maintenance_outages_mw", "Maint Outages"),
]

# How many execution dates (vintages) to keep
MAX_VINTAGES = 8


def build_view_model(df: pd.DataFrame, region: str = "RTO") -> dict:
    """Build the outage forecast vintage view model.

    Parameters
    ----------
    df : pd.DataFrame
        Output of ``outages_forecast_daily.pull()`` with columns:
        forecast_rank, forecast_execution_date, forecast_date,
        forecast_day_number, region, total_outages_mw,
        planned_outages_mw, maintenance_outages_mw, forced_outages_mw.
    region : str
        Region to filter to (default "RTO").
    """
    if df is None or df.empty:
        return {"error": "No outage forecast data available."}

    df = df.copy()
    df["forecast_execution_date"] = pd.to_datetime(df["forecast_execution_date"]).dt.date
    df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date

    # Filter to region
    if region and "region" in df.columns:
        df = df[df["region"] == region]

    if df.empty:
        return {"error": f"No outage forecast data for region {region}."}

    # Keep highest-rank (most recent) forecast per execution_date × forecast_date
    df = df.sort_values("forecast_rank", ascending=False)
    df = df.drop_duplicates(subset=["forecast_execution_date", "forecast_date"], keep="first")

    # Get the last N execution dates
    exec_dates = sorted(df["forecast_execution_date"].unique(), reverse=True)[:MAX_VINTAGES]
    df = df[df["forecast_execution_date"].isin(exec_dates)]

    if df.empty:
        return {"error": "No recent forecast vintages found."}

    forecast_dates = sorted(df["forecast_date"].unique())

    # Label execution dates
    vintage_labels = {}
    for i, ed in enumerate(exec_dates):
        if i == 0:
            vintage_labels[ed] = "Current Forecast"
        elif i == 1:
            vintage_labels[ed] = "24hrs Ago"
        else:
            vintage_labels[ed] = pd.Timestamp(ed).strftime("%a %b-%d")

    # Build vintage matrix per outage type
    outage_tables: dict[str, dict] = {}
    for col, label in OUTAGE_TYPES:
        if col not in df.columns:
            continue

        matrix: dict[str, dict[str, int | None]] = {}
        for ed in exec_dates:
            ed_str = str(ed)
            matrix[ed_str] = {"label": vintage_labels[ed]}
            ed_data = df[df["forecast_execution_date"] == ed]
            for fd in forecast_dates:
                match = ed_data[ed_data["forecast_date"] == fd]
                val = _sr(match.iloc[0][col]) if len(match) > 0 else None
                matrix[ed_str][str(fd)] = val

        # Compute vintage deltas (current vs 24hrs ago)
        delta = None
        if len(exec_dates) >= 2:
            curr_ed, prev_ed = exec_dates[0], exec_dates[1]
            curr_data = df[df["forecast_execution_date"] == curr_ed]
            prev_data = df[df["forecast_execution_date"] == prev_ed]
            # Compare on shared forecast dates
            shared = set(curr_data["forecast_date"]) & set(prev_data["forecast_date"])
            if shared:
                curr_avg = curr_data[curr_data["forecast_date"].isin(shared)][col].mean()
                prev_avg = prev_data[prev_data["forecast_date"].isin(shared)][col].mean()
                delta = _sr(curr_avg - prev_avg)

        outage_tables[col] = {
            "label": label,
            "matrix": matrix,
            "delta_vs_prior": delta,
        }

    return {
        "region": region,
        "vintage_dates": [str(ed) for ed in exec_dates],
        "forecast_dates": [str(fd) for fd in forecast_dates],
        "outage_types": outage_tables,
    }


def _sr(val, decimals: int = 0) -> int | None:
    """Safe round — returns None for NaN/None."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if np.isnan(f) else round(f, decimals)
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    import json
    import logging

    import src.settings  # noqa: F401 — load env vars

    from src.like_day_forecast import configs
    from src.data import outages_forecast_daily
    from src.utils.cache_utils import pull_with_cache

    logging.basicConfig(level=logging.INFO)

    CACHE = dict(
        cache_dir=configs.CACHE_DIR,
        cache_enabled=configs.CACHE_ENABLED,
        ttl_hours=configs.CACHE_TTL_HOURS,
        force_refresh=configs.FORCE_CACHE_REFRESH,
    )

    df = pull_with_cache(
        source_name="outages_forecast_daily",
        pull_fn=outages_forecast_daily.pull,
        pull_kwargs={"lookback_days": 14},
        **CACHE,
    )

    vm = build_view_model(df, region="RTO")
    print(json.dumps(vm, indent=2, default=str))
