"""View model for outage term bible — historical context for outage levels.

Takes a historical daily outage DataFrame and produces structured context:
current level vs seasonal norms, percentile rank, decomposition by type,
year-over-year comparison, trend, and annotations.

Consumed by:
  - reporting/fragments/outages_term_bible.py (HTML rendering)
  - API endpoints (JSON)
  - Agent (structured context)
"""
import logging
from datetime import date

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

OUTAGE_TYPES = [
    ("total_outages", "total_outages_mw"),
    ("forced_outages", "forced_outages_mw"),
    ("planned_outages", "planned_outages_mw"),
    ("maintenance_outages", "maintenance_outages_mw"),
]

MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def build_view_model(df: pd.DataFrame, reference_date: date | None = None) -> dict:
    """Transform historical outage data into a structured view model.

    Args:
        df: Historical daily outage data with columns: date,
            total_outages_mw, forced_outages_mw, planned_outages_mw,
            maintenance_outages_mw.
        reference_date: Date to anchor the analysis. Defaults to today.

    Returns:
        Structured dict with: per-type current level and seasonal context,
        year-month heatmap data, current month daily values, and annotations.
    """
    if df is None or len(df) == 0:
        return {"error": "No historical outage data available"}

    if reference_date is None:
        reference_date = date.today()

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day

    current_month = reference_date.month
    current_year = reference_date.year
    month_name = MONTHS[current_month - 1]

    # ── Per outage type analysis ────────────────────────────────────
    outage_types: dict[str, dict] = {}
    for type_key, col in OUTAGE_TYPES:
        if col not in df.columns:
            continue
        outage_types[type_key] = _analyze_outage_type(
            df, col, current_month, current_year, reference_date,
        )

    # ── Year-month heatmap data (for all types) ────────────────────
    heatmap: dict[str, dict] = {}
    for type_key, col in OUTAGE_TYPES:
        if col not in df.columns:
            continue
        heatmap[type_key] = _build_heatmap(df, col)

    # ── Current month daily values (year-over-year) ─────────────────
    current_month_daily: dict[str, dict] = {}
    for type_key, col in OUTAGE_TYPES:
        if col not in df.columns:
            continue
        current_month_daily[type_key] = _build_current_month_daily(
            df, col, current_month,
        )

    return {
        "reference_date": str(reference_date),
        "current_month": month_name,
        "outage_types": outage_types,
        "heatmap": heatmap,
        "current_month_daily": current_month_daily,
    }


# ── Per-type analysis ───────────────────────────────────────────────


def _analyze_outage_type(
    df: pd.DataFrame,
    col: str,
    current_month: int,
    current_year: int,
    reference_date: date,
) -> dict:
    """Compute current level, seasonal context, YoY, and trend for one type."""
    # Latest available value
    latest_row = df.loc[df["date"].idxmax()]
    latest_mw = _safe_round(latest_row[col], 0)

    # Historical distribution for this month (all years)
    month_history = df[df["month"] == current_month][col].dropna()
    month_avg = _safe_round(month_history.mean(), 0)
    month_std = _safe_round(month_history.std(), 0)
    month_max = _safe_round(month_history.max(), 0)
    month_min = _safe_round(month_history.min(), 0)

    percentile = None
    z_score = None
    if len(month_history) > 0 and latest_mw is not None:
        percentile = round(float((month_history < latest_mw).mean()), 2)
        if month_std and month_std > 0:
            z_score = round((latest_mw - month_avg) / month_std, 2)

    # Year-over-year: average for current month per year
    yoy: dict[int, float] = {}
    month_by_year = df[df["month"] == current_month].groupby("year")[col].mean()
    for year, val in month_by_year.items():
        yoy[int(year)] = _safe_round(val, 0)

    # YoY delta: current year vs prior year
    yoy_delta = None
    if current_year in yoy and (current_year - 1) in yoy:
        yoy_delta = round(yoy[current_year] - yoy[current_year - 1], 0)

    # 7-day trend
    recent = df.sort_values("date").tail(7)[col].dropna()
    trend_direction = None
    trend_delta = None
    if len(recent) >= 2:
        trend_delta = round(float(recent.iloc[-1] - recent.iloc[0]), 0)
        trend_direction = "increasing" if trend_delta > 0 else "decreasing" if trend_delta < 0 else "flat"

    return {
        "current_mw": latest_mw,
        "month_context": {
            "month": current_month,
            "avg": month_avg,
            "std": month_std,
            "max": month_max,
            "min": month_min,
            "percentile": percentile,
            "z_score": z_score,
        },
        "year_over_year": yoy,
        "yoy_delta": yoy_delta,
        "trend_7d": {
            "direction": trend_direction,
            "delta_mw": trend_delta,
        },
    }


# ── Heatmap data ────────────────────────────────────────────────────


def _build_heatmap(df: pd.DataFrame, col: str) -> dict:
    """Year x Month average MW matrix with yearly and monthly stats."""
    monthly = df.groupby(["year", "month"])[col].mean().reset_index()
    pivot = monthly.pivot(index="year", columns="month", values=col)

    years = sorted(pivot.index.tolist())

    # Year-month matrix
    matrix: dict[int, dict[int, float | None]] = {}
    for year in years:
        matrix[year] = {}
        for m in range(1, 13):
            val = pivot.loc[year].get(m)
            matrix[year][m] = _safe_round(val, 0)

    # Yearly stats
    yearly_stats: dict[int, dict[str, float | None]] = {}
    ys = df.groupby("year")[col].agg(["mean", "max", "min"])
    for year in ys.index:
        yearly_stats[int(year)] = {
            "avg": _safe_round(ys.loc[year, "mean"], 0),
            "max": _safe_round(ys.loc[year, "max"], 0),
            "min": _safe_round(ys.loc[year, "min"], 0),
        }

    # Monthly stats (across all years)
    monthly_stats: dict[int, dict[str, float | None]] = {}
    ms = monthly.groupby("month")[col].agg(["mean", "max", "min"])
    for m in ms.index:
        monthly_stats[int(m)] = {
            "avg": _safe_round(ms.loc[m, "mean"], 0),
            "max": _safe_round(ms.loc[m, "max"], 0),
            "min": _safe_round(ms.loc[m, "min"], 0),
        }

    return {
        "years": years,
        "matrix": matrix,
        "yearly_stats": yearly_stats,
        "monthly_stats": monthly_stats,
    }


# ── Current month daily ────────────────────────────────────────────


def _build_current_month_daily(
    df: pd.DataFrame, col: str, current_month: int,
) -> dict:
    """Daily values for current month, one series per year."""
    month_data = df[df["month"] == current_month]
    if len(month_data) == 0:
        return {"years": [], "daily": {}, "yearly_avg": {}}

    years = sorted(month_data["year"].unique().tolist())
    pivot = month_data.pivot_table(
        index="day", columns="year", values=col, aggfunc="first",
    )

    # Daily values: {day: {year: mw}}
    daily: dict[int, dict[int, float | None]] = {}
    for day in sorted(pivot.index):
        daily[int(day)] = {}
        for year in years:
            val = pivot.loc[day].get(year)
            daily[int(day)][int(year)] = _safe_round(val, 0)

    # Yearly averages for this month
    yearly_avg: dict[int, float | None] = {}
    avgs = month_data.groupby("year")[col].mean()
    for year, val in avgs.items():
        yearly_avg[int(year)] = _safe_round(val, 0)

    return {
        "years": [int(y) for y in years],
        "daily": daily,
        "yearly_avg": yearly_avg,
    }


# ── Helpers ─────────────────────────────────────────────────────────


def _safe_round(val, decimals: int = 2) -> float | None:
    """Round a value, returning None for NaN/None."""
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
    from src.data import outages_actual_daily
    from src.utils.cache_utils import pull_with_cache

    logging.basicConfig(level=logging.INFO)

    df = pull_with_cache(
        source_name="outages_actual_daily",
        pull_fn=outages_actual_daily.pull,
        pull_kwargs={"schema": configs.SCHEMA},
        cache_dir=configs.CACHE_DIR,
        cache_enabled=configs.CACHE_ENABLED,
        ttl_hours=configs.CACHE_TTL_HOURS,
        force_refresh=configs.FORCE_CACHE_REFRESH,
    )

    vm = build_view_model(df)
    print(json.dumps(vm, indent=2, default=str))
