"""View model: Like-day strip forecast results (D+1 through D+N).

Takes the dict returned by ``strip_forecast.run_strip()`` and produces
structured output with analog days, per-date summaries with P10/P90 bands,
and hourly detail per forecast date.

Consumed by:
  - API endpoints (JSON / markdown)
  - Agent (multi-day forecast context, term structure shape)
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = list(range(1, 8)) + [24]

# Quantile bands to include in the view (keep compact)
_BAND_QUANTILES = ["P10", "P25", "P50", "P75", "P90"]


def build_view_model(pipeline_result: dict) -> dict:
    """Transform strip forecast pipeline output into a structured view model.

    Args:
        pipeline_result: Dict returned by ``run_strip()``.
            Required keys: strip_table, quantiles_table, analogs,
            reference_date, forecast_dates, per_day.
    """
    if "error" in pipeline_result:
        return {"error": pipeline_result["error"]}

    strip_table: pd.DataFrame = pipeline_result["strip_table"]
    quantiles_table: pd.DataFrame = pipeline_result["quantiles_table"]
    analogs_df: pd.DataFrame | None = pipeline_result.get("analogs")
    reference_date: str = pipeline_result["reference_date"]
    forecast_dates: list[str] = pipeline_result["forecast_dates"]
    per_day: dict = pipeline_result.get("per_day", {})

    # ── Analog days ──────────────────────────────────────────────────
    analogs: list[dict] | None = None
    if analogs_df is not None and len(analogs_df) > 0:
        analogs = []
        for _, arow in analogs_df.iterrows():
            analogs.append({
                "date": str(arow["date"]),
                "rank": int(arow["rank"]) if "rank" in arow.index else None,
                "distance": _sr(arow.get("distance"), 4),
                "similarity": _sr(arow.get("similarity"), 4),
                "weight": _sr(arow.get("weight"), 4),
            })

    # ── Per-date strip entries ───────────────────────────────────────
    strip: list[dict] = []
    for fd_str in forecast_dates:
        fd = pd.to_datetime(fd_str).date()
        day_meta = per_day.get(fd_str, {})
        offset = day_meta.get("offset", 0)
        has_actuals = day_meta.get("has_actuals", False)
        n_used = day_meta.get("n_analogs_used", 0)

        # Extract forecast row
        fc_rows = strip_table[
            (strip_table["Date"].astype(str) == fd_str) & (strip_table["Type"] == "Forecast")
        ]
        act_rows = strip_table[
            (strip_table["Date"].astype(str) == fd_str) & (strip_table["Type"] == "Actual")
        ]

        fc = fc_rows.iloc[0] if len(fc_rows) > 0 else None
        act = act_rows.iloc[0] if len(act_rows) > 0 else None

        # Hourly detail
        hourly: list[dict] = []
        for h in range(1, 25):
            he = f"HE{h}"
            fcst = _sf(fc[he]) if fc is not None else None
            actual = _sf(act[he]) if act is not None else None
            error = round(fcst - actual, 2) if (fcst is not None and actual is not None) else None
            hourly.append({
                "hour": h,
                "period": "on_peak" if h in ONPEAK_HOURS else "off_peak",
                "forecast": _sr(fcst),
                "actual": _sr(actual),
                "error": error,
            })

        # Period summary
        summary = {}
        for pkey, phours in [("on_peak", ONPEAK_HOURS), ("off_peak", OFFPEAK_HOURS), ("flat", list(range(1, 25)))]:
            f_vals = [hr["forecast"] for hr in hourly if hr["hour"] in phours and hr["forecast"] is not None]
            a_vals = [hr["actual"] for hr in hourly if hr["hour"] in phours and hr["actual"] is not None]
            e_vals = [hr["error"] for hr in hourly if hr["hour"] in phours and hr["error"] is not None]
            summary[pkey] = {
                "forecast": _sr(np.mean(f_vals)) if f_vals else None,
                "actual": _sr(np.mean(a_vals)) if a_vals else None,
                "error": _sr(np.mean(e_vals)) if e_vals else None,
            }

        # Quantile bands (period-level only for compactness)
        bands: dict[str, dict] = {}
        fd_quants = quantiles_table[quantiles_table["Date"].astype(str) == fd_str]
        for _, qrow in fd_quants.iterrows():
            band_name = qrow["Type"]
            if band_name not in _BAND_QUANTILES:
                continue
            bands[band_name] = {
                "on_peak": _sr(_period_avg(qrow, ONPEAK_HOURS)),
                "off_peak": _sr(_period_avg(qrow, OFFPEAK_HOURS)),
                "flat": _sr(_period_avg(qrow, list(range(1, 25)))),
            }

        # Add P10/P90 to hourly for drill-down
        p10_row = fd_quants[fd_quants["Type"] == "P10"]
        p90_row = fd_quants[fd_quants["Type"] == "P90"]
        if len(p10_row) > 0 and len(p90_row) > 0:
            p10 = p10_row.iloc[0]
            p90 = p90_row.iloc[0]
            for hr in hourly:
                h = hr["hour"]
                hr["p10"] = _sr(_sf(p10[f"HE{h}"]))
                hr["p90"] = _sr(_sf(p90[f"HE{h}"]))

        strip.append({
            "date": fd_str,
            "offset": offset,
            "has_actuals": has_actuals,
            "n_analogs_used": n_used,
            "summary": summary,
            "bands": bands,
            "hourly": hourly,
        })

    return {
        "reference_date": reference_date,
        "forecast_dates": forecast_dates,
        "n_analogs_used": strip[0]["n_analogs_used"] if strip else 0,
        "analogs": analogs,
        "strip": strip,
    }


# ── Helpers ─────────────────────────────────────────────────────────


def _sf(val) -> float | None:
    """Safe float — return None for NaN/None."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    try:
        f = float(val)
        return None if np.isnan(f) else f
    except (TypeError, ValueError):
        return None


def _sr(val, decimals: int = 2) -> float | None:
    """Safe round — return None for NaN/None."""
    f = _sf(val)
    return round(f, decimals) if f is not None else None


def _period_avg(row: pd.Series, hours: list[int]) -> float | None:
    """Compute average across HE columns for a set of hours."""
    vals = []
    for h in hours:
        v = _sf(row.get(f"HE{h}"))
        if v is not None:
            vals.append(v)
    return np.mean(vals) if vals else None


if __name__ == "__main__":
    import json
    import logging

    import src.settings  # noqa: F401 — load env vars

    from src.like_day_forecast.pipelines.strip_forecast import run_strip
    from src.like_day_forecast import configs

    logging.basicConfig(level=logging.INFO)

    result = run_strip(
        horizon=3,
        config=configs.ScenarioConfig(),
    )

    vm = build_view_model(result)
    print(json.dumps(vm, indent=2, default=str))
