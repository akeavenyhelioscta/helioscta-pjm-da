"""View model: LightGBM QR strip forecast results (D+1 through D+N).

Takes the dict returned by ``strip_forecast.run_strip()`` and produces
structured output with per-date summaries, quantile bands, hourly detail.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = list(range(1, 8)) + [24]
_BAND_QUANTILES = ["P01", "P05", "P10", "P25", "P50", "P75", "P90", "P95", "P99"]


def build_view_model(pipeline_result: dict) -> dict:
    if "error" in pipeline_result:
        return {"error": pipeline_result["error"]}

    strip_table: pd.DataFrame = pipeline_result["strip_table"]
    quantiles_table: pd.DataFrame = pipeline_result["quantiles_table"]
    reference_date: str = pipeline_result["reference_date"]
    forecast_dates: list[str] = pipeline_result["forecast_dates"]
    per_day: dict = pipeline_result.get("per_day", {})
    model_info: dict = pipeline_result.get("model_info", {})

    strip: list[dict] = []
    for fd_str in forecast_dates:
        fd = pd.to_datetime(fd_str).date()
        day_meta = per_day.get(fd_str, {})
        offset = day_meta.get("offset", 0)
        has_actuals = day_meta.get("has_actuals", False)

        fc_rows = strip_table[
            (strip_table["Date"].astype(str) == fd_str)
            & (strip_table["Type"] == "Forecast")
        ]
        act_rows = strip_table[
            (strip_table["Date"].astype(str) == fd_str)
            & (strip_table["Type"] == "Actual")
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

        # Quantile bands
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
            "summary": summary,
            "bands": bands,
            "hourly": hourly,
        })

    return {
        "reference_date": reference_date,
        "forecast_dates": forecast_dates,
        "model_type": "lightgbm_quantile",
        "model_info": model_info,
        "strip": strip,
    }


# ── Helpers ────────────────────────────────────────────────────────


def _sf(val) -> float | None:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    try:
        f = float(val)
        return None if np.isnan(f) else f
    except (TypeError, ValueError):
        return None


def _sr(val, decimals: int = 2) -> float | None:
    f = _sf(val)
    return round(f, decimals) if f is not None else None


def _period_avg(row: pd.Series, hours: list[int]) -> float | None:
    vals = []
    for h in hours:
        v = _sf(row.get(f"HE{h}"))
        if v is not None:
            vals.append(v)
    return np.mean(vals) if vals else None
