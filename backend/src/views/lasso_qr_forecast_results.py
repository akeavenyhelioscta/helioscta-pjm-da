"""View model: LASSO QR single-day forecast results.

Takes the dict returned by ``lasso_qr forecast.run()`` and produces
structured output matching the like-day view contract plus model_info.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = list(range(1, 8)) + [24]

ERROR_THRESHOLDS = {"acceptable": 5.0, "warning": 15.0}

HE_COLS = [f"HE{h}" for h in range(1, 25)]
SUMMARY_COLS = ["OnPeak", "OffPeak", "Flat"]


def build_view_model(pipeline_result: dict) -> dict:
    if "error" in pipeline_result:
        return {"error": pipeline_result["error"]}

    output_table: pd.DataFrame = pipeline_result["output_table"]
    quantiles_table: pd.DataFrame = pipeline_result["quantiles_table"]
    forecast_date: str = pipeline_result["forecast_date"]
    reference_date: str = pipeline_result["reference_date"]
    has_actuals: bool = pipeline_result["has_actuals"]
    model_info: dict = pipeline_result.get("model_info", {})
    metrics: dict | None = pipeline_result.get("metrics")

    # ── Extract source rows ─────────────────────────────────────────
    forecast_row = output_table[output_table["Type"] == "Forecast"].iloc[0]
    actual_row = (
        output_table[output_table["Type"] == "Actual"].iloc[0]
        if has_actuals
        else None
    )

    # Quantile maps for outlier detection
    q25_map: dict[int, float] = {}
    q75_map: dict[int, float] = {}
    q25_rows = quantiles_table[quantiles_table["Type"] == "P25"]
    q75_rows = quantiles_table[quantiles_table["Type"] == "P75"]
    if len(q25_rows) > 0:
        q25_row = q25_rows.iloc[0]
        q25_map = {h: _sf(q25_row[f"HE{h}"]) for h in range(1, 25)}
    if len(q75_rows) > 0:
        q75_row = q75_rows.iloc[0]
        q75_map = {h: _sf(q75_row[f"HE{h}"]) for h in range(1, 25)}

    # ── Per-hour structured data ────────────────────────────────────
    hourly: list[dict] = []
    hours_outside_iqr: list[int] = []

    for h in range(1, 25):
        he = f"HE{h}"
        fcst = _sf(forecast_row[he])
        act = _sf(actual_row[he]) if actual_row is not None else None
        error = round(fcst - act, 2) if (fcst is not None and act is not None) else None

        severity = _error_severity(error)

        p25 = _sr(q25_map.get(h))
        p75 = _sr(q75_map.get(h))
        outside_iqr = False
        if act is not None and p25 is not None and p75 is not None:
            outside_iqr = act < p25 or act > p75
            if outside_iqr:
                hours_outside_iqr.append(h)

        # All quantile values for this hour
        quantiles: dict[str, float | None] = {}
        for _, qrow in quantiles_table.iterrows():
            band = qrow["Type"]
            quantiles[band] = _sr(qrow[he])

        hourly.append({
            "hour": h,
            "period": "on_peak" if h in ONPEAK_HOURS else "off_peak",
            "forecast": _sr(fcst),
            "actual": _sr(act),
            "error": error,
            "error_severity": severity,
            "p25": p25,
            "p75": p75,
            "outside_iqr": outside_iqr,
            "quantiles": quantiles,
        })

    # ── Period summaries ────────────────────────────────────────────
    summary = {
        "on_peak": _period_summary(hourly, ONPEAK_HOURS),
        "off_peak": _period_summary(hourly, OFFPEAK_HOURS),
        "flat": _period_summary(hourly, list(range(1, 25))),
    }

    # ── Quantile band rows (for table display) ─────────────────────
    bands: list[dict] = []
    for _, qrow in quantiles_table.iterrows():
        band_entry: dict = {"band": qrow["Type"]}
        for he in HE_COLS:
            band_entry[he] = _sr(qrow[he])
        for sc in SUMMARY_COLS:
            band_entry[sc] = _sr(qrow[sc])
        bands.append(band_entry)

    # ── Diff rows (Forecast-Actual) ─────────────────────────────────
    diffs: dict[str, dict] = {}
    if has_actuals:
        fa: dict[str, float | None] = {}
        for h in range(1, 25):
            fa[f"HE{h}"] = hourly[h - 1]["error"]
        for sc in SUMMARY_COLS:
            fv = _sf(forecast_row[sc])
            av = _sf(actual_row[sc]) if actual_row is not None else None
            fa[sc] = round(fv - av, 2) if (fv is not None and av is not None) else None
        diffs["forecast_minus_actual"] = fa

    # ── Quantile coverage stats ─────────────────────────────────────
    quantile_coverage = {
        "hours_outside_iqr": hours_outside_iqr,
        "coverage_80": metrics.get("coverage_80pct") if metrics else None,
        "coverage_90": metrics.get("coverage_90pct") if metrics else None,
        "coverage_98": metrics.get("coverage_98pct") if metrics else None,
        "sharpness_90": _sr(metrics.get("sharpness_90pct")) if metrics else None,
    }

    return {
        "forecast_date": forecast_date,
        "reference_date": reference_date,
        "has_actuals": has_actuals,
        "model_type": "lasso_quantile_regression",
        "model_info": model_info,
        "hourly": hourly,
        "summary": summary,
        "bands": bands,
        "diffs": diffs,
        "metrics": metrics,
        "quantile_coverage": quantile_coverage,
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


def _error_severity(error: float | None) -> str | None:
    if error is None:
        return None
    abs_err = abs(error)
    if abs_err < ERROR_THRESHOLDS["acceptable"]:
        return "acceptable"
    elif abs_err < ERROR_THRESHOLDS["warning"]:
        return "warning"
    return "bad"


def _period_summary(hourly: list[dict], hours: list[int]) -> dict:
    f_vals = [hr["forecast"] for hr in hourly if hr["hour"] in hours and hr["forecast"] is not None]
    a_vals = [hr["actual"] for hr in hourly if hr["hour"] in hours and hr["actual"] is not None]
    e_vals = [hr["error"] for hr in hourly if hr["hour"] in hours and hr["error"] is not None]
    return {
        "forecast": round(np.mean(f_vals), 2) if f_vals else None,
        "actual": round(np.mean(a_vals), 2) if a_vals else None,
        "error": round(np.mean(e_vals), 2) if e_vals else None,
    }


def _period_avg(row: pd.Series, hours: list[int]) -> float | None:
    vals = []
    for h in hours:
        v = _sf(row.get(f"HE{h}"))
        if v is not None:
            vals.append(v)
    return np.mean(vals) if vals else None
