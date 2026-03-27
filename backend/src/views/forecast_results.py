"""View model for forecast results — structured interpretation of pipeline output.

Takes the dict returned by forecast.run() and produces a structured dict
with explicit domain knowledge: error severity, IQR outlier flags, period
summaries, coverage stats, and annotations.

Consumed by:
  - reporting/fragments/forecast_results.py (HTML rendering)
  - API endpoints (JSON)
  - Agent (structured context)
"""
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

ONPEAK_HOURS = list(range(8, 24))   # HE8–HE23
OFFPEAK_HOURS = list(range(1, 8)) + [24]  # HE1–HE7, HE24

# Error severity thresholds ($/MWh absolute error)
ERROR_THRESHOLDS = {"acceptable": 5.0, "warning": 15.0}

HE_COLS = [f"HE{h}" for h in range(1, 25)]
SUMMARY_COLS = ["OnPeak", "OffPeak", "Flat"]


def build_view_model(pipeline_result: dict) -> dict:
    """Transform raw forecast pipeline output into a structured view model.

    Args:
        pipeline_result: The dict returned by ``forecast.run()``.
            Required keys: output_table, quantiles_table, df_forecast,
            has_actuals, forecast_date, reference_date, n_analogs_used.
            Optional keys: metrics, analogs.

    Returns:
        Structured dict with: hourly data, period summaries, quantile bands,
        diff rows, quantile coverage stats, and annotations.
    """
    if "error" in pipeline_result:
        return {"error": pipeline_result["error"]}

    output_table: pd.DataFrame = pipeline_result["output_table"]
    quantiles_table: pd.DataFrame = pipeline_result["quantiles_table"]
    df_forecast: pd.DataFrame = pipeline_result["df_forecast"]
    has_actuals: bool = pipeline_result["has_actuals"]
    forecast_date: str = pipeline_result["forecast_date"]
    reference_date: str = pipeline_result["reference_date"]
    n_analogs_used: int = pipeline_result["n_analogs_used"]
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
    if "q_0.25" in df_forecast.columns:
        q25_map = dict(zip(df_forecast["hour_ending"].astype(int), df_forecast["q_0.25"]))
    if "q_0.75" in df_forecast.columns:
        q75_map = dict(zip(df_forecast["hour_ending"].astype(int), df_forecast["q_0.75"]))

    # ── Per-hour structured data ────────────────────────────────────
    hourly: list[dict] = []
    hours_outside_iqr: list[int] = []

    for h in range(1, 25):
        he = f"HE{h}"
        fcst = _safe_float(forecast_row[he])
        act = _safe_float(actual_row[he]) if actual_row is not None else None
        error = round(fcst - act, 2) if (fcst is not None and act is not None) else None

        # Domain knowledge: error severity
        severity = _error_severity(error)

        # Domain knowledge: outlier detection (actual outside P25-P75)
        p25 = _safe_round(q25_map.get(h))
        p75 = _safe_round(q75_map.get(h))
        outside_iqr = False
        if act is not None and p25 is not None and p75 is not None:
            outside_iqr = act < p25 or act > p75
            if outside_iqr:
                hours_outside_iqr.append(h)

        # All quantile values for this hour
        quantiles: dict[str, float | None] = {}
        for _, qrow in quantiles_table.iterrows():
            band = qrow["Type"]  # "P01", "P05", etc.
            quantiles[band] = _safe_round(qrow[he])

        hourly.append({
            "hour": h,
            "period": "on_peak" if h in ONPEAK_HOURS else "off_peak",
            "forecast": _safe_round(fcst),
            "actual": _safe_round(act),
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
            band_entry[he] = _safe_round(qrow[he])
        for sc in SUMMARY_COLS:
            band_entry[sc] = _safe_round(qrow[sc])
        bands.append(band_entry)

    # ── Diff rows (Forecast-Actual) ─────────────────────────────────
    diffs: dict[str, dict] = {}
    if has_actuals:
        fa: dict[str, float | None] = {}
        for h in range(1, 25):
            hr = hourly[h - 1]
            fa[f"HE{h}"] = hr["error"]
        for sc in SUMMARY_COLS:
            fv = _safe_float(forecast_row[sc])
            av = _safe_float(actual_row[sc]) if actual_row is not None else None
            fa[sc] = round(fv - av, 2) if (fv is not None and av is not None) else None
        diffs["forecast_minus_actual"] = fa

    # ── Quantile coverage stats ─────────────────────────────────────
    quantile_coverage = {
        "hours_outside_iqr": hours_outside_iqr,
        "coverage_80": metrics.get("coverage_80pct") if metrics else None,
        "coverage_90": metrics.get("coverage_90pct") if metrics else None,
        "coverage_98": metrics.get("coverage_98pct") if metrics else None,
        "sharpness_90": _safe_round(metrics.get("sharpness_90pct")) if metrics else None,
    }

    return {
        "forecast_date": forecast_date,
        "reference_date": reference_date,
        "has_actuals": has_actuals,
        "n_analogs_used": n_analogs_used,
        "hourly": hourly,
        "summary": summary,
        "bands": bands,
        "diffs": diffs,
        "metrics": metrics,
        "quantile_coverage": quantile_coverage,
    }


# ── Helpers ─────────────────────────────────────────────────────────


def _safe_float(val) -> float | None:
    """Convert a value to float, returning None for NaN/None."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    try:
        f = float(val)
        return None if np.isnan(f) else f
    except (TypeError, ValueError):
        return None


def _safe_round(val, decimals: int = 2) -> float | None:
    """Round a value, returning None for NaN/None."""
    f = _safe_float(val)
    return round(f, decimals) if f is not None else None


def _error_severity(error: float | None) -> str | None:
    """Classify forecast error magnitude."""
    if error is None:
        return None
    abs_err = abs(error)
    if abs_err < ERROR_THRESHOLDS["acceptable"]:
        return "acceptable"
    elif abs_err < ERROR_THRESHOLDS["warning"]:
        return "warning"
    return "bad"


def _period_summary(hourly: list[dict], hours: list[int]) -> dict:
    """Compute average forecast/actual/error for a set of hours."""
    f_vals = [hr["forecast"] for hr in hourly if hr["hour"] in hours and hr["forecast"] is not None]
    a_vals = [hr["actual"] for hr in hourly if hr["hour"] in hours and hr["actual"] is not None]
    e_vals = [hr["error"] for hr in hourly if hr["hour"] in hours and hr["error"] is not None]
    return {
        "forecast": round(np.mean(f_vals), 2) if f_vals else None,
        "actual": round(np.mean(a_vals), 2) if a_vals else None,
        "error": round(np.mean(e_vals), 2) if e_vals else None,
    }


def _compress_hours(hours: list[int]) -> str:
    """Compress a list of hours into range notation.

    Example: [8, 9, 10, 14, 22, 23] → "HE8-10, HE14, HE22-23"
    """
    if not hours:
        return ""
    sorted_hours = sorted(hours)
    ranges: list[str] = []
    start = prev = sorted_hours[0]
    for h in sorted_hours[1:]:
        if h == prev + 1:
            prev = h
        else:
            ranges.append(f"HE{start}" if start == prev else f"HE{start}-{prev}")
            start = prev = h
    ranges.append(f"HE{start}" if start == prev else f"HE{start}-{prev}")
    return ", ".join(ranges)


if __name__ == "__main__":
    import json
    import src.settings  # noqa: F401 — load env vars

    from src.like_day_forecast import configs
    from src.like_day_forecast.pipelines.forecast import run as run_forecast

    logging.basicConfig(level=logging.INFO)

    result = run_forecast(
        forecast_date=None,
        config=configs.ScenarioConfig(),
        cache_dir=configs.CACHE_DIR,
        cache_enabled=configs.CACHE_ENABLED,
        cache_ttl_hours=configs.CACHE_TTL_HOURS,
        force_refresh=configs.FORCE_CACHE_REFRESH,
    )

    vm = build_view_model(result)
    print(json.dumps(vm, indent=2, default=str))
