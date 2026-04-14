"""View model for supply stack forecast results."""
from __future__ import annotations

import numpy as np
import pandas as pd

ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = list(range(1, 8)) + [24]


def _safe_float(value) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return None if np.isnan(out) else out


def _safe_round(value, decimals: int = 2) -> float | None:
    out = _safe_float(value)
    if out is None:
        return None
    return round(out, decimals)


def _period_summary(hourly: list[dict], hours: list[int]) -> dict:
    f_vals = [h["forecast"] for h in hourly if h["hour"] in hours and h["forecast"] is not None]
    a_vals = [h["actual"] for h in hourly if h["hour"] in hours and h["actual"] is not None]
    e_vals = [h["error"] for h in hourly if h["hour"] in hours and h["error"] is not None]
    return {
        "forecast": round(float(np.mean(f_vals)), 2) if f_vals else None,
        "actual": round(float(np.mean(a_vals)), 2) if a_vals else None,
        "error": round(float(np.mean(e_vals)), 2) if e_vals else None,
    }


def build_view_model(pipeline_result: dict) -> dict:
    """Transform pipeline output into a structured response for API/reporting."""
    if "error" in pipeline_result:
        return {"error": pipeline_result["error"]}

    output_table: pd.DataFrame = pipeline_result["output_table"]
    quantiles_table: pd.DataFrame = pipeline_result["quantiles_table"]
    df_forecast: pd.DataFrame = pipeline_result["df_forecast"]
    has_actuals: bool = pipeline_result["has_actuals"]
    forecast_date: str = pipeline_result["forecast_date"]
    metrics: dict | None = pipeline_result.get("metrics")
    model_config: dict = pipeline_result.get("config", {})

    forecast_row = output_table[output_table["Type"] == "Forecast"].iloc[0]
    actual_row = (
        output_table[output_table["Type"] == "Actual"].iloc[0]
        if has_actuals
        else None
    )

    hourly: list[dict] = []
    for h in range(1, 25):
        he = f"HE{h}"
        fr = df_forecast[df_forecast["hour_ending"] == h]
        if len(fr) == 0:
            continue
        fr0 = fr.iloc[0]

        fcst = _safe_float(forecast_row[he])
        act = _safe_float(actual_row[he]) if actual_row is not None else None
        err = round(fcst - act, 2) if (fcst is not None and act is not None) else None

        qvals: dict[str, float | None] = {}
        for _, qrow in quantiles_table.iterrows():
            qvals[str(qrow["Type"])] = _safe_round(qrow.get(he))

        hourly.append(
            {
                "hour": h,
                "period": "on_peak" if h in ONPEAK_HOURS else "off_peak",
                "forecast": _safe_round(fcst),
                "actual": _safe_round(act),
                "error": err,
                "marginal_fuel": str(fr0.get("marginal_fuel")),
                "marginal_heat_rate": _safe_round(fr0.get("marginal_heat_rate"), 3),
                "marginal_variable_cost": _safe_round(fr0.get("marginal_variable_cost"), 3),
                "reserve_margin_mw": _safe_round(fr0.get("reserve_margin_mw"), 2),
                "stack_position_pct": _safe_round(
                    (
                        _safe_float(fr0.get("stack_position_pct")) * 100.0
                        if _safe_float(fr0.get("stack_position_pct")) is not None
                        else None
                    ),
                    2,
                ),
                "dispatch_status": fr0.get("dispatch_status"),
                "shortage_mw": _safe_round(fr0.get("shortage_mw"), 2),
                "net_load_mw": _safe_round(fr0.get("net_load_mw"), 2),
                "gas_price_usd_mmbtu": _safe_round(fr0.get("gas_price_usd_mmbtu"), 4),
                "outages_mw": _safe_round(fr0.get("outages_mw"), 2),
                "quantiles": qvals,
            }
        )

    summary = {
        "on_peak": _period_summary(hourly, ONPEAK_HOURS),
        "off_peak": _period_summary(hourly, OFFPEAK_HOURS),
        "flat": _period_summary(hourly, list(range(1, 25))),
    }

    bands: list[dict] = []
    for _, qrow in quantiles_table.iterrows():
        band = {"band": qrow["Type"]}
        for h in range(1, 25):
            band[f"HE{h}"] = _safe_round(qrow.get(f"HE{h}"))
        for col in ["OnPeak", "OffPeak", "Flat"]:
            band[col] = _safe_round(qrow.get(col))
        bands.append(band)

    diffs: dict[str, dict] = {}
    if has_actuals:
        row = {}
        for h in range(1, 25):
            hr = next((x for x in hourly if x["hour"] == h), None)
            row[f"HE{h}"] = hr["error"] if hr else None
        for col in ["OnPeak", "OffPeak", "Flat"]:
            f_val = _safe_float(forecast_row[col])
            a_val = _safe_float(actual_row[col]) if actual_row is not None else None
            row[col] = round(f_val - a_val, 2) if (f_val is not None and a_val is not None) else None
        diffs["forecast_minus_actual"] = row

    return {
        "forecast_date": forecast_date,
        "reference_date": pipeline_result.get("reference_date"),
        "has_actuals": has_actuals,
        "metrics": metrics,
        "model_config": model_config,
        "hourly": hourly,
        "summary": summary,
        "bands": bands,
        "diffs": diffs,
    }
