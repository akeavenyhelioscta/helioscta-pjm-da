"""View model for Meteologica DA price forecast — structured for tomorrow.

Produces output matching the like-day forecast view model format so the
markdown formatter and MCP endpoint can render it identically.
"""
import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = list(range(1, 8)) + [24]


def build_view_model(
    df_meteo: pd.DataFrame,
    forecast_date: date | None = None,
) -> dict:
    """Build a view model for Meteologica's DA price forecast for a single date.

    Produces the same structure as like_day_forecast_results.build_view_model()
    so it can be rendered by format_like_day_forecast_results() or compared
    side-by-side.

    Args:
        df_meteo: Meteologica DA price forecast with columns
            [forecast_date, hour_ending, forecast_da_price, ...].
        forecast_date: Target date. Defaults to tomorrow.

    Returns:
        Dict with: forecast_date, hourly, summary, bands (empty), etc.
    """
    if forecast_date is None:
        forecast_date = date.today() + timedelta(days=1)

    df = df_meteo[df_meteo["forecast_date"] == forecast_date].copy()
    if df.empty:
        return {"error": f"No Meteologica forecast for {forecast_date}"}

    df = df.sort_values("hour_ending")
    prices = dict(zip(df["hour_ending"].astype(int), df["forecast_da_price"]))

    # Execution timestamp
    exec_ts = None
    if "forecast_execution_datetime" in df.columns:
        latest = pd.to_datetime(df["forecast_execution_datetime"]).max()
        if pd.notna(latest):
            exec_ts = str(latest)

    # ── Per-hour structured data ────────────────────────────────────
    hourly = []
    for h in range(1, 25):
        val = prices.get(h)
        fcst = round(float(val), 2) if pd.notna(val) else None
        hourly.append({
            "hour": h,
            "period": "on_peak" if h in ONPEAK_HOURS else "off_peak",
            "forecast": fcst,
            "actual": None,
            "error": None,
            "error_severity": None,
            "p25": None,
            "p75": None,
            "outside_iqr": False,
            "quantiles": {},
        })

    # ── Period summaries ────────────────────────────────────────────
    def _period_avg(hours):
        vals = [h["forecast"] for h in hourly if h["hour"] in hours and h["forecast"] is not None]
        return round(np.mean(vals), 2) if vals else None

    summary = {
        "on_peak": {"forecast": _period_avg(ONPEAK_HOURS), "actual": None, "error": None},
        "off_peak": {"forecast": _period_avg(OFFPEAK_HOURS), "actual": None, "error": None},
        "flat": {"forecast": _period_avg(list(range(1, 25))), "actual": None, "error": None},
    }

    return {
        "forecast_date": str(forecast_date),
        "reference_date": str(date.today()),
        "source": "meteologica",
        "execution_timestamp": exec_ts,
        "has_actuals": False,
        "n_analogs_used": 0,
        "analogs": None,
        "hourly": hourly,
        "summary": summary,
        "bands": [],
        "diffs": {},
        "metrics": None,
        "quantile_coverage": {},
    }
