"""Target-date (D+1) outage features for similarity matching.

Coalescing logic (forecast wins over shifted actuals):
  1. Build D+1 daily outage aggregates from shifted outage actuals (deep history).
  2. Overlay PJM outage forecast (latest vintage) where available.
  3. Forecast values OVERWRITE shifted actuals — the forecast is the
     information set available at decision time.
  4. All dates represent the reference date (D), features describe D+1.

Regions: RTO and WEST (Western Hub pricing zone).
"""
import pandas as pd
import numpy as np
import logging
from datetime import timedelta

logger = logging.getLogger(__name__)


def build(
    df_outages: pd.DataFrame | None = None,
    df_outage_forecast: pd.DataFrame | None = None,
    df_ref_outage_features: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build target-date (D+1) outage features from forecast + shifted actuals.

    Args:
        df_outages: Daily actual outages [date, total_outages_mw, forced_outages_mw, ...].
        df_outage_forecast: PJM outage forecast with columns
            [forecast_execution_date, forecast_date, region, total_outages_mw,
             forced_outages_mw, planned_outages_mw].
        df_ref_outage_features: Reference-date outage features for cross-day delta.

    Returns:
        DataFrame with one row per date (reference date D), target-outage features for D+1.
    """
    # ── 1. Shifted actuals (deep history) ──────────────────────────────
    result = pd.DataFrame(columns=["date"])

    if df_outages is not None and len(df_outages) > 0:
        df = df_outages[["date", "total_outages_mw", "forced_outages_mw"]].copy()
        df = df.sort_values("date").reset_index(drop=True)

        daily = df.rename(columns={
            "total_outages_mw": "tgt_outage_total_mw",
            "forced_outages_mw": "tgt_outage_forced_mw",
        })

        # Shift date back by 1 day: D+1 actuals → assigned to reference date D
        daily["date"] = daily["date"] - pd.Timedelta(days=1)
        daily["date"] = pd.to_datetime(daily["date"]).dt.date
        result = daily

    # ── 2. Overlay PJM outage forecast (latest vintage) ────────────────
    if df_outage_forecast is not None and len(df_outage_forecast) > 0:
        df_fcst = df_outage_forecast.copy()

        # Use latest execution date only
        if "forecast_execution_date" in df_fcst.columns:
            latest_exec = df_fcst["forecast_execution_date"].max()
            df_fcst = df_fcst[df_fcst["forecast_execution_date"] == latest_exec]

        # RTO totals
        df_rto = df_fcst[df_fcst["region"] == "RTO"].copy()
        if len(df_rto) > 0:
            date_col = "forecast_date"
            rto_daily = df_rto[[date_col, "total_outages_mw", "forced_outages_mw"]].copy()
            rto_daily = rto_daily.rename(columns={
                date_col: "date",
                "total_outages_mw": "tgt_outage_total_mw",
                "forced_outages_mw": "tgt_outage_forced_mw",
            })
            # Shift back: D+1 forecast → assigned to reference date D
            rto_daily["date"] = rto_daily["date"] - pd.Timedelta(days=1)
            rto_daily["date"] = pd.to_datetime(rto_daily["date"]).dt.date

            if len(result) > 0:
                result = result.set_index("date")
                rto_daily = rto_daily.set_index("date")
                result.update(rto_daily)
                new_dates = rto_daily.index.difference(result.index)
                if len(new_dates) > 0:
                    result = pd.concat([result, rto_daily.loc[new_dates]])
                result = result.reset_index()
            else:
                result = rto_daily.reset_index() if "date" not in rto_daily.columns else rto_daily

        # WEST region outages
        df_west = df_fcst[df_fcst["region"] == "WEST"].copy()
        if len(df_west) > 0:
            date_col = "forecast_date"
            west_daily = df_west[[date_col, "total_outages_mw", "forced_outages_mw"]].copy()
            west_daily = west_daily.rename(columns={
                date_col: "date",
                "total_outages_mw": "tgt_outage_west_total_mw",
                "forced_outages_mw": "tgt_outage_west_forced_mw",
            })
            west_daily["date"] = west_daily["date"] - pd.Timedelta(days=1)
            west_daily["date"] = pd.to_datetime(west_daily["date"]).dt.date
            result = result.merge(west_daily, on="date", how="left")

    if len(result) == 0:
        logger.warning("No outage data for target outage features")
        return pd.DataFrame(columns=["date"])

    result = result.sort_values("date").reset_index(drop=True)
    result["date"] = pd.to_datetime(result["date"]).dt.date

    # ── 3. Cross-day delta: D+1 total outages - D total outages ────────
    if (
        df_ref_outage_features is not None
        and "outage_total_mw" in df_ref_outage_features.columns
        and "tgt_outage_total_mw" in result.columns
    ):
        ref = df_ref_outage_features[["date", "outage_total_mw"]].copy()
        result = result.merge(ref, on="date", how="left")
        result["tgt_outage_change_vs_ref"] = (
            result["tgt_outage_total_mw"] - result["outage_total_mw"]
        )
        result = result.drop(columns=["outage_total_mw"])

    n_features = len([c for c in result.columns if c != "date"])
    logger.info(f"Built {n_features} target-outage similarity features (forecast overlay)")
    return result
