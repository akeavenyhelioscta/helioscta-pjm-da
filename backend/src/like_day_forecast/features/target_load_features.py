"""Target-date (D+1) load features for similarity matching.

Coalescing logic (forecast wins over shifted actuals):
  1. Build D+1 daily load aggregates from shifted RT metered actuals (2020+, deep history).
  2. Build D+1 daily aggregates from PJM load forecast "Latest" vintage (recent dates).
  3. Forecast values OVERWRITE shifted actuals where both exist — the forecast is the
     information set available at decision time.
  4. All dates represent the reference date (D), features describe D+1.

Regions: RTO, WEST, MIDATL, SOUTH — each gets avg/peak features.
"""
import pandas as pd
import numpy as np
import logging
from datetime import timedelta

logger = logging.getLogger(__name__)

REGIONS = ["RTO", "WEST", "MIDATL", "SOUTH"]


def _add_ramp_features(
    daily: pd.DataFrame,
    df_hourly: pd.DataFrame,
    prefix: str,
    load_col: str = "rt_load_mw",
) -> pd.DataFrame:
    """Add morning and evening ramp features to a daily DataFrame."""
    # Normalize hourly dates to match daily dates
    hourly = df_hourly.copy()
    hourly["_date_norm"] = pd.to_datetime(hourly["date"]).dt.date
    daily["_date_norm"] = pd.to_datetime(daily["date"]).dt.date

    for d, grp in hourly.groupby("_date_norm"):
        he5 = grp.loc[grp["hour_ending"] == 5, load_col]
        he8 = grp.loc[grp["hour_ending"] == 8, load_col]
        if len(he5) > 0 and len(he8) > 0:
            daily.loc[daily["_date_norm"] == d, f"{prefix}_morning_ramp"] = (
                float(he8.iloc[0]) - float(he5.iloc[0])
            )

        he15 = grp.loc[grp["hour_ending"] == 15, load_col]
        he20 = grp.loc[grp["hour_ending"] == 20, load_col]
        if len(he15) > 0 and len(he20) > 0:
            daily.loc[daily["_date_norm"] == d, f"{prefix}_evening_ramp"] = (
                float(he20.iloc[0]) - float(he15.iloc[0])
            )

    daily = daily.drop(columns=["_date_norm"])
    return daily


def build(
    df_rt_load: pd.DataFrame | None = None,
    df_load_forecast: pd.DataFrame | None = None,
    df_meteo_load_forecast: pd.DataFrame | None = None,
    df_ref_load_features: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build target-date (D+1) load features from forecast + shifted actuals.

    Args:
        df_rt_load: RT metered load hourly [date, hour_ending, region, rt_load_mw].
        df_load_forecast: PJM load forecast with columns
            [region, forecast_date, hour_ending, forecast_load_mw, vintage_label].
        df_meteo_load_forecast: Meteologica load forecast, same schema as PJM.
        df_ref_load_features: Reference-date load features for cross-day delta.
            Must have column [date, load_daily_avg].

    Returns:
        DataFrame with one row per date (reference date D), target-load features for D+1.
    """
    frames = []

    # ── 1. Shifted actuals (deep history) ──────────────────────────────
    if df_rt_load is not None and len(df_rt_load) > 0:
        for region in REGIONS:
            prefix = "tgt_load" if region == "RTO" else f"tgt_load_{region.lower()}"
            df_r = df_rt_load[df_rt_load["region"] == region][
                ["date", "hour_ending", "rt_load_mw"]
            ].copy()
            if len(df_r) == 0:
                continue

            daily = df_r.groupby("date").agg(
                **{f"{prefix}_daily_avg": ("rt_load_mw", "mean"),
                   f"{prefix}_daily_peak": ("rt_load_mw", "max")},
            ).reset_index()

            # Ramp features from hourly data
            daily = _add_ramp_features(daily, df_r, prefix)

            # Shift back: D+1 actuals → assigned to reference date D
            daily["date"] = pd.to_datetime(daily["date"]) - pd.Timedelta(days=1)
            daily["date"] = pd.to_datetime(daily["date"]).dt.date
            frames.append(daily)

    # Merge all regional actuals
    if frames:
        result = frames[0]
        for f in frames[1:]:
            result = result.merge(f, on="date", how="outer")
        # Normalize date type
        result["date"] = pd.to_datetime(result["date"]).dt.date
    else:
        result = pd.DataFrame(columns=["date"])

    # ── 2. Overlay PJM load forecast "Latest" vintage ──────────────────
    if df_load_forecast is not None and len(df_load_forecast) > 0:
        df_fcst = df_load_forecast.copy()

        # Filter to Latest vintage only
        if "vintage_label" in df_fcst.columns:
            df_fcst = df_fcst[df_fcst["vintage_label"] == "Latest"]

        date_col = "forecast_date" if "forecast_date" in df_fcst.columns else "date"
        value_col = "forecast_load_mw" if "forecast_load_mw" in df_fcst.columns else "load_mw"

        for region in REGIONS:
            prefix = "tgt_load" if region == "RTO" else f"tgt_load_{region.lower()}"
            df_r = df_fcst[df_fcst["region"] == region][[date_col, "hour_ending", value_col]].copy()
            if len(df_r) == 0:
                continue

            # Rename and normalize date type for ramp helper
            df_r_renamed = df_r.rename(columns={date_col: "date", value_col: "load_mw"})
            df_r_renamed["date"] = pd.to_datetime(df_r_renamed["date"]).dt.date

            daily_fcst = df_r_renamed.groupby("date").agg(
                **{f"{prefix}_daily_avg": ("load_mw", "mean"),
                   f"{prefix}_daily_peak": ("load_mw", "max")},
            ).reset_index()

            # Ramp features from hourly forecast
            daily_fcst = _add_ramp_features(daily_fcst, df_r_renamed, prefix, load_col="load_mw")

            # Shift back: D+1 forecast → assigned to reference date D
            daily_fcst["date"] = daily_fcst["date"] - pd.Timedelta(days=1)
            daily_fcst["date"] = pd.to_datetime(daily_fcst["date"]).dt.date

            # Overlay: forecast overwrites actuals where both exist
            if len(result) > 0:
                result = result.set_index("date")
                daily_fcst = daily_fcst.set_index("date")
                result.update(daily_fcst)
                # Add new rows from forecast (future dates with no actuals)
                new_dates = daily_fcst.index.difference(result.index)
                if len(new_dates) > 0:
                    result = pd.concat([result, daily_fcst.loc[new_dates]])
                result = result.reset_index()
            else:
                result = daily_fcst.reset_index() if "date" not in daily_fcst.columns else daily_fcst

    # ── 3. Meteologica load forecast — RTO avg + spread vs PJM ──────────
    if df_meteo_load_forecast is not None and len(df_meteo_load_forecast) > 0:
        df_meteo = df_meteo_load_forecast.copy()

        if "vintage_label" in df_meteo.columns:
            df_meteo = df_meteo[df_meteo["vintage_label"] == "Latest"]

        date_col = "forecast_date" if "forecast_date" in df_meteo.columns else "date"
        value_col = "forecast_load_mw" if "forecast_load_mw" in df_meteo.columns else "load_mw"

        # Meteologica RTO avg
        df_rto = df_meteo[df_meteo["region"] == "RTO"][[date_col, "hour_ending", value_col]].copy()
        if len(df_rto) > 0:
            meteo_daily = df_rto.groupby(date_col).agg(
                tgt_meteo_load_daily_avg=(value_col, "mean"),
                tgt_meteo_load_daily_peak=(value_col, "max"),
            ).reset_index()
            meteo_daily = meteo_daily.rename(columns={date_col: "date"})
            meteo_daily["date"] = meteo_daily["date"] - pd.Timedelta(days=1)
            meteo_daily["date"] = pd.to_datetime(meteo_daily["date"]).dt.date

            if len(result) > 0:
                result = result.merge(meteo_daily, on="date", how="left")
            else:
                result = meteo_daily

        # Meteologica WEST avg
        df_west = df_meteo[df_meteo["region"] == "WEST"][[date_col, "hour_ending", value_col]].copy()
        if len(df_west) > 0:
            meteo_west = df_west.groupby(date_col).agg(
                tgt_meteo_load_west_daily_avg=(value_col, "mean"),
            ).reset_index()
            meteo_west = meteo_west.rename(columns={date_col: "date"})
            meteo_west["date"] = meteo_west["date"] - pd.Timedelta(days=1)
            meteo_west["date"] = pd.to_datetime(meteo_west["date"]).dt.date
            result = result.merge(meteo_west, on="date", how="left")

    if len(result) == 0:
        logger.warning("No load data for target load features")
        return pd.DataFrame(columns=["date"])

    result = result.sort_values("date").reset_index(drop=True)

    # Ensure date is python date (not Timestamp)
    result["date"] = pd.to_datetime(result["date"]).dt.date

    # ── 4. Cross-day delta: D+1 avg load - D avg load (RTO) ───────────
    if (
        df_ref_load_features is not None
        and "load_daily_avg" in df_ref_load_features.columns
        and "tgt_load_daily_avg" in result.columns
    ):
        ref = df_ref_load_features[["date", "load_daily_avg"]].copy()
        result = result.merge(ref, on="date", how="left")
        result["tgt_load_change_vs_ref"] = (
            result["tgt_load_daily_avg"] - result["load_daily_avg"]
        )
        result = result.drop(columns=["load_daily_avg"])

    n_features = len([c for c in result.columns if c != "date"])
    logger.info(f"Built {n_features} target-load similarity features "
                f"({len(REGIONS)} regions, forecast overlay)")
    return result
