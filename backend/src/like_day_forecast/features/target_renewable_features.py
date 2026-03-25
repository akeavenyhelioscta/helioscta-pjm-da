"""Target-date (D+1) renewable features for similarity matching.

For historical dates: shifts D+1 actual solar/wind generation back to the
reference date (from fuel mix actuals).
For production: D+1 solar/wind forecasts from gridstatus are used when
available, falling back to shifted actuals.

Coalescing logic (forecast wins over actuals):
  1. Build daily aggregates from fuel mix actuals (2020+, deep history).
  2. Build daily aggregates from D+1 solar/wind forecasts (Apr 2025+).
  3. Forecast values OVERWRITE actuals where both exist (forecast is the
     information set available at decision time). For future dates where
     actuals don't exist yet, forecast values CREATE new rows.
  4. Shift all dates back by 1 day so D+1 features align with reference date D.
"""
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def build(
    df_fuel_mix: pd.DataFrame | None = None,
    df_solar_forecast: pd.DataFrame | None = None,
    df_wind_forecast: pd.DataFrame | None = None,
    df_ref_renewable_features: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build target-date (D+1) renewable features, shifted back to reference date.

    Uses D+1 actuals from fuel mix for historical depth (2020+). Overlays
    D+1 forecasts from gridstatus where available (Apr 2025+) for production.

    Args:
        df_fuel_mix: Hourly fuel mix [date, hour_ending, solar, wind].
        df_solar_forecast: D+1 solar forecast [date, hour_ending, solar_forecast].
        df_wind_forecast: D+1 wind forecast [date, hour_ending, wind_forecast].
        df_ref_renewable_features: Reference-date renewable features for cross-day delta.

    Returns:
        DataFrame with one row per date (reference date D), target-date features for D+1.
    """
    if df_fuel_mix is None or len(df_fuel_mix) == 0:
        logger.warning("No fuel mix data provided for target renewable features")
        return pd.DataFrame(columns=["date"])

    # --- 1. Build from actuals (deep history, 2020+) ---
    df = df_fuel_mix[["date", "hour_ending", "solar", "wind"]].copy()
    df = df.sort_values(["date", "hour_ending"]).reset_index(drop=True)
    df["solar"] = df["solar"].fillna(0)
    df["wind"] = df["wind"].fillna(0)
    df["renewable_total"] = df["solar"] + df["wind"]

    daily = df.groupby("date").agg(
        tgt_solar_daily_avg=("solar", "mean"),
        tgt_wind_daily_avg=("wind", "mean"),
        tgt_renewable_daily_avg=("renewable_total", "mean"),
        tgt_renewable_daily_max=("renewable_total", "max"),
    )

    # --- 2. Build from forecasts (Apr 2025+, includes future dates) ---
    # Forecasts win over actuals: they represent the information set available
    # at decision time. For D+1 (tomorrow), actuals don't exist yet — the
    # forecast creates the row. For recent history where both exist, the
    # forecast overwrites the actual.
    if df_solar_forecast is not None and len(df_solar_forecast) > 0:
        sf = df_solar_forecast[["date", "hour_ending", "solar_forecast"]].copy()
        sf_daily = sf.groupby("date").agg(
            tgt_solar_daily_avg=("solar_forecast", "mean"),
        )
        # combine_first: forecast fills gaps + adds new rows; then overwrite
        # overlapping dates with forecast values (forecast is authoritative)
        daily = sf_daily.combine_first(daily)
        daily.update(sf_daily)

    if df_wind_forecast is not None and len(df_wind_forecast) > 0:
        wf = df_wind_forecast[["date", "hour_ending", "wind_forecast"]].copy()
        wf_daily = wf.groupby("date").agg(
            tgt_wind_daily_avg=("wind_forecast", "mean"),
        )
        daily = wf_daily.combine_first(daily)
        daily.update(wf_daily)

    daily = daily.reset_index()

    # Recompute totals after overlay
    daily["tgt_renewable_daily_avg"] = (
        daily["tgt_solar_daily_avg"].fillna(0) + daily["tgt_wind_daily_avg"].fillna(0)
    )

    # Shift date back by 1 day: D+1 features → assigned to reference date D
    daily["date"] = daily["date"] - pd.Timedelta(days=1)

    # Cross-day delta: D+1 renewable avg - D renewable avg
    if (df_ref_renewable_features is not None
            and "renewable_daily_avg" in df_ref_renewable_features.columns):
        ref = df_ref_renewable_features[["date", "renewable_daily_avg"]].copy()
        daily = daily.merge(ref, on="date", how="left")
        daily["tgt_renewable_change_vs_ref"] = (
            daily["tgt_renewable_daily_avg"] - daily["renewable_daily_avg"]
        )
        daily = daily.drop(columns=["renewable_daily_avg"])

    n_features = len([c for c in daily.columns if c != "date"])
    logger.info(f"Built {n_features} target-renewable similarity features")
    return daily
