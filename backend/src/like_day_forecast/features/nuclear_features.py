"""Daily nuclear generation features for similarity matching.

Nuclear baseload from gridstatus.pjm_fuel_mix_hourly (2020+). When nuclear
is lower (outages/refueling), marginal costs shift upward as gas/coal fill
the gap. Captures the baseload supply floor.
"""
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def build(
    df_fuel_mix: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build daily nuclear generation features from fuel mix actuals.

    Args:
        df_fuel_mix: Hourly fuel mix [date, hour_ending, nuclear, gas, coal, ...].

    Returns:
        DataFrame with one row per date, nuclear similarity features.
    """
    if df_fuel_mix is None or len(df_fuel_mix) == 0:
        logger.warning("No fuel mix data provided for nuclear features")
        return pd.DataFrame(columns=["date"])

    gen_cols = ["solar", "wind", "gas", "coal", "nuclear", "hydro",
                "oil", "storage", "other", "other_renewables", "multiple_fuels"]
    available_gen = [c for c in gen_cols if c in df_fuel_mix.columns]

    df = df_fuel_mix[["date", "hour_ending"] + available_gen].copy()
    df = df.sort_values(["date", "hour_ending"]).reset_index(drop=True)

    for col in available_gen:
        df[col] = df[col].fillna(0)

    df["total_gen"] = df[available_gen].sum(axis=1)

    # --- Daily aggregates ---
    daily = df.groupby("date").agg(
        nuclear_daily_avg=("nuclear", "mean"),
    )

    daily_nuclear_sum = df.groupby("date")["nuclear"].sum()
    daily_total_sum = df.groupby("date")["total_gen"].sum().replace(0, np.nan)
    daily["nuclear_share_of_total"] = daily_nuclear_sum / daily_total_sum

    daily["nuclear_7d_rolling_mean"] = (
        daily["nuclear_daily_avg"].rolling(7, min_periods=1).mean()
    )

    result = daily.reset_index()

    n_features = len([c for c in result.columns if c != "date"])
    logger.info(f"Built {n_features} nuclear similarity features")
    return result
