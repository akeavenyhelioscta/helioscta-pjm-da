"""Daily congestion component features for similarity matching.

Uses DA LMP congestion price from pjm_cleaned.pjm_lmps_hourly. Congestion
indicates transmission constraints causing locational price divergence from
system energy price. Persistent congestion regimes create distinct patterns.
"""
import pandas as pd
import logging

logger = logging.getLogger(__name__)

ONPEAK_HOURS = list(range(8, 24))


def build(
    df_lmp_da: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build daily congestion component features from DA LMP data.

    Args:
        df_lmp_da: DA LMP hourly with [date, hour_ending, lmp_congestion_price].

    Returns:
        DataFrame with one row per date, congestion similarity features.
    """
    if df_lmp_da is None or len(df_lmp_da) == 0:
        logger.warning("No DA LMP data provided for congestion features")
        return pd.DataFrame(columns=["date"])

    df = df_lmp_da[["date", "hour_ending", "lmp_congestion_price"]].copy()
    df = df.sort_values(["date", "hour_ending"]).reset_index(drop=True)

    # --- Daily aggregates ---
    daily = df.groupby("date").agg(
        congestion_daily_avg=("lmp_congestion_price", "mean"),
        congestion_daily_max=("lmp_congestion_price", "max"),
    )

    onpeak = (
        df[df["hour_ending"].isin(ONPEAK_HOURS)]
        .groupby("date")["lmp_congestion_price"]
        .mean()
        .rename("congestion_onpeak_avg")
    )
    daily = daily.join(onpeak)

    daily["congestion_7d_rolling_std"] = (
        daily["congestion_daily_avg"].rolling(7, min_periods=1).std()
    )

    result = daily.reset_index()

    n_features = len([c for c in result.columns if c != "date"])
    logger.info(f"Built {n_features} congestion similarity features")
    return result
