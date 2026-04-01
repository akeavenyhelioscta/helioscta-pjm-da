"""Daily load features for similarity matching.

RT metered load is the demand signal (available 2014+). Builds features for
RTO and each sub-region (WEST, MIDATL, SOUTH) so the model can match on
both system-wide and regional demand patterns.
"""
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

REGIONS = ["RTO", "WEST", "MIDATL", "SOUTH"]


def _build_region(df_region: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """Build load features for a single region with column prefix."""
    load_col = "rt_load_mw"
    df = df_region.sort_values(["date", "hour_ending"]).reset_index(drop=True)


    daily = df.groupby("date").agg(
        **{f"{prefix}_daily_avg": (load_col, "mean"),
           f"{prefix}_daily_peak": (load_col, "max"),
           f"{prefix}_daily_valley": (load_col, "min")},
    )

    daily[f"{prefix}_peak_ratio"] = (
        daily[f"{prefix}_daily_peak"] / daily[f"{prefix}_daily_avg"].replace(0, np.nan)
    )

    # Max hour-to-hour ramp
    df["ramp"] = df.groupby("date")[load_col].diff()
    daily[f"{prefix}_ramp_max"] = df.groupby("date")["ramp"].max()

    # Morning ramp: HE8 - HE5 (demand pickup into on-peak)
    for d, grp in df.groupby("date"):
        he5 = grp.loc[grp["hour_ending"] == 5, load_col]
        he8 = grp.loc[grp["hour_ending"] == 8, load_col]
        if len(he5) > 0 and len(he8) > 0:
            daily.at[d, f"{prefix}_morning_ramp"] = float(he8.iloc[0]) - float(he5.iloc[0])

    # Evening ramp: HE20 - HE15 (afternoon pickup into evening peak)
    for d, grp in df.groupby("date"):
        he15 = grp.loc[grp["hour_ending"] == 15, load_col]
        he20 = grp.loc[grp["hour_ending"] == 20, load_col]
        if len(he15) > 0 and len(he20) > 0:
            daily.at[d, f"{prefix}_evening_ramp"] = float(he20.iloc[0]) - float(he15.iloc[0])

    daily[f"{prefix}_7d_rolling_mean"] = (
        daily[f"{prefix}_daily_avg"].rolling(7, min_periods=1).mean()
    )

    daily[f"{prefix}_daily_change"] = daily[f"{prefix}_daily_avg"].diff()

    return daily.reset_index()


def build(
    df_rt_load: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build daily load feature vectors for all regions.

    Args:
        df_rt_load: RT metered load hourly [date, hour_ending, region, rt_load_mw].

    Returns:
        DataFrame with one row per date, load features per region.
    """
    if df_rt_load is None or len(df_rt_load) == 0:
        logger.warning("No load data provided")
        return pd.DataFrame(columns=["date"])

    result = None

    for region in REGIONS:
        prefix = "load" if region == "RTO" else f"load_{region.lower()}"
        df_region = df_rt_load[df_rt_load["region"] == region][
            ["date", "hour_ending", "rt_load_mw"]
        ].copy()

        if len(df_region) == 0:
            logger.warning(f"No load data for region {region}")
            continue

        df_feat = _build_region(df_region, prefix)

        if result is None:
            result = df_feat
        else:
            result = result.merge(df_feat, on="date", how="outer")

    if result is None:
        return pd.DataFrame(columns=["date"])

    n_features = len([c for c in result.columns if c != "date"])
    logger.info(f"Built {n_features} load similarity features ({len(REGIONS)} regions)")
    return result
