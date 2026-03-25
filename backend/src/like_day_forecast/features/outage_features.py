"""Daily outage features for similarity matching.

Uses actual outage data from pjm_cleaned.pjm_outages_actual_daily (2020+).
Captures supply-side conditions: planned maintenance schedules, forced outage
events, and total capacity offline.
"""
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def build(
    df_outages: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build daily outage features from actual outage data.

    Args:
        df_outages: Daily outages [date, total_outages_mw, planned_outages_mw,
                    maintenance_outages_mw, forced_outages_mw].

    Returns:
        DataFrame with one row per date, outage similarity features.
    """
    if df_outages is None or len(df_outages) == 0:
        logger.warning("No outage data provided")
        return pd.DataFrame(columns=["date"])

    df = df_outages[["date", "total_outages_mw", "planned_outages_mw",
                      "maintenance_outages_mw", "forced_outages_mw"]].copy()
    df = df.sort_values("date").reset_index(drop=True)

    # Core outage levels
    df = df.rename(columns={
        "total_outages_mw": "outage_total_mw",
        "planned_outages_mw": "outage_planned_mw",
        "maintenance_outages_mw": "outage_maintenance_mw",
        "forced_outages_mw": "outage_forced_mw",
    })

    # Forced outage share (signals unexpected supply stress)
    df["outage_forced_share"] = (
        df["outage_forced_mw"] / df["outage_total_mw"].replace(0, np.nan)
    )

    # Rolling trend
    df["outage_total_7d_mean"] = (
        df["outage_total_mw"].rolling(7, min_periods=1).mean()
    )

    # Day-over-day change in total outages
    df["outage_total_daily_change"] = df["outage_total_mw"].diff()

    result = df[["date", "outage_total_mw", "outage_planned_mw",
                 "outage_maintenance_mw", "outage_forced_mw",
                 "outage_forced_share", "outage_total_7d_mean",
                 "outage_total_daily_change"]].copy()

    n_features = len([c for c in result.columns if c != "date"])
    logger.info(f"Built {n_features} outage similarity features")
    return result
