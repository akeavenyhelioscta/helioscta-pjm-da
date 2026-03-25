"""Target-date (D+1) outage features for similarity matching.

Shifts D+1 actual outages back to the reference date for historical analog
matching. For production, yesterday's actuals serve as the best available
proxy for tomorrow's outage level (planned outages change slowly).
"""
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def build(
    df_outages: pd.DataFrame | None = None,
    df_ref_outage_features: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build target-date (D+1) outage features, shifted back to reference date.

    Args:
        df_outages: Daily outages [date, total_outages_mw, planned_outages_mw,
                    maintenance_outages_mw, forced_outages_mw].
        df_ref_outage_features: Reference-date outage features for cross-day delta.

    Returns:
        DataFrame with one row per date (reference date D), target-date features for D+1.
    """
    if df_outages is None or len(df_outages) == 0:
        logger.warning("No outage data provided for target outage features")
        return pd.DataFrame(columns=["date"])

    df = df_outages[["date", "total_outages_mw", "forced_outages_mw"]].copy()
    df = df.sort_values("date").reset_index(drop=True)

    daily = df.rename(columns={
        "total_outages_mw": "tgt_outage_total_mw",
        "forced_outages_mw": "tgt_outage_forced_mw",
    })

    # Shift date back by 1 day: D+1 features → assigned to reference date D
    daily["date"] = daily["date"] - pd.Timedelta(days=1)

    # Cross-day delta: D+1 total outages - D total outages
    if (df_ref_outage_features is not None
            and "outage_total_mw" in df_ref_outage_features.columns):
        ref = df_ref_outage_features[["date", "outage_total_mw"]].copy()
        daily = daily.merge(ref, on="date", how="left")
        daily["tgt_outage_change_vs_ref"] = (
            daily["tgt_outage_total_mw"] - daily["outage_total_mw"]
        )
        daily = daily.drop(columns=["outage_total_mw"])

    n_features = len([c for c in daily.columns if c != "date"])
    logger.info(f"Built {n_features} target-outage similarity features")
    return daily
