"""Hourly gas price features for similarity matching.

Next-day gas cash prices from ICE across 10 PJM-relevant hubs. Captures:
- Gas price level and intraday shape (morning ramps, on/off-peak)
- Regional basis spreads (M3 vs production hubs, east vs west)
- Pipeline constraint signals from basis widening

Hub groupings by PJM region:
  Western PJM:   gas_m3 (Tetco M3), gas_tco (Columbia TCO)
  Eastern PJM:   gas_tz6 (Transco Z6), gas_tz5 (Transco Z5 North)
  Marcellus:     gas_dom_south (Dominion South), gas_leidy, gas_tn4
  Midwest:       gas_ventura (NNG), gas_chicago (Chicago CG)
  Mid-Atlantic:  gas_m2 (Tetco M2)
"""
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = list(range(1, 8)) + [24]

# Primary hubs for detailed features (top 4 by capacity)
PRIMARY_HUBS = ["gas_m3", "gas_tco", "gas_tz6", "gas_dom_south"]

# All hubs for daily avg (used in basis spreads)
ALL_HUBS = [
    "gas_m3", "gas_dom_south", "gas_tz6", "gas_tco", "gas_ventura",
    "gas_m2", "gas_tz5", "gas_tn4", "gas_leidy", "gas_chicago",
]

# Basis pairs: (hub_a, hub_b) → spread = hub_a - hub_b
# Positive spread = hub_a is more expensive (constraint into hub_a region)
BASIS_PAIRS = [
    ("gas_m3", "gas_dom_south"),    # M3 vs Marcellus production — pipeline out of basin
    ("gas_tz6", "gas_m3"),          # Eastern vs Western PJM
    ("gas_tco", "gas_m3"),          # Appalachian vs Western
    ("gas_m3", "gas_chicago"),      # Western PJM vs Midwest
]


def build(
    df_gas_hourly: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build daily gas features from hourly next-day cash prices.

    Args:
        df_gas_hourly: Hourly gas with columns [date, hour_ending, gas_m3, gas_tco, ...].

    Returns:
        DataFrame with one row per date, gas features.
    """
    if df_gas_hourly is None or len(df_gas_hourly) == 0:
        logger.warning("No hourly gas data provided")
        return pd.DataFrame(columns=["date"])

    df = df_gas_hourly.copy()
    df = df.sort_values(["date", "hour_ending"]).reset_index(drop=True)

    # Fill forward within each day
    available_hubs = [h for h in ALL_HUBS if h in df.columns]
    for col in available_hubs:
        df[col] = df.groupby("date")[col].ffill()

    daily = pd.DataFrame(index=df["date"].unique())

    # --- Per-hub features (primary hubs only — detailed) ---
    for hub in PRIMARY_HUBS:
        if hub not in df.columns:
            continue
        prefix = hub  # e.g., "gas_m3"
        grp = df.groupby("date")[hub]

        daily[f"{prefix}_daily_avg"] = grp.mean()
        daily[f"{prefix}_daily_max"] = grp.max()
        daily[f"{prefix}_intraday_range"] = grp.max() - grp.min()

        # On-peak vs off-peak
        onpk = df[df["hour_ending"].isin(ONPEAK_HOURS)].groupby("date")[hub].mean()
        offpk = df[df["hour_ending"].isin(OFFPEAK_HOURS)].groupby("date")[hub].mean()
        daily[f"{prefix}_onpeak_avg"] = onpk
        daily[f"{prefix}_offpeak_avg"] = offpk

    # Morning ramp for M3 (primary marginal hub)
    if "gas_m3" in df.columns:
        for d, grp in df.groupby("date"):
            he5 = grp.loc[grp["hour_ending"] == 5, "gas_m3"]
            he8 = grp.loc[grp["hour_ending"] == 8, "gas_m3"]
            if len(he5) > 0 and len(he8) > 0:
                daily.at[d, "gas_m3_morning_ramp"] = float(he8.iloc[0]) - float(he5.iloc[0])

    # --- Secondary hubs: daily avg only ---
    for hub in ALL_HUBS:
        if hub in PRIMARY_HUBS or hub not in df.columns:
            continue
        daily[f"{hub}_daily_avg"] = df.groupby("date")[hub].mean()

    # --- Basis spread features ---
    for hub_a, hub_b in BASIS_PAIRS:
        col_a = f"{hub_a}_daily_avg"
        col_b = f"{hub_b}_daily_avg"
        if col_a in daily.columns and col_b in daily.columns:
            spread_name = f"gas_basis_{hub_a.replace('gas_', '')}_{hub_b.replace('gas_', '')}"
            daily[spread_name] = daily[col_a] - daily[col_b]

    # --- Rolling trend (M3) ---
    if "gas_m3_daily_avg" in daily.columns:
        daily["gas_m3_7d_rolling_mean"] = (
            daily["gas_m3_daily_avg"].rolling(7, min_periods=1).mean()
        )

    daily.index.name = "date"
    result = daily.reset_index()

    n_features = len([c for c in result.columns if c != "date"])
    logger.info(f"Built {n_features} hourly gas similarity features ({len(available_hubs)} hubs)")
    return result
