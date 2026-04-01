"""Regional spark spread view model.

Computes implied heat rates (LMP / gas price) for each PJM power hub
paired with its primary gas fuel hub based on the stack model mapping.
"""
import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = list(range(1, 8)) + [24]

# Power hub → gas hub mapping per PJM Market Monitor methodology
# (State of the Market Report, Section 7 — Net Revenue / Spark Spreads)
# Reference heat rate: 7,000 Btu/kWh (7.0 MMBtu/MWh) for combined cycle
HUB_GAS_MAP = {
    "WESTERN HUB": "gas_m3",          # Texas Eastern M3 (per PJM IMM)
    "AEP GEN HUB": "gas_m2",          # Tetco M2 (AEP zone delivery — 26.8% of capacity)
    "DOMINION HUB": "gas_tz5",         # Transco Z5 (Dominion zone delivery)
    "EASTERN HUB": "gas_tz6",          # Transco Z6 non-NY (BGE/PECO/METED)
}

HUB_DISPLAY = {
    "WESTERN HUB": "Western Hub",
    "AEP GEN HUB": "AEP Gen",
    "DOMINION HUB": "Dominion",
    "EASTERN HUB": "Eastern Hub",
}

GAS_DISPLAY = {
    "gas_m3": "Tetco M3",
    "gas_m2": "Tetco M2",
    "gas_tz5": "Transco Z5",
    "gas_tz6": "Transco Z6",
}


def build_view_model(
    df_da: pd.DataFrame,
    df_rt: pd.DataFrame | None,
    df_gas_hourly: pd.DataFrame,
    lookback_days: int = 30,
) -> dict:
    """Build regional spark spread view model with hourly detail.

    Args:
        df_da: DA LMP hourly for all hubs [date, hour_ending, hub, lmp_total].
        df_rt: RT LMP hourly for all hubs (same schema). Can be None.
        df_gas_hourly: Hourly gas [date, hour_ending, gas_m3, gas_tz6, ...].
        lookback_days: Number of days to include.

    Returns:
        Dict with daily_summary and hourly_detail (per hub, per day, per hour).
    """
    start = date.today() - timedelta(days=lookback_days)

    df_da = df_da.copy()
    df_da["date"] = pd.to_datetime(df_da["date"]).dt.date
    df_da = df_da[df_da["date"] >= start]

    df_gas = df_gas_hourly.copy()
    df_gas["date"] = pd.to_datetime(df_gas["date"]).dt.date

    # Gas day runs 10am-10am ET. HE1-9 on electric day D use gas day D-1
    # (gas day D covers HE10-24 of electric day D + HE1-9 of electric day D+1).
    # Shift gas day D, HE10-24 → electric day D; gas day D, HE1-9 → electric day D+1.
    gas_he10_24 = df_gas[df_gas["hour_ending"] >= 10].copy()  # stays on same date
    gas_he1_9 = df_gas[df_gas["hour_ending"] < 10].copy()
    gas_he1_9["date"] = gas_he1_9["date"] + timedelta(days=1)  # shift to next electric day
    df_gas = pd.concat([gas_he10_24, gas_he1_9], ignore_index=True)
    df_gas = df_gas.sort_values(["date", "hour_ending"]).reset_index(drop=True)

    df_gas = df_gas[df_gas["date"] >= start]

    has_rt = df_rt is not None and len(df_rt) > 0
    if has_rt:
        df_rt = df_rt.copy()
        df_rt["date"] = pd.to_datetime(df_rt["date"]).dt.date
        df_rt = df_rt[df_rt["date"] >= start]

    daily_results = []
    hourly_results = []

    for power_hub, gas_col in HUB_GAS_MAP.items():
        if gas_col not in df_gas.columns:
            continue

        hub_gas = df_gas[["date", "hour_ending", gas_col]].copy()
        hub_gas = hub_gas.rename(columns={gas_col: "gas_price"})

        # DA hourly
        hub_da = df_da[df_da["hub"] == power_hub][["date", "hour_ending", "lmp_total"]].copy()
        da_merged = hub_da.merge(hub_gas, on=["date", "hour_ending"], how="inner")
        if len(da_merged) > 0:
            da_merged["heat_rate"] = da_merged["lmp_total"] / da_merged["gas_price"].replace(0, np.nan)
            da_merged["spark_7"] = da_merged["lmp_total"] - (da_merged["gas_price"] * 7.0)

            for _, row in da_merged.iterrows():
                hourly_results.append({
                    "date": row["date"],
                    "hour_ending": int(row["hour_ending"]),
                    "market": "DA",
                    "power_hub": power_hub,
                    "power_hub_display": HUB_DISPLAY.get(power_hub, power_hub),
                    "gas_hub_display": GAS_DISPLAY.get(gas_col, gas_col),
                    "lmp": row["lmp_total"],
                    "gas_price": row["gas_price"],
                    "heat_rate": row["heat_rate"],
                    "spark_7": row["spark_7"],
                })

        # RT hourly
        if has_rt:
            hub_rt = df_rt[df_rt["hub"] == power_hub][["date", "hour_ending", "lmp_total"]].copy()
            rt_merged = hub_rt.merge(hub_gas, on=["date", "hour_ending"], how="inner")
            if len(rt_merged) > 0:
                rt_merged["heat_rate"] = rt_merged["lmp_total"] / rt_merged["gas_price"].replace(0, np.nan)
                rt_merged["spark_7"] = rt_merged["lmp_total"] - (rt_merged["gas_price"] * 7.0)

                for _, row in rt_merged.iterrows():
                    hourly_results.append({
                        "date": row["date"],
                        "hour_ending": int(row["hour_ending"]),
                        "market": "RT",
                        "power_hub": power_hub,
                        "power_hub_display": HUB_DISPLAY.get(power_hub, power_hub),
                        "gas_hub_display": GAS_DISPLAY.get(gas_col, gas_col),
                        "lmp": row["lmp_total"],
                        "gas_price": row["gas_price"],
                        "heat_rate": row["heat_rate"],
                        "spark_7": row["spark_7"],
                    })

        # Daily summary (DA only)
        if len(da_merged) > 0:
            for d, grp in da_merged.groupby("date"):
                onpk = grp[grp["hour_ending"].isin(ONPEAK_HOURS)]
                offpk = grp[grp["hour_ending"].isin(OFFPEAK_HOURS)]
                daily_results.append({
                    "date": d,
                    "power_hub": power_hub,
                    "power_hub_display": HUB_DISPLAY.get(power_hub, power_hub),
                    "gas_hub": gas_col,
                    "gas_hub_display": GAS_DISPLAY.get(gas_col, gas_col),
                    "lmp_onpeak": onpk["lmp_total"].mean() if len(onpk) else np.nan,
                    "lmp_offpeak": offpk["lmp_total"].mean() if len(offpk) else np.nan,
                    "gas_onpeak": onpk["gas_price"].mean() if len(onpk) else np.nan,
                    "gas_offpeak": offpk["gas_price"].mean() if len(offpk) else np.nan,
                    "heat_rate_onpeak": onpk["heat_rate"].mean() if len(onpk) else np.nan,
                    "heat_rate_offpeak": offpk["heat_rate"].mean() if len(offpk) else np.nan,
                    "spark_7_onpeak": onpk["spark_7"].mean() if len(onpk) else np.nan,
                    "spark_7_offpeak": offpk["spark_7"].mean() if len(offpk) else np.nan,
                })

    return {
        "lookback_days": lookback_days,
        "start_date": str(start),
        "end_date": str(date.today()),
        "hub_gas_map": {k: GAS_DISPLAY.get(v, v) for k, v in HUB_GAS_MAP.items()},
        "daily": pd.DataFrame(daily_results),
        "hourly": pd.DataFrame(hourly_results),
    }
