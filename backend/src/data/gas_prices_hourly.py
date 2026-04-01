"""Pull hourly next-day gas prices from ICE.

All major PJM-relevant gas hubs from the stack model, ordered by
gas-linked generation capacity:
  Tetco M3          17,767 MW  (Western PJM marginal hub)
  Columbia TCO Pool 17,677 MW  (Appalachian/Mid-Atlantic)
  Transco Z6 NY     13,447 MW  (Eastern PJM / NJ-PA)
  Dominion South Pt 12,035 MW  (SW PA / Marcellus production)
  NNG Ventura        7,771 MW  (ComEd / Midwest)
  Tetco M2           7,312 MW  (Mid-Atlantic secondary)
  Transco Z5 North   6,546 MW  (Virginia/Dominion)
  Tenn Z4 Marcellus  3,551 MW  (Marcellus production area)
  Transco Leidy      3,182 MW  (PA pipeline hub)
  Chicago CityGate   2,130 MW  (Midwest delivery)
"""
import pandas as pd
import logging

from src.utils.azure_postgresql import pull_from_db

logger = logging.getLogger(__name__)

# Short names for feature columns
HUB_COLUMNS = {
    "tetco_m3_cash": "gas_m3",
    "dominion_south_cash": "gas_dom_south",
    "transco_z6_ny_cash": "gas_tz6",
    "columbia_tco_cash": "gas_tco",
    "nng_ventura_cash": "gas_ventura",
    "tetco_m2_cash": "gas_m2",
    "transco_z5_north_cash": "gas_tz5",
    "tenn_z4_marcellus_cash": "gas_tn4",
    "transco_leidy_cash": "gas_leidy",
    "chicago_cg_cash": "gas_chicago",
}


def pull() -> pd.DataFrame:
    """Pull hourly next-day gas cash prices for all PJM-relevant hubs."""
    query = """
    select
        gas_day as date,
        hour_ending,
        tetco_m3_cash,
        dominion_south_cash,
        transco_z6_ny_cash,
        columbia_tco_cash,
        nng_ventura_cash,
        tetco_m2_cash,
        transco_z5_north_cash,
        tenn_z4_marcellus_cash,
        transco_leidy_cash,
        chicago_cg_cash
    from ice_python_cleaned.ice_python_next_day_gas_hourly
    where gas_day >= '2020-01-01'
    order by gas_day, hour_ending
    """
    logger.info("Pulling hourly gas prices (10 hubs) from ICE")
    df = pull_from_db(query=query)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # Rename to shorter feature-friendly names
    df = df.rename(columns=HUB_COLUMNS)

    logger.info(f"Pulled hourly gas: {len(df):,} rows, {df['date'].nunique():,} days, "
                f"{len(HUB_COLUMNS)} hubs")
    return df
