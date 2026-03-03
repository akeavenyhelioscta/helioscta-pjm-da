from pathlib import Path
import pandas as pd
import logging

from src.pjm_da_forecast.utils.azure_postgresql import pull_from_db
from src.pjm_da_forecast import configs

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent.parent / "sql"


def pull(
    hubs: list[str] | None = None,
) -> pd.DataFrame:
    """Pull next-day gas prices from ICE for specified hubs."""
    if hubs is None:
        hubs = configs.GAS_HUBS

    sql_file = SQL_DIR / "gas_prices.sql"
    with open(sql_file, "r") as f:
        query = f.read()

    hubs_str = ", ".join(f"'{h}'" for h in hubs)
    query = query.format(hubs=hubs_str)
    logger.info(f"Pulling gas prices for hubs: {hubs}")

    df = pull_from_db(query=query)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # Pivot to wide format: one column per hub
    df_wide = df.pivot_table(index="date", columns="hub", values="price").reset_index()
    df_wide.columns.name = None

    # Rename columns to standardized names
    rename_map = {
        "M3": "gas_m3_price",
        "HH": "gas_hh_price",
        "Transco Z6 NY": "gas_transco_z6_price",
    }
    df_wide = df_wide.rename(columns=rename_map)

    logger.info(f"Pulled gas prices: {len(df_wide):,} dates, hubs: {list(df_wide.columns[1:])}")
    return df_wide
