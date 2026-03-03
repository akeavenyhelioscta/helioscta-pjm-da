from pathlib import Path
import pandas as pd
import logging

from src.pjm_da_forecast.utils.azure_postgresql import pull_from_db
from src.pjm_da_forecast import configs

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent.parent / "sql"


def pull(
    schema: str = configs.SCHEMA,
    hub: str = configs.HUB,
    market: str = configs.TARGET_MARKET,
) -> pd.DataFrame:
    """Pull hourly LMP data for a given hub and market."""
    sql_file = SQL_DIR / "lmps_hourly.sql"
    with open(sql_file, "r") as f:
        query = f.read()

    query = query.format(schema=schema, hub=hub, market=market)
    logger.info(f"Pulling LMP hourly: {hub} ({market}) from {schema}")

    df = pull_from_db(query=query)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    logger.info(f"Pulled {len(df):,} rows")
    return df
