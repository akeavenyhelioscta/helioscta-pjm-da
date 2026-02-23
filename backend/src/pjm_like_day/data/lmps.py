from pathlib import Path

import pandas as pd

from src.utils.azure_postgresql import pull_from_db
from src.pjm_like_day import configs

import logging
logging.basicConfig(level=logging.DEBUG)

SQL_DIR = Path(__file__).parent.parent / "sql"


def pull(
        schema: str = configs.SCHEMA,
        hub: str = configs.HUB,
        market: str = "da",
    ) -> pd.DataFrame:

    sql_file = SQL_DIR / "pjm_lmps_hourly.sql"

    with open(sql_file, "r") as f:
        query = f.read()

    query = query.format(schema=schema, hub=hub, market=market)
    logging.info(f"Pulling LMP hourly data for {hub} ({market}) from {schema} ...")

    df = pull_from_db(query=query)
    logging.info(f"Pulled {len(df):,} rows")

    return df
