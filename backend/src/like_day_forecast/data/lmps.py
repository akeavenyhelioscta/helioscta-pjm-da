from pathlib import Path

import pandas as pd

from src.utils.azure_postgresql import pull_from_db
from src.like_day_forecast import configs
from src.like_day_forecast.utils.sql_templates import render_sql_template

import logging
logging.basicConfig(level=logging.DEBUG)

SQL_DIR = Path(__file__).parent.parent / "sql"


def pull(
        schema: str = configs.SCHEMA,
        hub: str = configs.HUB,
        market: str = "da",
        sql_overrides: dict[str, str | int | bool | None] | None = None,
    ) -> pd.DataFrame:

    sql_file = SQL_DIR / "pjm_lmps_hourly.sql"
    overrides: dict[str, str | int | bool | None] = {
        "schema": schema,
        "hub": hub,
        "market": market,
    }
    if sql_overrides:
        overrides.update(sql_overrides)
    query = render_sql_template(sql_file, overrides)
    logging.info(f"Pulling LMP hourly data for {hub} ({market}) from {schema} ...")

    df = pull_from_db(query=query)
    logging.info(f"Pulled {len(df):,} rows")

    return df
