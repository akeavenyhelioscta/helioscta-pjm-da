from pathlib import Path
import pandas as pd
import logging

from src.like_day_forecast.utils.azure_postgresql import pull_from_db
from src.like_day_forecast import configs

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent.parent / "sql"


def pull_daily(
    schema: str = configs.SCHEMA,
) -> pd.DataFrame:
    """Pull daily calendar features."""
    sql_file = SQL_DIR / "dates_daily.sql"
    with open(sql_file, "r") as f:
        query = f.read()

    query = query.format(schema=schema)
    logger.info(f"Pulling daily calendar from {schema}")

    df = pull_from_db(query=query)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    logger.info(f"Pulled {len(df):,} rows")
    return df
