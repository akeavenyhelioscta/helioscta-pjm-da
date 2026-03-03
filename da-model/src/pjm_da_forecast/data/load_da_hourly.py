from pathlib import Path
import pandas as pd
import logging

from src.pjm_da_forecast.utils.azure_postgresql import pull_from_db
from src.pjm_da_forecast import configs

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent.parent / "sql"


def pull(
    schema: str = configs.SCHEMA,
    region: str = configs.LOAD_REGION,
) -> pd.DataFrame:
    """Pull DA load hourly data (2020+)."""
    sql_file = SQL_DIR / "load_da_hourly.sql"
    with open(sql_file, "r") as f:
        query = f.read()

    query = query.format(schema=schema, region=region)
    logger.info(f"Pulling DA load hourly: {region} from {schema}")

    df = pull_from_db(query=query)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    logger.info(f"Pulled {len(df):,} rows")
    return df
