from pathlib import Path
import pandas as pd
import logging

from src.like_day_forecast.utils.azure_postgresql import pull_from_db
from src.like_day_forecast import configs

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent.parent / "sql"


def pull(
    schema: str = configs.SCHEMA,
    region: str = configs.LOAD_REGION,
) -> pd.DataFrame:
    """Pull RT metered load hourly data (2014+)."""
    sql_file = SQL_DIR / "load_rt_metered_hourly.sql"
    with open(sql_file, "r") as f:
        query = f.read()

    query = query.format(schema=schema, region=region)
    logger.info(f"Pulling RT metered load hourly: {region} from {schema}")

    df = pull_from_db(query=query)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    logger.info(f"Pulled {len(df):,} rows")
    return df
