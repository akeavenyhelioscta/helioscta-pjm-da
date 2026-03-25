from pathlib import Path
import pandas as pd
import logging

from src.like_day_forecast.utils.azure_postgresql import pull_from_db
from src.like_day_forecast import configs

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent.parent / "sql"


def pull(
    schema: str = configs.WEATHER_SCHEMA,
    station: str = configs.WEATHER_STATION,
    start_date: str = configs.FULL_FEATURE_START,
) -> pd.DataFrame:
    """Pull hourly weather (observed + forecast) for PJM aggregate station."""
    sql_file = SQL_DIR / "weather_hourly.sql"
    with open(sql_file, "r") as f:
        query = f.read()

    query = query.format(schema=schema, station=station, start_date=start_date)
    logger.info(f"Pulling weather hourly: {station} from {schema}")

    df = pull_from_db(query=query)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    logger.info(f"Pulled {len(df):,} rows")
    return df
