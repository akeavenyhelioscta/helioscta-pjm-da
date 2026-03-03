from pathlib import Path
import pandas as pd
import logging

from pjm_like_day_forecast.utils.azure_postgresql import pull_from_db
from pjm_like_day_forecast import configs

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent.parent / "sql"


def pull(
    schema: str = configs.WEATHER_SCHEMA,
    region: str = configs.WEATHER_REGION,
    station: str = configs.WEATHER_STATION,
) -> pd.DataFrame:
    """Pull hourly observed weather data for PJM aggregate station (1995+)."""
    sql_file = SQL_DIR / "weather_hourly.sql"
    with open(sql_file, "r") as f:
        query = f.read()

    query = query.format(schema=schema, region=region, station=station)
    logger.info(f"Pulling observed weather hourly: {station} from {schema}")

    df = pull_from_db(query=query)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    logger.info(f"Pulled {len(df):,} rows")
    return df
