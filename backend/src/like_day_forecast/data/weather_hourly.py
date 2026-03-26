from pathlib import Path
import pandas as pd
import logging

from src.like_day_forecast.utils.azure_postgresql import pull_from_db
from src.like_day_forecast.utils.sql_templates import render_sql_template
from src.like_day_forecast import configs

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent.parent / "sql"


def pull(
    schema: str = configs.WEATHER_SCHEMA,
    station: str = configs.WEATHER_STATION,
    start_date: str = configs.FULL_FEATURE_START,
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull hourly weather (observed + forecast) for PJM aggregate station."""
    sql_file = SQL_DIR / "weather_hourly.sql"
    overrides: dict[str, str | int | bool | None] = {
        "schema": schema,
        "station": station,
        "start_date": start_date,
    }
    if sql_overrides:
        overrides.update(sql_overrides)
    query = render_sql_template(sql_file, overrides)
    logger.info(f"Pulling weather hourly: {station} from {schema}")

    df = pull_from_db(query=query)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    logger.info(f"Pulled {len(df):,} rows")
    return df
