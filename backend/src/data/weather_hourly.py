from pathlib import Path
import pandas as pd
import logging

from src.utils.azure_postgresql import pull_from_db
from src.data.sql_templates import render_sql_template
from src.data.frame_validation import validate_source_frame
from src.like_day_forecast import configs

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent / "sql"


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
    df = validate_source_frame(
        df=df,
        source_name="weather_hourly",
        required_columns=["date", "hour_ending", "temp"],
        unique_key_columns=["date", "hour_ending", "station_name"],
        hourly_coverage_date_col="date",
        hourly_coverage_hour_col="hour_ending",
        drop_duplicate_keys=True,
    )
    logger.info(f"Pulled {len(df):,} rows")
    return df
