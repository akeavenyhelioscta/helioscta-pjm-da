from pathlib import Path
import pandas as pd
import logging

from src.like_day_forecast.utils.azure_postgresql import pull_from_db

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent.parent / "sql"


def pull() -> pd.DataFrame:
    """Pull D+1 solar forecast hourly from gridstatus.

    Filters to forecasts published the day before delivery,
    giving the next-day solar outlook available before market clearing.
    """
    sql_file = SQL_DIR / "solar_forecast_hourly.sql"
    with open(sql_file, "r") as f:
        query = f.read()

    logger.info("Pulling solar forecast hourly from gridstatus")

    df = pull_from_db(query=query)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    logger.info(f"Pulled solar forecast: {len(df):,} rows, "
                f"{df['date'].nunique():,} days")
    return df
