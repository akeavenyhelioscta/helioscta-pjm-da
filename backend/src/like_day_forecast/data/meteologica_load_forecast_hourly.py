"""Pull Meteologica RTO load (demand) forecast (latest execution per target date)."""
from pathlib import Path

import pandas as pd
import logging

from src.like_day_forecast.utils.azure_postgresql import pull_from_db

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent.parent / "sql"


def pull(region: str = "RTO", lookback_days: int = 30) -> pd.DataFrame:
    """Pull Meteologica load forecast, one row per (date, hour_ending)."""
    sql_file = SQL_DIR / "meteologica_load_forecast_rto_hourly.sql"
    with open(sql_file, "r") as f:
        query = f.read()

    query = query.format(region=region, lookback_days=lookback_days)
    logger.info(f"Pulling Meteologica load forecast: {region}, last {lookback_days} days")

    df = pull_from_db(query=query)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    logger.info(f"Pulled {len(df):,} rows")
    return df


def pull_strip(region: str = "RTO") -> pd.DataFrame:
    """Pull latest Meteologica forecast vintage — full multi-day strip."""
    sql_file = SQL_DIR / "meteologica_load_forecast_strip_rto.sql"
    with open(sql_file, "r") as f:
        query = f.read()

    query = query.format(region=region)
    logger.info(f"Pulling Meteologica load forecast strip: {region}")

    df = pull_from_db(query=query)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    logger.info(f"Pulled {len(df):,} rows")
    return df
