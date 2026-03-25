"""Pull Meteologica ECMWF ensemble RTO load forecast (latest vintage strip)."""
from pathlib import Path

import pandas as pd
import logging

from src.like_day_forecast.utils.azure_postgresql import pull_from_db

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent.parent / "sql"


def pull_strip(region: str = "RTO") -> pd.DataFrame:
    """Pull latest Euro ensemble strip: avg + top/bottom bounds per hour."""
    sql_file = SQL_DIR / "meteologica_euro_ens_forecast_strip_rto.sql"
    with open(sql_file, "r") as f:
        query = f.read()

    query = query.format(region=region)
    logger.info(f"Pulling Meteologica Euro ensemble strip: {region}")

    df = pull_from_db(query=query)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    logger.info(f"Pulled {len(df):,} rows")
    return df
