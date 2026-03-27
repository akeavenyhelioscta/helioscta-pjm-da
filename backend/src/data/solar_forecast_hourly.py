from pathlib import Path
import pandas as pd
import logging

from src.utils.azure_postgresql import pull_from_db
from src.data.sql_templates import render_sql_template

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent / "sql"


def pull(
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull D+1 solar forecast hourly from gridstatus.

    Filters to forecasts published the day before delivery,
    giving the next-day solar outlook available before market clearing.
    """
    sql_file = SQL_DIR / "solar_forecast_hourly.sql"
    query = render_sql_template(sql_file, sql_overrides)

    logger.info("Pulling solar forecast hourly from gridstatus")

    df = pull_from_db(query=query)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    logger.info(f"Pulled solar forecast: {len(df):,} rows, "
                f"{df['date'].nunique():,} days")
    return df
