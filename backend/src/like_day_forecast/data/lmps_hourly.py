from pathlib import Path
import pandas as pd
import logging

from src.like_day_forecast.utils.azure_postgresql import pull_from_db
from src.like_day_forecast.utils.sql_templates import render_sql_template
from src.like_day_forecast import configs

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent.parent / "sql"


def pull(
    schema: str = configs.SCHEMA,
    hub: str = configs.HUB,
    market: str = configs.TARGET_MARKET,
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull hourly LMP data for a given hub and market."""
    sql_file = SQL_DIR / "lmps_hourly.sql"
    overrides: dict[str, str | int | bool | None] = {
        "schema": schema,
        "hub": hub,
        "market": market,
    }
    if sql_overrides:
        overrides.update(sql_overrides)
    query = render_sql_template(sql_file, overrides)
    logger.info(f"Pulling LMP hourly: {hub} ({market}) from {schema}")

    df = pull_from_db(query=query)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    logger.info(f"Pulled {len(df):,} rows")
    return df
