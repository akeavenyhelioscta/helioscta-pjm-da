"""Pull Meteologica ECMWF ensemble RTO load forecast (latest vintage strip)."""
from pathlib import Path

import pandas as pd
import logging

from src.like_day_forecast.utils.azure_postgresql import pull_from_db
from src.like_day_forecast.utils.sql_templates import render_sql_template

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent.parent / "sql"


def pull_strip(
    region: str = "RTO",
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull latest Euro ensemble strip: avg + top/bottom bounds per hour."""
    sql_file = SQL_DIR / "meteologica_euro_ens_forecast_rto_latest.sql"
    overrides: dict[str, str | int | bool | None] = {"region": region}
    if sql_overrides:
        overrides.update(sql_overrides)
    query = render_sql_template(sql_file, overrides)
    logger.info(f"Pulling Meteologica Euro ensemble strip: {region}")

    df = pull_from_db(query=query)
    df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date
    logger.info(f"Pulled {len(df):,} rows")
    return df
