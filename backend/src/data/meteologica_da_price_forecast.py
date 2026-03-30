"""Pull Meteologica DA price forecast (latest execution per target date)."""
from pathlib import Path

import logging
import pandas as pd

from src.utils.azure_postgresql import pull_from_db
from src.data.sql_templates import render_sql_template

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent / "sql"


def pull(
    hub: str = "SYSTEM",
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull latest Meteologica DA price forecast from today onward, one row per (date, hour_ending)."""
    sql_file = SQL_DIR / "meteologica_da_price_forecast_latest.sql"
    overrides: dict[str, str | int | bool | None] = {
        "hub": hub,
    }
    if sql_overrides:
        overrides.update(sql_overrides)
    query = render_sql_template(sql_file, overrides)
    logger.info(f"Pulling Meteologica DA price forecast: {hub}, today onward")

    df = pull_from_db(query=query)
    df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date
    logger.info(f"Pulled {len(df):,} rows")
    return df


def pull_da_cutoff_vintages(
    hub: str = "SYSTEM",
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull 4 DA cutoff vintages: DA Cutoff, DA -12h, DA -24h, DA -48h."""
    sql_file = SQL_DIR / "meteologica_da_price_forecast_da_cutoff_vintages.sql"
    overrides: dict[str, str | int | bool | None] = {
        "hub": hub,
    }
    if sql_overrides:
        overrides.update(sql_overrides)
    query = render_sql_template(sql_file, overrides)
    logger.info(f"Pulling Meteologica DA price DA cutoff vintages: {hub}")

    df = pull_from_db(query=query)
    df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date
    logger.info(f"Pulled {len(df):,} rows across vintages")
    return df
