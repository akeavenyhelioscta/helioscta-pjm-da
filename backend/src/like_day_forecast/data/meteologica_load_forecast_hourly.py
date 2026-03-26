"""Pull Meteologica RTO load (demand) forecast (latest execution per target date)."""
from pathlib import Path

import logging
import pandas as pd

from src.like_day_forecast.utils.azure_postgresql import pull_from_db
from src.like_day_forecast.utils.sql_templates import render_sql_template

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent.parent / "sql"


def pull(
    region: str = "RTO",
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull latest Meteologica load forecast from today onward, one row per (date, hour_ending)."""
    sql_file = SQL_DIR / "meteologica_load_forecast_rto_latest.sql"
    overrides: dict[str, str | int | bool | None] = {
        "region": region,
    }
    if sql_overrides:
        overrides.update(sql_overrides)
    query = render_sql_template(sql_file, overrides)
    logger.info(f"Pulling Meteologica load forecast: {region}, today onward")

    df = pull_from_db(query=query)
    df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date
    logger.info(f"Pulled {len(df):,} rows")
    return df


def pull_da_cutoff_vintages(
    region: str = "RTO",
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull 4 DA cutoff vintages: DA Cutoff, DA -12h, DA -24h, DA -48h."""
    sql_file = SQL_DIR / "meteologica_load_forecast_da_cutoff_vintages.sql"
    overrides: dict[str, str | int | bool | None] = {
        "region": region,
    }
    if sql_overrides:
        overrides.update(sql_overrides)
    query = render_sql_template(sql_file, overrides)
    logger.info(f"Pulling Meteologica DA cutoff vintages: {region}")

    df = pull_from_db(query=query)
    df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date
    logger.info(f"Pulled {len(df):,} rows across vintages")
    return df


def pull_strip(
    region: str = "RTO",
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull latest Meteologica forecast vintage - full multi-day strip."""
    sql_file = SQL_DIR / "meteologica_load_forecast_rto_latest.sql"
    overrides: dict[str, str | int | bool | None] = {"region": region}
    if sql_overrides:
        overrides.update(sql_overrides)
    query = render_sql_template(sql_file, overrides)
    logger.info(f"Pulling Meteologica load forecast strip: {region}")

    df = pull_from_db(query=query)
    df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date
    logger.info(f"Pulled {len(df):,} rows")
    return df
