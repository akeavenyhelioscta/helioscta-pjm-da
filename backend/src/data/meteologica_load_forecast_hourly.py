"""Pull Meteologica RTO load (demand) forecast (latest execution per target date)."""
from pathlib import Path

import logging
import pandas as pd

from src.utils.azure_postgresql import pull_from_db
from src.data.sql_templates import render_sql_template

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent / "sql"


def pull(
    region: str = "RTO",
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull latest Meteologica load forecast from today onward.

    Pass region="" to pull all regions in one query.
    """
    sql_file = SQL_DIR / "meteologica_load_forecast_latest.sql"
    overrides: dict[str, str | int | bool | None] = {
        "region": region,
    }
    if sql_overrides:
        overrides.update(sql_overrides)
    query = render_sql_template(sql_file, overrides)
    label = region or "ALL REGIONS"
    logger.info(f"Pulling Meteologica load forecast: {label}, today onward")

    df = pull_from_db(query=query)
    df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date
    logger.info(f"Pulled {len(df):,} rows")
    return df


def pull_da_cutoff_vintages(
    region: str = "RTO",
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull 4 DA cutoff vintages: DA Cutoff, DA -12h, DA -24h, DA -48h.

    Pass region="" to pull all regions in one query.
    """
    sql_file = SQL_DIR / "meteologica_load_forecast_da_cutoff_vintages.sql"
    overrides: dict[str, str | int | bool | None] = {
        "region": region,
    }
    if sql_overrides:
        overrides.update(sql_overrides)
    query = render_sql_template(sql_file, overrides)
    label = region or "ALL REGIONS"
    logger.info(f"Pulling Meteologica DA cutoff vintages: {label}")

    df = pull_from_db(query=query)
    df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date
    logger.info(f"Pulled {len(df):,} rows across vintages")
    return df
