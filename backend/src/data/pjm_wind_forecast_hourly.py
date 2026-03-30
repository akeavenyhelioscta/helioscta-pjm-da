"""Pull PJM official wind forecast (latest execution per target date)."""
from pathlib import Path

import logging
import pandas as pd

from src.utils.azure_postgresql import pull_from_db
from src.data.frame_validation import validate_source_frame
from src.data.sql_templates import render_sql_template

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent / "sql"


def pull(
    timezone: str = "America/New_York",
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull latest PJM wind forecast from today onward, one row per (date, hour_ending)."""
    sql_file = SQL_DIR / "pjm_wind_forecast_rto_latest.sql"
    overrides: dict[str, str | int | bool | None] = {"timezone": timezone}
    if sql_overrides:
        overrides.update(sql_overrides)
    query = render_sql_template(sql_file, overrides)
    logger.info("Pulling PJM wind forecast: RTO, today onward")

    df = pull_from_db(query=query)
    df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date
    df = validate_source_frame(
        df=df,
        source_name="pjm_wind_forecast_latest",
        required_columns=["forecast_date", "hour_ending", "forecast_execution_datetime", "wind_forecast"],
        unique_key_columns=["forecast_date", "hour_ending"],
        hourly_coverage_date_col="forecast_date",
        hourly_coverage_hour_col="hour_ending",
        drop_duplicate_keys=True,
    )
    logger.info(f"Pulled {len(df):,} rows")
    return df


def pull_da_cutoff_vintages(
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull 4 DA cutoff vintages: DA Cutoff, DA -12h, DA -24h, DA -48h."""
    sql_file = SQL_DIR / "pjm_wind_forecast_da_cutoff_vintages.sql"
    overrides: dict[str, str | int | bool | None] = {}
    if sql_overrides:
        overrides.update(sql_overrides)
    query = render_sql_template(sql_file, overrides)
    logger.info("Pulling PJM wind DA cutoff vintages")

    df = pull_from_db(query=query)
    df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date
    logger.info(f"Pulled {len(df):,} rows across vintages")
    return df
