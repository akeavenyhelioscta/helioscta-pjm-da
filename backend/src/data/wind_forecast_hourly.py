from pathlib import Path
import pandas as pd
import logging

from src.utils.azure_postgresql import pull_from_db
from src.data.sql_templates import render_sql_template
from src.data.frame_validation import validate_source_frame

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent / "sql"


def pull(
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull D+1 wind forecast hourly from gridstatus.

    Filters to forecasts published the day before delivery,
    giving the next-day wind outlook available before market clearing.
    """
    sql_file = SQL_DIR / "wind_forecast_hourly.sql"
    query = render_sql_template(sql_file, sql_overrides)

    logger.info("Pulling wind forecast hourly from gridstatus")

    df = pull_from_db(query=query)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = validate_source_frame(
        df=df,
        source_name="wind_forecast_hourly_d1",
        required_columns=["date", "hour_ending", "wind_forecast"],
        unique_key_columns=["date", "hour_ending"],
        hourly_coverage_date_col="date",
        hourly_coverage_hour_col="hour_ending",
        drop_duplicate_keys=True,
    )

    logger.info(f"Pulled wind forecast: {len(df):,} rows, "
                f"{df['date'].nunique():,} days")
    return df
