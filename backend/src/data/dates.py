from pathlib import Path
import pandas as pd
import logging

from src.utils.azure_postgresql import pull_from_db
from src.data.sql_templates import render_sql_template
from src.data.frame_validation import validate_source_frame
from src.like_day_forecast import configs

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent / "sql"


def pull_daily(
    schema: str = configs.SCHEMA,
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull daily calendar features."""
    sql_file = SQL_DIR / "dates_daily.sql"
    overrides: dict[str, str | int | bool | None] = {"schema": schema}
    if sql_overrides:
        overrides.update(sql_overrides)
    query = render_sql_template(sql_file, overrides)
    logger.info(f"Pulling daily calendar from {schema}")

    df = pull_from_db(query=query)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = validate_source_frame(
        df=df,
        source_name="dates_daily",
        required_columns=[
            "date",
            "day_of_week_number",
            "is_weekend",
            "is_nerc_holiday",
            "summer_winter",
        ],
        unique_key_columns=["date"],
        drop_duplicate_keys=True,
    )
    logger.info(f"Pulled {len(df):,} rows")
    return df
