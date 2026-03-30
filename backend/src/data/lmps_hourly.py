from pathlib import Path
import pandas as pd
import logging

from src.utils.azure_postgresql import pull_from_db
from src.data.sql_templates import render_sql_template
from src.data.frame_validation import validate_source_frame
from src.like_day_forecast import configs

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent / "sql"


def pull(
    schema: str = configs.SCHEMA,
    hub: str | None = None,
    market: str = configs.TARGET_MARKET,
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull hourly LMP data. If hub is None, pulls all hubs."""
    sql_file = SQL_DIR / "lmps_hourly.sql"
    overrides: dict[str, str | int | bool | None] = {
        "schema": schema,
        "hub": hub or "",
        "market": market,
    }
    if sql_overrides:
        overrides.update(sql_overrides)
    query = render_sql_template(sql_file, overrides)
    label = hub or "ALL HUBS"
    logger.info(f"Pulling LMP hourly: {label} ({market}) from {schema}")

    df = pull_from_db(query=query)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = validate_source_frame(
        df=df,
        source_name=f"lmps_hourly[{market}]",
        required_columns=["date", "hour_ending", "hub", "market", "lmp_total"],
        unique_key_columns=["date", "hour_ending", "hub", "market"],
        hourly_coverage_date_col="date",
        hourly_coverage_hour_col="hour_ending",
        drop_duplicate_keys=True,
    )
    logger.info(f"Pulled {len(df):,} rows")
    return df
