from pathlib import Path
import pandas as pd
import logging

from src.utils.azure_postgresql import pull_from_db
from src.data.sql_templates import render_sql_template
from src.like_day_forecast import configs

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent / "sql"


def pull(
    schema: str = configs.SCHEMA,
    region: str = configs.LOAD_REGION,
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull actual daily outages (total/planned/maintenance/forced MW)."""
    sql_file = SQL_DIR / "outages_actual_daily.sql"
    overrides: dict[str, str | int | bool | None] = {"schema": schema, "region": region}
    if sql_overrides:
        overrides.update(sql_overrides)
    query = render_sql_template(sql_file, overrides)
    logger.info(f"Pulling outages actual daily: region={region}")

    df = pull_from_db(query=query)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    logger.info(f"Pulled outages: {len(df):,} days")
    return df
