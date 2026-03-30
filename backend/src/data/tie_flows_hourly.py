from pathlib import Path
import pandas as pd
import logging

from src.utils.azure_postgresql import pull_from_db
from src.data.sql_templates import render_sql_template

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent / "sql"


def pull(
    tie_flow_name: str | None = None,
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull PJM tie flows hourly. If tie_flow_name is None, pulls all tie flows."""
    sql_file = SQL_DIR / "tie_flows_hourly.sql"
    overrides: dict[str, str | int | bool | None] = {"tie_flow_name": tie_flow_name or ""}
    if sql_overrides:
        overrides.update(sql_overrides)
    query = render_sql_template(sql_file, overrides)
    label = tie_flow_name or "ALL TIE FLOWS"
    logger.info(f"Pulling tie flows hourly: {label}")

    df = pull_from_db(query=query)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    logger.info(f"Pulled {len(df):,} rows")
    return df
