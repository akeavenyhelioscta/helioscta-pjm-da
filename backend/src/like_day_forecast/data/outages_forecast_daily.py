from pathlib import Path
import pandas as pd
import logging

from src.like_day_forecast.utils.azure_postgresql import pull_from_db
from src.like_day_forecast.utils.sql_templates import render_sql_template
from src.like_day_forecast import configs

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent.parent / "sql"


def pull(
    schema: str = configs.SCHEMA,
    region: str = configs.LOAD_REGION,
    lookback_days: int = 14,
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull 7-day outage forecasts (total/planned/maintenance/forced MW)."""
    sql_file = SQL_DIR / "outages_forecast_daily.sql"
    overrides: dict[str, str | int | bool | None] = {
        "schema": schema,
        "region": region,
        "lookback_days": lookback_days,
    }
    if sql_overrides:
        overrides.update(sql_overrides)
    query = render_sql_template(sql_file, overrides)
    logger.info(f"Pulling outages forecast daily: region={region}, lookback={lookback_days}d")

    df = pull_from_db(query=query)
    df["forecast_execution_date"] = pd.to_datetime(df["forecast_execution_date"]).dt.date
    df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date

    logger.info(f"Pulled outage forecasts: {len(df):,} rows")
    return df
