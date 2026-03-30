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
    region: str | None = None,
    lookback_days: int = 14,
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull 7-day outage forecasts (total/planned/maintenance/forced MW).

    If region is None, pulls all regions.
    """
    sql_file = SQL_DIR / "outages_forecast_daily.sql"
    overrides: dict[str, str | int | bool | None] = {
        "schema": schema,
        "region": region or "",
        "lookback_days": lookback_days,
    }
    if sql_overrides:
        overrides.update(sql_overrides)
    query = render_sql_template(sql_file, overrides)
    label = region or "ALL REGIONS"
    logger.info(f"Pulling outages forecast daily: {label}, lookback={lookback_days}d")

    df = pull_from_db(query=query)
    df["forecast_execution_date"] = pd.to_datetime(df["forecast_execution_date"]).dt.date
    df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date
    df = validate_source_frame(
        df=df,
        source_name="outages_forecast_daily",
        required_columns=[
            "forecast_execution_date",
            "forecast_date",
            "region",
            "total_outages_mw",
            "forced_outages_mw",
        ],
        unique_key_columns=["forecast_execution_date", "forecast_date", "region", "forecast_rank"],
        drop_duplicate_keys=True,
    )

    logger.info(f"Pulled outage forecasts: {len(df):,} rows")
    return df
