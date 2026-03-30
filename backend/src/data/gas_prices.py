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
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull next-day gas prices from ICE (daily VWAP snapshot)."""
    sql_file = SQL_DIR / "gas_prices.sql"
    query = render_sql_template(sql_file, sql_overrides)

    logger.info("Pulling gas prices from ice_python_cleaned")

    df = pull_from_db(query=query)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = validate_source_frame(
        df=df,
        source_name="gas_prices",
        required_columns=["date", "gas_m3_price", "gas_hh_price"],
        unique_key_columns=["date"],
        drop_duplicate_keys=True,
    )

    logger.info(f"Pulled gas prices: {len(df):,} dates, columns: {list(df.columns[1:])}")
    return df
