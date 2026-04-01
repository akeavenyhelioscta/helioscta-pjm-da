from pathlib import Path
import pandas as pd
import logging

from src.utils.azure_postgresql import pull_from_db
from src.data.sql_templates import render_sql_template

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent / "sql"


def pull(
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull active/approved transmission outages (≥230 kV lines, xfmrs, phase shifters)."""
    sql_file = SQL_DIR / "pjm_transmission_outages.sql"
    query = render_sql_template(sql_file, sql_overrides)

    logger.info("Pulling transmission outages from pjm.transmission_outages")

    df = pull_from_db(query=query)

    logger.info(f"Pulled transmission outages: {len(df):,} rows")
    return df
