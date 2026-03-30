"""Pull Meteologica ECMWF ensemble load forecast (latest vintage strip)."""
from pathlib import Path

import pandas as pd
import logging

from src.utils.azure_postgresql import pull_from_db
from src.data.sql_templates import render_sql_template

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent / "sql"

REGIONS = ["RTO", "WEST", "MIDATL", "SOUTH"]


def pull_strip(
    region: str = "RTO",
    sql_overrides: dict[str, str | int | bool | None] | None = None,
) -> pd.DataFrame:
    """Pull latest Euro ensemble strip: avg + top/bottom bounds per hour."""
    sql_file = SQL_DIR / "meteologica_euro_ens_forecast_rto_latest.sql"
    overrides: dict[str, str | int | bool | None] = {"region": region}
    if sql_overrides:
        overrides.update(sql_overrides)
    query = render_sql_template(sql_file, overrides)
    logger.info(f"Pulling Meteologica Euro ensemble strip: {region}")

    df = pull_from_db(query=query)
    df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date
    logger.info(f"Pulled {len(df):,} rows")
    return df


def pull_strip_all_regions() -> pd.DataFrame:
    """Pull latest Euro ensemble strip for ALL regions in one DataFrame."""
    frames: list[pd.DataFrame] = []
    for region in REGIONS:
        df = pull_strip(region=region)
        if df is not None and len(df) > 0:
            df["region"] = region
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
