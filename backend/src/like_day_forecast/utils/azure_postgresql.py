import pandas as pd
import psycopg2
import logging

from src.like_day_forecast import settings

logger = logging.getLogger(__name__)

import warnings
warnings.simplefilter(action='ignore', category=Warning)


def _connect(database: str = settings.AZURE_POSTGRESQL_DB_NAME) -> psycopg2.extensions.connection:
    return psycopg2.connect(
        user=settings.AZURE_POSTGRESQL_DB_USER,
        password=settings.AZURE_POSTGRESQL_DB_PASSWORD,
        host=settings.AZURE_POSTGRESQL_DB_HOST,
        port=settings.AZURE_POSTGRESQL_DB_PORT,
        dbname=database,
    )


def pull_from_db(query: str, database: str = settings.AZURE_POSTGRESQL_DB_NAME) -> pd.DataFrame:
    try:
        connection = _connect(database=database)
        df = pd.read_sql(query, connection)
        connection.close()
        return df
    except Exception as e:
        logger.error(f"Database pull failed: {e}")
        raise
