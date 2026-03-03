import os
from dotenv import load_dotenv
from pathlib import Path

from src.pjm_da_forecast.utils.logging_utils import init_logging

CONFIG_DIR = Path(__file__).parent
load_dotenv(dotenv_path=CONFIG_DIR / ".env", override=False)

# Initialize colored pipeline logger (captures all logging.getLogger() calls)
init_logging(name="pjm-da-forecast", log_dir=CONFIG_DIR.parent / "logs")

# Azure PostgreSQL
AZURE_POSTGRESQL_DB_HOST = os.getenv("AZURE_POSTGRESQL_DB_HOST")
AZURE_POSTGRESQL_DB_PORT = os.getenv("AZURE_POSTGRESQL_DB_PORT")
AZURE_POSTGRESQL_DB_NAME = os.getenv("AZURE_POSTGRESQL_DB_NAME")
AZURE_POSTGRESQL_DB_USER = os.getenv("AZURE_POSTGRESQL_DB_USER")
AZURE_POSTGRESQL_DB_PASSWORD = os.getenv("AZURE_POSTGRESQL_DB_PASSWORD")
