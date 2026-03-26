import os
from dotenv import load_dotenv
from pathlib import Path

from src.like_day_forecast.utils.logging_utils import init_logging

# .env lives at src/.env (one level up from this file's package directory)
CONFIG_DIR = Path(__file__).parent.parent
load_dotenv(dotenv_path=CONFIG_DIR / ".env", override=False)

# Initialize colored pipeline logger
init_logging(name="pjm-like-day-forecast", log_dir=CONFIG_DIR.parent / "logs")

# Azure PostgreSQL
AZURE_POSTGRESQL_DB_HOST = os.getenv("AZURE_POSTGRESQL_DB_HOST")
AZURE_POSTGRESQL_DB_PORT = os.getenv("AZURE_POSTGRESQL_DB_PORT")
AZURE_POSTGRESQL_DB_NAME = os.getenv("AZURE_POSTGRESQL_DB_NAME")
AZURE_POSTGRESQL_DB_USER = os.getenv("AZURE_POSTGRESQL_DB_USER")
AZURE_POSTGRESQL_DB_PASSWORD = os.getenv("AZURE_POSTGRESQL_DB_PASSWORD")

# Azure Blob Storage
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")
