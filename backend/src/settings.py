import os
from dotenv import load_dotenv
from pathlib import Path

# init logging
import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger().handlers[0].setLevel(logging.DEBUG)

# Get the directory where this config file lives
CONFIG_DIR = Path(__file__).parent
logging.info(f"CONFIG_DIR: {CONFIG_DIR}")
load_dotenv(dotenv_path=CONFIG_DIR / ".env", override=False)

#===============================================
# AZURE POSTGRESQL DATABASE
#===============================================

AZURE_POSTGRESQL_DB_HOST = os.getenv("AZURE_POSTGRESQL_DB_HOST")
AZURE_POSTGRESQL_DB_PORT = os.getenv("AZURE_POSTGRESQL_DB_PORT")
AZURE_POSTGRESQL_DB_NAME = os.getenv("AZURE_POSTGRESQL_DB_NAME")
AZURE_POSTGRESQL_DB_USER = os.getenv("AZURE_POSTGRESQL_DB_USER")
AZURE_POSTGRESQL_DB_PASSWORD = os.getenv("AZURE_POSTGRESQL_DB_PASSWORD")