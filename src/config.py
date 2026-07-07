"""Configuration values for the stock market ETL pipeline."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

# --- Tickers and data window ---
TICKERS = [
    ticker.strip().upper()
    for ticker in os.getenv("TICKERS", "AAPL,MSFT,NVDA,GOOGL,AMZN").split(",")
    if ticker.strip()
]
INTERVAL = os.getenv("INTERVAL", "1d")
# Maximum calendar days to look back when a ticker has no stored data yet.
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "730"))

# --- Storage ---
DATABASE_PATH = PROJECT_ROOT / "data" / "stock_market.db"
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"sqlite:///{DATABASE_PATH.as_posix()}",
)

# --- Logging ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR = PROJECT_ROOT / os.getenv("LOG_DIR", "logs")
REPORTS_DIR = PROJECT_ROOT / os.getenv("REPORTS_DIR", "reports")

# --- Bronze layer (raw data landing) ---
# "local" writes Parquet under data/bronze/; "azure" writes to Blob Storage.
BRONZE_TARGET = os.getenv("BRONZE_TARGET", "local").lower()
BRONZE_LOCAL_DIR = PROJECT_ROOT / os.getenv("BRONZE_LOCAL_DIR", "data/bronze")
AZURE_STORAGE_CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "bronze")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_STORAGE_ACCOUNT_URL = os.getenv("AZURE_STORAGE_ACCOUNT_URL")


def get_database_location() -> str:
    """Return a human-readable database location for pipeline logging.

    Never returns credentials: non-SQLite URLs are reduced to host/database.
    """
    if DATABASE_URL.startswith("sqlite:///"):
        database_value = DATABASE_URL.removeprefix("sqlite:///")
        database_path = Path(database_value)
        if database_path.is_absolute():
            try:
                return str(database_path.relative_to(PROJECT_ROOT))
            except ValueError:
                return str(database_path)
        return str(database_path)

    from sqlalchemy.engine import make_url

    try:
        url = make_url(DATABASE_URL)
        return f"{url.drivername}://{url.host}/{url.database}"
    except Exception:
        return "<database url hidden>"
