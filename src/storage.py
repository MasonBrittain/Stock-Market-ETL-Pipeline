"""Bronze-layer storage backends for raw extracted data.

The Bronze layer lands raw yfinance data as Parquet before transformation,
following the medallion architecture pattern. Two backends are provided:

- LocalBronzeStorage  — writes under data/bronze/ (default; zero dependencies)
- AzureBronzeStorage  — writes to an Azure Blob Storage container

Both use the same path scheme so a later cloud migration is a config change:
    ticker=AAPL/year=2026/AAPL_2026_{batch_id}.parquet

Selected via the BRONZE_TARGET config value ("local" | "azure").
"""

from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import Protocol

import pandas as pd

logger = logging.getLogger(__name__)


class BronzeStorage(Protocol):
    """Interface for Bronze-layer writers."""

    def write_bronze(self, raw_data: pd.DataFrame, ticker: str, batch_id: str) -> str:
        """Persist one ticker's raw extract; return the path/URI written."""
        ...


def _blob_path(raw_data: pd.DataFrame, ticker: str, batch_id: str) -> str:
    """Build the partitioned relative path for one ticker's extract."""
    dates = pd.to_datetime(raw_data.index if raw_data.index.name else raw_data.get("Date", raw_data.index))
    try:
        year = int(pd.DatetimeIndex(dates).year.max())
    except (TypeError, ValueError):
        year = pd.Timestamp.now().year
    return f"ticker={ticker}/year={year}/{ticker}_{year}_{batch_id}.parquet"


class LocalBronzeStorage:
    """Writes Bronze Parquet files to the local filesystem."""

    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir

    def write_bronze(self, raw_data: pd.DataFrame, ticker: str, batch_id: str) -> str:
        relative = _blob_path(raw_data, ticker, batch_id)
        target = self.base_dir / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        raw_data.to_parquet(target)
        logger.info("Bronze written (local) → %s", target)
        return str(target)


class AzureBronzeStorage:
    """Writes Bronze Parquet files to an Azure Blob Storage container.

    Authentication order:
    1. AZURE_STORAGE_CONNECTION_STRING if set (simplest for local testing)
    2. DefaultAzureCredential against AZURE_STORAGE_ACCOUNT_URL
       (managed identity in Azure, az-cli login locally)
    """

    def __init__(
        self,
        container: str,
        connection_string: str | None = None,
        account_url: str | None = None,
    ) -> None:
        # Imported lazily so the azure-storage-blob package is only required
        # when BRONZE_TARGET=azure is actually configured.
        from azure.storage.blob import BlobServiceClient

        if connection_string:
            service = BlobServiceClient.from_connection_string(connection_string)
        elif account_url:
            from azure.identity import DefaultAzureCredential

            service = BlobServiceClient(account_url, credential=DefaultAzureCredential())
        else:
            raise ValueError(
                "AzureBronzeStorage requires AZURE_STORAGE_CONNECTION_STRING "
                "or AZURE_STORAGE_ACCOUNT_URL to be configured."
            )
        self._container = service.get_container_client(container)

    def write_bronze(self, raw_data: pd.DataFrame, ticker: str, batch_id: str) -> str:
        relative = _blob_path(raw_data, ticker, batch_id)
        buffer = io.BytesIO()
        raw_data.to_parquet(buffer)
        buffer.seek(0)
        self._container.upload_blob(name=relative, data=buffer, overwrite=True)
        uri = f"{self._container.url}/{relative}"
        logger.info("Bronze written (azure) → %s", uri)
        return uri


def create_bronze_storage(
    target: str,
    local_dir: Path,
    azure_container: str = "bronze",
    azure_connection_string: str | None = None,
    azure_account_url: str | None = None,
) -> BronzeStorage:
    """Factory: return the Bronze backend selected by BRONZE_TARGET."""
    if target == "azure":
        return AzureBronzeStorage(
            container=azure_container,
            connection_string=azure_connection_string,
            account_url=azure_account_url,
        )
    if target == "local":
        return LocalBronzeStorage(base_dir=local_dir)
    raise ValueError(f"Unknown BRONZE_TARGET: {target!r} (expected 'local' or 'azure')")
