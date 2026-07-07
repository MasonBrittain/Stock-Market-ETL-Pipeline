"""Tests for the Bronze-layer storage backends."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.storage import LocalBronzeStorage, create_bronze_storage


def _raw_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Open": [150.0, 151.0],
            "High": [155.0, 156.0],
            "Low": [149.0, 150.0],
            "Close": [153.0, 154.0],
            "Adj Close": [153.0, 154.0],
            "Volume": [1_000_000, 1_100_000],
        },
        index=pd.DatetimeIndex(["2026-06-25", "2026-06-26"], name="Date"),
    )


def test_local_bronze_writes_partitioned_parquet(tmp_path: Path) -> None:
    storage = LocalBronzeStorage(base_dir=tmp_path)

    written = storage.write_bronze(_raw_frame(), "AAPL", "batch-abc")

    expected = tmp_path / "ticker=AAPL" / "year=2026" / "AAPL_2026_batch-abc.parquet"
    assert Path(written) == expected
    assert expected.exists()

    round_trip = pd.read_parquet(expected)
    assert len(round_trip) == 2
    assert round_trip["Close"].iloc[1] == 154.0


def test_local_bronze_creates_nested_directories(tmp_path: Path) -> None:
    storage = LocalBronzeStorage(base_dir=tmp_path / "deep" / "nested")
    storage.write_bronze(_raw_frame(), "MSFT", "batch-1")
    assert (tmp_path / "deep" / "nested" / "ticker=MSFT" / "year=2026").is_dir()


def test_factory_returns_local_backend(tmp_path: Path) -> None:
    storage = create_bronze_storage(target="local", local_dir=tmp_path)
    assert isinstance(storage, LocalBronzeStorage)


def test_factory_rejects_unknown_target(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Unknown BRONZE_TARGET"):
        create_bronze_storage(target="s3", local_dir=tmp_path)


def test_azure_backend_requires_credentials(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="AzureBronzeStorage requires"):
        create_bronze_storage(target="azure", local_dir=tmp_path)


def test_azure_bronze_uploads_blob_with_partitioned_name() -> None:
    mock_container = MagicMock()
    mock_container.url = "https://acct.blob.core.windows.net/bronze"
    mock_service = MagicMock()
    mock_service.get_container_client.return_value = mock_container

    with patch(
        "azure.storage.blob.BlobServiceClient.from_connection_string",
        return_value=mock_service,
    ):
        from src.storage import AzureBronzeStorage

        storage = AzureBronzeStorage(
            container="bronze",
            connection_string="DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=key;",
        )
        uri = storage.write_bronze(_raw_frame(), "NVDA", "batch-xyz")

    mock_container.upload_blob.assert_called_once()
    call_kwargs = mock_container.upload_blob.call_args.kwargs
    assert call_kwargs["name"] == "ticker=NVDA/year=2026/NVDA_2026_batch-xyz.parquet"
    assert call_kwargs["overwrite"] is True
    assert uri.endswith("NVDA_2026_batch-xyz.parquet")
