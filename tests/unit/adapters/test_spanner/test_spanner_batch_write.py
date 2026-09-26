"""Unit tests for load_from_arrow with enable_batch_write_api."""

from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from sqlspec.adapters.spanner.driver import SpannerSyncDriver
from sqlspec.exceptions import SQLConversionError

CAPABILITIES = {
    "arrow_export_enabled": True,
    "arrow_import_enabled": True,
    "parquet_export_enabled": True,
    "parquet_import_enabled": True,
    "partition_strategies": ["fixed"],
}


def test_batch_write_succeeds_without_transaction() -> None:
    """Verify load_from_arrow with enable_batch_write_api succeeds on non-transaction connection."""
    mock_db = MagicMock()
    mock_mg = MagicMock()
    mock_db.mutation_groups.return_value.__enter__.return_value = mock_mg
    mock_group = MagicMock()
    mock_mg.group.return_value = mock_group
    mock_response = MagicMock()
    mock_response.status = None
    mock_mg.batch_write.return_value = [mock_response]

    mock_snapshot = MagicMock()
    mock_snapshot._session._database = mock_db

    driver = SpannerSyncDriver(
        connection=mock_snapshot, driver_features={"storage_capabilities": CAPABILITIES, "enable_batch_write_api": True}
    )

    arrow_table = pa.table({"id": [1, 2], "name": ["alice", "bob"]})

    job = driver.load_from_arrow("users", arrow_table)
    assert job.telemetry["rows_processed"] == 2
    mock_db.mutation_groups.assert_called_once()
    mock_mg.batch_write.assert_called_once()
    mock_group.insert_or_update.assert_called_once()


def test_standard_insert_requires_transaction() -> None:
    """Verify load_from_arrow without enable_batch_write_api still requires a SpannerTransaction."""
    mock_snapshot = MagicMock()

    driver = SpannerSyncDriver(
        connection=mock_snapshot,
        driver_features={"storage_capabilities": CAPABILITIES, "enable_batch_write_api": False},
    )

    arrow_table = pa.table({"id": [1, 2], "name": ["alice", "bob"]})

    with pytest.raises(SQLConversionError, match=r"Arrow import requires a Transaction context\."):
        driver.load_from_arrow("users", arrow_table)
