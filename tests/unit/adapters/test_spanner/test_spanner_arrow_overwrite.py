"""Unit tests for load_from_arrow(overwrite=True) using Partitioned DML."""

from unittest.mock import MagicMock, patch

import pyarrow as pa

from sqlspec.adapters.spanner.driver import SpannerSyncDriver

CAPABILITIES = {
    "arrow_export_enabled": True,
    "arrow_import_enabled": True,
    "parquet_export_enabled": True,
    "parquet_import_enabled": True,
    "partition_strategies": ["fixed"],
}


def test_load_from_arrow_overwrite_uses_partitioned_dml() -> None:
    """Verify load_from_arrow(overwrite=True) calls execute_partitioned_dml for truncation."""
    mock_db = MagicMock()
    mock_db.execute_partitioned_dml.return_value = 1000

    mock_connection = MagicMock()
    mock_connection._session._database = mock_db

    driver = SpannerSyncDriver(connection=mock_connection, driver_features={"storage_capabilities": CAPABILITIES})

    arrow_table = pa.table({"id": [1, 2], "name": ["a", "b"]})

    with patch.object(SpannerSyncDriver, "_arrow_table_to_rows", return_value=(["id", "name"], [])):
        driver.load_from_arrow("users", arrow_table, overwrite=True)

    mock_db.execute_partitioned_dml.assert_called_once()
    sql = mock_db.execute_partitioned_dml.call_args[0][0]
    assert "DELETE FROM users WHERE TRUE" in sql
