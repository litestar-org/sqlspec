"""Unit tests for Spanner Partitioned DML execution on driver and config."""

from unittest.mock import MagicMock, patch

from google.cloud.spanner_v1.types.type import TypeCode

from sqlspec.adapters.spanner.config import SpannerSyncConfig
from sqlspec.adapters.spanner.core import default_statement_config
from sqlspec.adapters.spanner.driver import SpannerSyncDriver


def test_driver_execute_partitioned_dml() -> None:
    """Verify that driver.execute_partitioned_dml delegates to database.execute_partitioned_dml."""
    mock_db = MagicMock()
    mock_db.execute_partitioned_dml.return_value = 42

    mock_connection = MagicMock()
    mock_connection._session._database = mock_db

    driver = SpannerSyncDriver(
        connection=mock_connection, statement_config=default_statement_config, driver_features={}
    )

    rows = driver.execute_partitioned_dml("DELETE FROM large_table WHERE active = FALSE")
    assert rows == 42
    mock_db.execute_partitioned_dml.assert_called_once()
    sql = mock_db.execute_partitioned_dml.call_args[0][0]
    assert "DELETE FROM large_table WHERE active = FALSE" in sql


def test_driver_execute_partitioned_dml_with_parameters() -> None:
    """Verify parameters and types are coerced and passed to execute_partitioned_dml."""
    mock_db = MagicMock()
    mock_db.execute_partitioned_dml.return_value = 10

    mock_connection = MagicMock()
    mock_connection._session._database = mock_db

    driver = SpannerSyncDriver(
        connection=mock_connection, statement_config=default_statement_config, driver_features={}
    )

    rows = driver.execute_partitioned_dml(
        "UPDATE large_table SET status = :status WHERE threshold > :limit", {"status": "archived", "limit": 100}
    )
    assert rows == 10
    mock_db.execute_partitioned_dml.assert_called_once()
    _, kwargs = mock_db.execute_partitioned_dml.call_args
    assert kwargs["params"] == {"status": "archived", "limit": 100}
    assert "status" in kwargs["param_types"]
    assert kwargs["param_types"]["status"].code == TypeCode.STRING
    assert "limit" in kwargs["param_types"]
    assert kwargs["param_types"]["limit"].code == TypeCode.INT64


def test_config_execute_partitioned_dml() -> None:
    """Verify that config.execute_partitioned_dml delegates to get_database().execute_partitioned_dml."""
    config = SpannerSyncConfig(connection_config={"project": "p", "instance_id": "i", "database_id": "d"})
    mock_db = MagicMock()
    mock_db.execute_partitioned_dml.return_value = 100

    with patch.object(config, "get_database", return_value=mock_db):
        rows = config.execute_partitioned_dml("DELETE FROM large_table WHERE TRUE")

    assert rows == 100
    mock_db.execute_partitioned_dml.assert_called_once()
    sql = mock_db.execute_partitioned_dml.call_args[0][0]
    assert "DELETE FROM large_table WHERE TRUE" in sql


def test_driver_execute_partitioned_dml_with_sql_object_and_options() -> None:
    """Verify executing partitioned DML with SQL object, query options, and request options."""
    from sqlspec.core import SQL

    mock_db = MagicMock()
    mock_db.execute_partitioned_dml.return_value = 50

    mock_connection = MagicMock()
    mock_connection._session._database = mock_db

    driver = SpannerSyncDriver(
        connection=mock_connection, statement_config=default_statement_config, driver_features={}
    )

    statement = SQL("DELETE FROM large_table WHERE expired = TRUE", statement_config=default_statement_config)
    mock_query_options = MagicMock()
    mock_request_options = MagicMock()

    rows = driver.execute_partitioned_dml(
        statement,
        query_options=mock_query_options,
        request_options=mock_request_options,
        exclude_txn_from_change_streams=True,
    )
    assert rows == 50
    mock_db.execute_partitioned_dml.assert_called_once()
    _, kwargs = mock_db.execute_partitioned_dml.call_args
    assert kwargs["query_options"] is mock_query_options
    assert kwargs["request_options"] is mock_request_options
    assert kwargs["exclude_txn_from_change_streams"] is True


def test_driver_execute_partitioned_dml_no_database_raises() -> None:
    """Verify error raised when database cannot be resolved."""
    import pytest

    from sqlspec.exceptions import SQLConversionError

    mock_connection = MagicMock()
    mock_connection._session = None
    mock_connection._database = None

    driver = SpannerSyncDriver(
        connection=mock_connection, statement_config=default_statement_config, driver_features={}
    )

    with pytest.raises(SQLConversionError, match="Could not resolve Spanner database"):
        driver.execute_partitioned_dml("DELETE FROM large_table WHERE TRUE")


def test_config_execute_partitioned_dml_with_parameters_and_options() -> None:
    """Verify config partitioned DML forwards parameters, types, and options."""
    config = SpannerSyncConfig(connection_config={"project": "p", "instance_id": "i", "database_id": "d"})
    mock_db = MagicMock()
    mock_db.execute_partitioned_dml.return_value = 25

    mock_query_options = MagicMock()
    mock_request_options = MagicMock()

    with patch.object(config, "get_database", return_value=mock_db):
        rows = config.execute_partitioned_dml(
            "UPDATE items SET status = :status WHERE id = :id",
            {"status": "deleted", "id": 5},
            query_options=mock_query_options,
            request_options=mock_request_options,
            exclude_txn_from_change_streams=True,
        )

    assert rows == 25
    mock_db.execute_partitioned_dml.assert_called_once()
    _, kwargs = mock_db.execute_partitioned_dml.call_args
    assert kwargs["params"] == {"status": "deleted", "id": 5}
    assert "status" in kwargs["param_types"]
    assert kwargs["query_options"] is mock_query_options
    assert kwargs["request_options"] is mock_request_options
    assert kwargs["exclude_txn_from_change_streams"] is True
