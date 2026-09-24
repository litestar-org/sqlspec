"""Unit tests for Spanner QueryOptions forwarding."""

from unittest.mock import MagicMock

from sqlspec.adapters.spanner.config import SpannerSyncConfig
from sqlspec.adapters.spanner.core import default_statement_config
from sqlspec.adapters.spanner.driver import SpannerSyncDriver


def test_driver_execute_with_statement_query_options() -> None:
    """Verify driver.execute passes query_options to execute_sql."""
    mock_cursor = MagicMock()
    mock_result_set = MagicMock()
    mock_field = MagicMock()
    mock_field.name = "v"
    mock_field.type_.code = 1
    mock_result_set.metadata.row_type.fields = [mock_field]
    mock_result_set.__iter__.return_value = [[1]]
    mock_cursor.execute_sql.return_value = mock_result_set

    driver = SpannerSyncDriver(connection=mock_cursor, statement_config=default_statement_config, driver_features={})

    query_opts = {"optimizer_version": "6", "optimizer_statistics_package": "latest"}
    driver.execute("SELECT 1", query_options=query_opts)

    mock_cursor.execute_sql.assert_called_once()
    _, kwargs = mock_cursor.execute_sql.call_args
    assert kwargs.get("query_options") == query_opts


def test_driver_feature_query_options() -> None:
    """Verify driver-level query_options are passed to execute_sql."""
    mock_cursor = MagicMock()
    mock_result_set = MagicMock()
    mock_field = MagicMock()
    mock_field.name = "v"
    mock_field.type_.code = 1
    mock_result_set.metadata.row_type.fields = [mock_field]
    mock_result_set.__iter__.return_value = [[1]]
    mock_cursor.execute_sql.return_value = mock_result_set

    query_opts = {"optimizer_version": "latest"}
    driver = SpannerSyncDriver(
        connection=mock_cursor, statement_config=default_statement_config, driver_features={"query_options": query_opts}
    )

    driver.execute("SELECT 1")

    mock_cursor.execute_sql.assert_called_once()
    _, kwargs = mock_cursor.execute_sql.call_args
    assert kwargs.get("query_options") == query_opts


def test_config_provide_session_query_options() -> None:
    """Verify config.provide_session forwards query_options to driver_features."""
    config = SpannerSyncConfig(connection_config={"project": "p", "instance_id": "i", "database_id": "d"})
    query_opts = {"optimizer_version": "5"}
    features = config._session_driver_features(
        request_options=None, directed_read_options=None, query_options=query_opts, retry=None, timeout=None
    )
    assert features["query_options"] == query_opts


def test_driver_execute_many_omits_query_options() -> None:
    """Verify execute_many does not forward query_options to batch_update."""
    mock_cursor = MagicMock()
    mock_cursor.batch_update.return_value = (None, [1, 1])

    query_opts = {"optimizer_version": "latest"}
    driver = SpannerSyncDriver(
        connection=mock_cursor, statement_config=default_statement_config, driver_features={"query_options": query_opts}
    )

    driver.execute_many("INSERT INTO t (id) VALUES (:id)", [{"id": 1}, {"id": 2}], query_options=query_opts)

    mock_cursor.batch_update.assert_called_once()
    _, kwargs = mock_cursor.batch_update.call_args
    assert "query_options" not in kwargs


def test_driver_select_stream_query_options() -> None:
    """Verify select_stream passes query_options to execute_sql."""
    mock_cursor = MagicMock()
    mock_result_set = MagicMock()
    mock_field = MagicMock()
    mock_field.name = "v"
    mock_field.type_.code = 1
    mock_result_set.metadata.row_type.fields = [mock_field]
    mock_result_set.__iter__.return_value = [[1], [2]]
    mock_cursor.execute_sql.return_value = mock_result_set

    query_opts = {"optimizer_version": "6"}
    driver = SpannerSyncDriver(connection=mock_cursor, statement_config=default_statement_config, driver_features={})

    stream = driver.select_stream("SELECT 1", query_options=query_opts)
    rows = list(stream) if stream else []
    assert len(rows) == 2

    mock_cursor.execute_sql.assert_called_once()
    _, kwargs = mock_cursor.execute_sql.call_args
    assert kwargs.get("query_options") == query_opts
