"""Unit tests for Spanner RequestOptions and DirectedReadOptions propagation."""

from unittest.mock import MagicMock

from sqlspec.adapters.spanner.core import default_statement_config
from sqlspec.adapters.spanner.driver import SpannerSyncDriver


def test_execute_select_forwards_request_and_directed_read_options() -> None:
    """Verify execute for SELECT forwards request_options and directed_read_options."""
    mock_cursor = MagicMock()
    mock_result_set = MagicMock()
    mock_field = MagicMock()
    mock_field.name = "v"
    mock_field.type_.code = 1
    mock_result_set.metadata.row_type.fields = [mock_field]
    mock_result_set.__iter__.return_value = [[1]]
    mock_cursor.execute_sql.return_value = mock_result_set

    driver = SpannerSyncDriver(connection=mock_cursor, statement_config=default_statement_config, driver_features={})

    req_opts = {"request_tag": "select-tag", "priority": 1}
    directed_read = MagicMock()
    driver.execute("SELECT 1", request_options=req_opts, directed_read_options=directed_read)

    mock_cursor.execute_sql.assert_called_once()
    _, kwargs = mock_cursor.execute_sql.call_args
    assert kwargs.get("request_options") == req_opts
    assert kwargs.get("directed_read_options") is directed_read


def test_execute_update_omits_directed_read_options() -> None:
    """Verify execute for UPDATE forwards request_options but strips directed_read_options."""
    mock_cursor = MagicMock()
    mock_cursor.execute_update.return_value = 1

    driver = SpannerSyncDriver(
        connection=mock_cursor,
        statement_config=default_statement_config,
        driver_features={"directed_read_options": MagicMock()},
    )

    req_opts = {"request_tag": "update-tag", "transaction_tag": "tx-tag"}
    per_call_directed = MagicMock()
    driver.execute("UPDATE t SET x = 1", request_options=req_opts, directed_read_options=per_call_directed)

    mock_cursor.execute_update.assert_called_once()
    _, kwargs = mock_cursor.execute_update.call_args
    assert kwargs.get("request_options") == req_opts
    assert "directed_read_options" not in kwargs


def test_execute_many_forwards_request_options_and_omits_read_options() -> None:
    """Verify execute_many forwards request_options and omits directed_read_options and query_options."""
    mock_cursor = MagicMock()
    mock_cursor.batch_update.return_value = (None, [1, 1])

    driver = SpannerSyncDriver(
        connection=mock_cursor,
        statement_config=default_statement_config,
        driver_features={"query_options": {"optimizer_version": "6"}, "directed_read_options": MagicMock()},
    )

    req_opts = {"request_tag": "batch-tag", "priority": 2}
    driver.execute_many(
        "INSERT INTO t (id) VALUES (:id)",
        [{"id": 1}, {"id": 2}],
        request_options=req_opts,
        directed_read_options=MagicMock(),
        query_options={"optimizer_version": "5"},
    )

    mock_cursor.batch_update.assert_called_once()
    _, kwargs = mock_cursor.batch_update.call_args
    assert kwargs.get("request_options") == req_opts
    assert "directed_read_options" not in kwargs
    assert "query_options" not in kwargs


def test_execute_script_forwards_options_appropriately() -> None:
    """Verify execute_script separates read and write options across script statements."""
    mock_cursor = MagicMock()
    mock_result_set = MagicMock()
    mock_cursor.execute_sql.return_value = mock_result_set
    mock_cursor.execute_update.return_value = 1

    req_opts = {"request_tag": "script-tag"}
    directed_read = MagicMock()
    driver = SpannerSyncDriver(
        connection=mock_cursor,
        statement_config=default_statement_config,
        driver_features={"request_options": req_opts, "directed_read_options": directed_read},
    )

    driver.execute_script("SELECT 1; UPDATE t SET x = 1;")

    mock_cursor.execute_sql.assert_called_once()
    _, read_kwargs = mock_cursor.execute_sql.call_args
    assert read_kwargs.get("request_options") == req_opts
    assert read_kwargs.get("directed_read_options") is directed_read

    mock_cursor.execute_update.assert_called_once()
    _, write_kwargs = mock_cursor.execute_update.call_args
    assert write_kwargs.get("request_options") == req_opts
    assert "directed_read_options" not in write_kwargs
