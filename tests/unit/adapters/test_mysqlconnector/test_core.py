"""mysql-connector compiled core helpers."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.mysqlconnector.core import (
    MysqlConnectorAsyncStreamSource,
    MysqlConnectorSyncStreamSource,
    collect_stream_rows,
)
from sqlspec.utils.serializers import from_json


def test_collect_stream_rows_preserves_dict_values_and_decodes_json() -> None:
    """Dict rows from a dictionary=True cursor keep their values and still decode JSON."""
    rows = [{"id": 1, "payload": '{"name": "alpha"}'}, {"id": 2, "payload": '{"name": "beta"}'}]

    collected = collect_stream_rows(rows, (["id", "payload"], [1]), from_json)

    assert collected == [{"id": 1, "payload": {"name": "alpha"}}, {"id": 2, "payload": {"name": "beta"}}]


def test_collect_stream_rows_still_zips_tuple_rows() -> None:
    """Tuple rows from a plain cursor keep the positional zip path."""
    collected = collect_stream_rows([(1, '{"name": "alpha"}')], (["id", "payload"], [1]), from_json)

    assert collected == [{"id": 1, "payload": {"name": "alpha"}}]


def test_sync_stream_source_closes_cursor_when_execute_raises() -> None:
    """MysqlConnectorSyncStreamSource.start() closes the cursor if execute() fails."""
    cursor = MagicMock()
    cursor.execute.side_effect = RuntimeError("execute boom")
    connection = MagicMock()
    connection.cursor.return_value = cursor
    driver = MagicMock(connection=connection, driver_features={})
    source = MysqlConnectorSyncStreamSource(driver, "SELECT 1", (), 100, set())

    with pytest.raises(RuntimeError, match="execute boom"):
        source.start()

    cursor.close.assert_called_once()
    assert source._cursor is None


async def test_async_stream_source_closes_cursor_when_execute_raises() -> None:
    """MysqlConnectorAsyncStreamSource.start() closes the cursor if execute() fails."""
    cursor = MagicMock()
    cursor.execute = AsyncMock(side_effect=RuntimeError("execute boom"))
    cursor.close = AsyncMock()
    connection = MagicMock(_cnx=None, unread_result=False)
    connection.cursor = AsyncMock(return_value=cursor)
    driver = MagicMock(connection=connection, driver_features={})
    driver._run_with_exception_handler = lambda _handler, fn: fn()
    source = MysqlConnectorAsyncStreamSource(driver, "SELECT 1", (), 100, set())

    with pytest.raises(RuntimeError, match="execute boom"):
        await source.start()

    cursor.close.assert_awaited_once()
    assert source._cursor is None
