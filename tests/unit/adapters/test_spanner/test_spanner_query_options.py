"""Unit tests for Spanner QueryOptions forwarding."""

from collections.abc import AsyncIterator, Iterator
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from google.api_core import exceptions as api_exceptions
from google.cloud.spanner_v1.types.type import TypeCode

from sqlspec.adapters.spanner.config import SpannerSyncConfig
from sqlspec.adapters.spanner.core import default_statement_config
from sqlspec.adapters.spanner.driver import SpannerAsyncDriver, SpannerSyncDriver
from sqlspec.exceptions import OperationalError


class _StreamResult:
    def __init__(self, error: BaseException | None) -> None:
        self.error = error
        self.closed = False
        self.metadata = SimpleNamespace(
            row_type=SimpleNamespace(fields=[SimpleNamespace(name="id", type_=SimpleNamespace(code=TypeCode.INT64))])
        )

    def __iter__(self) -> Iterator[list[int]]:
        try:
            yield [1]
            if self.error is not None:
                raise self.error
            yield [2]
        finally:
            self.closed = True

    async def __aiter__(self) -> AsyncIterator[list[int]]:
        try:
            yield [1]
            if self.error is not None:
                raise self.error
            yield [2]
        finally:
            self.closed = True


async def _consume_stream(driver: Any, is_async: bool, *, fails_fetch: bool = False) -> list[dict[str, Any]]:
    stream = driver.select_stream("SELECT id FROM users", chunk_size=1)
    if is_async:
        async with stream:
            if fails_fetch:
                assert await anext(stream) == {"id": 1}
            return [row async for row in stream]
    with stream:
        if fails_fetch:
            assert next(stream) == {"id": 1}
        return list(stream)


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("phase", ["start", "fetch"])
@pytest.mark.parametrize("ambient", [False, True], ids=["normal", "application-except"])
async def test_stream_maps_sdk_errors_with_ambient_application_exception(
    is_async: bool, phase: str, ambient: bool
) -> None:
    error = api_exceptions.ServiceUnavailable("Spanner unavailable")
    result = _StreamResult(error)
    execute = AsyncMock() if is_async else MagicMock()
    execute.side_effect = error if phase == "start" else None
    execute.return_value = result
    connection = SimpleNamespace(execute_sql=execute)
    driver = SpannerAsyncDriver(connection=connection) if is_async else SpannerSyncDriver(connection=connection)

    with pytest.raises(OperationalError, match="Spanner unavailable") as caught:
        try:
            if ambient:
                raise LookupError("application error")
        except LookupError:
            await _consume_stream(driver, is_async, fails_fetch=phase == "fetch")
        else:
            await _consume_stream(driver, is_async, fails_fetch=phase == "fetch")

    assert caught.value.__cause__ is error
    execute.assert_called_once()
    if phase == "fetch":
        assert result.closed is True


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_stream_succeeds_with_ambient_application_exception(is_async: bool) -> None:
    result = _StreamResult(None)
    execute = AsyncMock(return_value=result) if is_async else MagicMock(return_value=result)
    connection = SimpleNamespace(execute_sql=execute)
    driver = SpannerAsyncDriver(connection=connection) if is_async else SpannerSyncDriver(connection=connection)
    error = LookupError("application error")

    try:
        raise error
    except LookupError as caught:
        rows = await _consume_stream(driver, is_async)
        assert caught is error

    assert rows == [{"id": 1}, {"id": 2}]
    assert result.closed is True


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
    context = config.provide_session(query_options=query_opts)
    assert context._driver_features["query_options"] == query_opts  # pyright: ignore[reportPrivateUsage]
    assert "query_options" not in config.driver_features


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


def test_driver_dml_retains_query_options() -> None:
    cursor = MagicMock()
    cursor.execute_update.return_value = 1
    driver = SpannerSyncDriver(connection=cursor)
    driver.execute("UPDATE t SET x = 1", query_options={"optimizer_version": "6"})
    assert cursor.execute_update.call_args.kwargs["query_options"] == {"optimizer_version": "6"}
