"""Unit tests for SpannerAsyncDriver, SpannerAsyncExceptionHandler, and SpannerAsyncStreamSource."""

from collections.abc import AsyncGenerator
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pytest
from google.api_core import exceptions as api_exceptions
from google.cloud.spanner_v1.data_types import JsonObject
from google.cloud.spanner_v1.types.type import TypeCode
from typing_extensions import Self

import sqlspec.adapters.spanner.driver as spanner_driver_module
from sqlspec.adapters.spanner.core import SpannerAsyncStreamSource, default_statement_config, resolve_row_plan
from sqlspec.adapters.spanner.driver import SpannerAsyncDriver, SpannerAsyncExceptionHandler
from sqlspec.driver import AsyncRowStream
from sqlspec.exceptions import DeadlockError, SQLConversionError, UniqueViolationError
from sqlspec.utils.serializers import from_json

_ARROW_CAPABILITIES = {
    "arrow_export_enabled": True,
    "arrow_import_enabled": True,
    "parquet_export_enabled": True,
    "parquet_import_enabled": True,
    "partition_strategies": ["fixed"],
}


def _field(name: str, code: int) -> SimpleNamespace:
    return SimpleNamespace(name=name, type_=SimpleNamespace(code=code))


class _FakeAsyncResultSet:
    def __init__(self, rows: list[tuple[Any, ...]], fields: list[Any]) -> None:
        self._rows = rows
        self._fields = fields
        self.metadata: Any = None
        self.closed = False

    def to_dict_list(self) -> list[dict[str, Any]]:
        msg = "SpannerAsyncDriver must not call blocking to_dict_list() on AsyncStreamedResultSet"
        raise AssertionError(msg)

    async def __aiter__(self) -> "AsyncGenerator[tuple[Any, ...], None]":
        try:
            for index, row in enumerate(self._rows):
                if index == 0:
                    self.metadata = SimpleNamespace(row_type=SimpleNamespace(fields=self._fields))
                yield row
        finally:
            self.closed = True


async def test_spanner_async_exception_handler_maps_google_api_errors() -> None:
    """Verify SpannerAsyncExceptionHandler maps GoogleAPICallError subclasses to SQLSpecError."""
    handler = SpannerAsyncExceptionHandler()
    async with handler:
        raise cast("Any", api_exceptions.Aborted)("Transaction was aborted.")

    assert isinstance(handler.pending_exception, DeadlockError)
    assert isinstance(handler.pending_exception.__cause__, api_exceptions.Aborted)

    handler2 = SpannerAsyncExceptionHandler()
    async with handler2:
        raise cast("Any", api_exceptions.AlreadyExists)("Row already exists.")

    assert isinstance(handler2.pending_exception, UniqueViolationError)


async def test_spanner_async_select_stream_source_chunks_and_resolves_metadata() -> None:
    """Verify SpannerAsyncStreamSource streams chunks via AsyncRowStream and resolves metadata lazily."""
    json_cls = cast("Any", JsonObject)
    fields = [_field("id", TypeCode.INT64), _field("payload", TypeCode.JSON)]
    fake_rs = _FakeAsyncResultSet(
        rows=[(1, json_cls({"k": "v1"})), (2, json_cls({"k": "v2"})), (3, json_cls({"k": "v3"}))], fields=fields
    )

    class _FakeConnection:
        def __init__(self) -> None:
            self.calls: list[tuple[str, Any, Any, dict[str, Any]]] = []

        async def execute_sql(
            self,
            sql: str,
            params: dict[str, Any] | None = None,
            param_types: dict[str, Any] | None = None,
            **kwargs: Any,
        ) -> _FakeAsyncResultSet:
            self.calls.append((sql, params, param_types, kwargs))
            return fake_rs

    conn = _FakeConnection()

    class _FakeDriver:
        def __init__(self) -> None:
            self.connection = conn

        def handle_database_exceptions(self) -> SpannerAsyncExceptionHandler:
            return SpannerAsyncExceptionHandler()

        def _check_pending_exception(self, exc_handler: SpannerAsyncExceptionHandler) -> None:
            if exc_handler.pending_exception is not None:
                raise exc_handler.pending_exception

        def _resolve_row_plan(self, result_fields: Any) -> tuple[list[str], tuple[tuple[int, Any], ...] | None]:
            return resolve_row_plan(result_fields, {}, json_deserializer=from_json)

    source = SpannerAsyncStreamSource(
        cast("Any", _FakeDriver()), "SELECT id, payload FROM items", {"p": 1}, {}, 2, {"timeout": 5.0}
    )
    stream: AsyncRowStream[dict[str, Any]] = AsyncRowStream(source)
    async with stream as active_stream:
        collected = [item async for item in active_stream]

    assert collected == [
        {"id": 1, "payload": {"k": "v1"}},
        {"id": 2, "payload": {"k": "v2"}},
        {"id": 3, "payload": {"k": "v3"}},
    ]
    assert fake_rs.closed is True


async def test_async_driver_dispatch_execute_select() -> None:
    """Verify SpannerAsyncDriver executes SELECT queries via async iteration and resolves metadata."""
    json_cls = cast("Any", JsonObject)
    fields = [_field("id", TypeCode.INT64), _field("meta", TypeCode.JSON)]
    fake_rs = _FakeAsyncResultSet(rows=[(10, json_cls({"ok": True}))], fields=fields)

    mock_conn = MagicMock()
    mock_conn.execute_sql = AsyncMock(return_value=fake_rs)

    driver = SpannerAsyncDriver(connection=mock_conn, statement_config=default_statement_config)
    query_opts = {"optimizer_version": "6"}
    result = await driver.execute("SELECT id, meta FROM users WHERE id = @id", {"id": 10}, query_options=query_opts)

    assert result.all() == [{"id": 10, "meta": {"ok": True}}]
    mock_conn.execute_sql.assert_awaited_once()
    _, kwargs = mock_conn.execute_sql.call_args
    assert kwargs.get("query_options") == query_opts


async def test_async_driver_dispatch_execute_dml_and_last_statement() -> None:
    """Verify SpannerAsyncDriver executes DML via await execute_update and forwards last_statement."""
    mock_conn = MagicMock()
    mock_conn.execute_update = AsyncMock(return_value=2)
    mock_conn.committed = None

    driver = SpannerAsyncDriver(connection=mock_conn)
    result = await driver.execute("UPDATE users SET active = TRUE WHERE id = @id", {"id": 1}, last_statement=True)

    assert result.rows_affected == 2
    mock_conn.execute_update.assert_awaited_once()
    _, kwargs = mock_conn.execute_update.call_args
    assert kwargs.get("last_statement") is True


async def test_async_driver_dispatch_execute_dml_on_read_only_snapshot_raises() -> None:
    """Verify SpannerAsyncDriver raises SQLConversionError on DML when cursor lacks execute_update."""
    snapshot = SimpleNamespace(execute_sql=AsyncMock())
    driver = SpannerAsyncDriver(connection=cast("Any", snapshot))

    with pytest.raises(SQLConversionError, match="Cannot execute DML in a read-only Snapshot context"):
        await driver.execute("DELETE FROM users WHERE id = 1")


async def test_async_driver_dispatch_execute_many() -> None:
    """Verify SpannerAsyncDriver.execute_many invokes await batch_update and omits query_options."""
    mock_conn = MagicMock()
    mock_conn.batch_update = AsyncMock(return_value=(None, [1, 1]))

    driver = SpannerAsyncDriver(
        connection=mock_conn,
        statement_config=default_statement_config,
        driver_features={"query_options": {"optimizer_version": "latest"}},
    )
    result = await driver.execute_many(
        "INSERT INTO users (id, name) VALUES (:id, :name)",
        [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
        query_options={"optimizer_version": "6"},
    )

    assert result.rows_affected == 2
    mock_conn.batch_update.assert_awaited_once()
    _, kwargs = mock_conn.batch_update.call_args
    assert "query_options" not in kwargs


async def test_async_driver_dispatch_execute_script_marks_only_final_dml_as_last_statement() -> None:
    """Verify SpannerAsyncDriver.execute_script forwards last_statement only to the final DML statement."""
    mock_conn = MagicMock()
    mock_conn.execute_update = AsyncMock(return_value=1)

    driver = SpannerAsyncDriver(connection=mock_conn)
    result = await driver.execute_script("UPDATE t SET x = 1; UPDATE t SET x = 2", last_statement=True)

    assert result.total_statements == 2
    calls = mock_conn.execute_update.await_args_list
    assert len(calls) == 2
    assert "last_statement" not in calls[0].kwargs
    assert calls[1].kwargs["last_statement"] is True


async def test_async_driver_select_stream_and_select_to_arrow() -> None:
    """Verify SpannerAsyncDriver.select_stream and select_to_arrow work asynchronously."""
    fields = [_field("id", TypeCode.INT64), _field("name", TypeCode.STRING)]

    mock_conn = MagicMock()
    mock_conn.execute_sql = AsyncMock(
        side_effect=lambda *args, **kwargs: _FakeAsyncResultSet(rows=[(1, "alice"), (2, "bob")], fields=fields)
    )

    driver = SpannerAsyncDriver(connection=mock_conn)
    stream = driver.select_stream("SELECT id, name FROM users", chunk_size=1, query_options={"optimizer_version": "6"})
    async with stream as active_stream:
        streamed_rows = [row async for row in active_stream]

    assert streamed_rows == [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]

    arrow_result = await driver.select_to_arrow("SELECT id, name FROM users")
    assert arrow_result.to_dict() == [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]


async def test_async_driver_commit_rollback_and_savepoints(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify SpannerAsyncDriver commit and rollback renew the session transaction, and savepoints raise."""

    class _FakeAsyncTxn:
        def __init__(self, session: Any, *, begun: bool) -> None:
            self._session = session
            self.committed: Any = None
            self.rolled_back = False
            self._transaction_id = b"txn" if begun else None
            self._mutations: list[Any] = []
            self.commit_calls = 0
            self.rollback_calls = 0

        async def commit(self) -> None:
            self.commit_calls += 1
            self.committed = True

        async def rollback(self) -> None:
            self.rollback_calls += 1
            self.rolled_back = True

    session = SimpleNamespace()
    session.transaction = lambda: _FakeAsyncTxn(session, begun=False)
    monkeypatch.setattr(spanner_driver_module, "SpannerAsyncTransaction", _FakeAsyncTxn)
    txn = _FakeAsyncTxn(session, begun=True)
    driver = SpannerAsyncDriver(connection=cast("Any", txn))

    await driver.begin()
    await driver.commit()
    renewed = cast("Any", driver.connection)
    assert txn.commit_calls == 1
    assert renewed is not txn
    assert renewed._transaction_id is None

    await driver.commit()
    assert renewed.commit_calls == 0
    assert driver.connection is renewed

    renewed._transaction_id = b"txn-2"
    await driver.rollback()
    assert renewed.rollback_calls == 1
    assert driver.connection is not renewed

    for method in ("create_savepoint", "release_savepoint", "rollback_to_savepoint"):
        with pytest.raises(NotImplementedError, match="Spanner"):
            await getattr(driver, method)("sp1")


async def test_async_driver_load_from_arrow_transactional_and_overwrite(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify SpannerAsyncDriver.load_from_arrow mutations and overwrite=True delete-then-mutate."""

    class _FakeAsyncMutationTxn:
        def __init__(self) -> None:
            self.insert_or_update_calls: list[tuple[str, list[str], list[list[Any]]]] = []
            self.execute_update_calls: list[str] = []

        def insert_or_update(self, table: str, columns: Any, values: Any) -> None:
            self.insert_or_update_calls.append((table, list(columns), [list(v) for v in values]))

        async def execute_update(self, sql: str, params: Any = None, param_types: Any = None, **kwargs: Any) -> int:
            del params, param_types, kwargs
            self.execute_update_calls.append(sql)
            return 0

    monkeypatch.setattr(spanner_driver_module, "SpannerAsyncTransaction", _FakeAsyncMutationTxn)
    txn = _FakeAsyncMutationTxn()
    driver = SpannerAsyncDriver(
        connection=cast("Any", txn), driver_features={"storage_capabilities": _ARROW_CAPABILITIES}
    )

    arrow_table = pa.table({"id": [1, 2], "name": ["a", "b"]})
    job = await driver.load_from_arrow("my_schema.users", arrow_table, overwrite=True)

    assert job.telemetry["rows_processed"] == 2
    assert txn.execute_update_calls == ["DELETE FROM `my_schema`.`users` WHERE TRUE"]
    assert txn.insert_or_update_calls == [("my_schema.users", ["id", "name"], [[1, "a"], [2, "b"]])]


async def test_async_driver_load_from_arrow_batch_write_api() -> None:
    """Verify SpannerAsyncDriver.load_from_arrow with enable_batch_write_api=True uses async mutation_groups."""

    class _FakeGroup:
        def __init__(self) -> None:
            self.calls: list[tuple[str, list[str], list[list[Any]]]] = []

        def insert_or_update(self, table: str, columns: Any, values: Any) -> None:
            self.calls.append((table, list(columns), [list(v) for v in values]))

    class _FakeAsyncMutationGroups:
        def __init__(self) -> None:
            self.groups: list[_FakeGroup] = []
            self.batch_write_calls = 0
            self.entered = False
            self.exited = False

        async def __aenter__(self) -> Self:
            self.entered = True
            return self

        async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
            self.exited = True

        def group(self) -> _FakeGroup:
            group = _FakeGroup()
            self.groups.append(group)
            return group

        async def batch_write(self) -> Any:
            self.batch_write_calls += 1

            async def _gen() -> Any:
                yield SimpleNamespace(status=SimpleNamespace(code=0, message="OK"))

            return _gen()

    class _FakeAsyncDatabase:
        def __init__(self) -> None:
            self.mutation_groups_obj = _FakeAsyncMutationGroups()

        def mutation_groups(self) -> _FakeAsyncMutationGroups:
            return self.mutation_groups_obj

    database = _FakeAsyncDatabase()
    snapshot = SimpleNamespace(_session=SimpleNamespace(_database=database))
    driver = SpannerAsyncDriver(
        connection=cast("Any", snapshot),
        driver_features={"storage_capabilities": _ARROW_CAPABILITIES, "enable_batch_write_api": True},
    )

    job = await driver.load_from_arrow("users", pa.table({"id": [1, 2], "name": ["a", "b"]}))
    assert job.telemetry["rows_processed"] == 2
    assert database.mutation_groups_obj.entered is True
    assert database.mutation_groups_obj.exited is True
    assert database.mutation_groups_obj.batch_write_calls == 1
    assert database.mutation_groups_obj.groups[0].calls == [("users", ["id", "name"], [[1, "a"], [2, "b"]])]


async def test_async_driver_select_stream_handles_empty_result_set() -> None:
    """Verify SpannerAsyncDriver.select_stream handles an empty AsyncStreamedResultSet cleanly."""
    fake_rs = _FakeAsyncResultSet(rows=[], fields=[_field("id", TypeCode.INT64)])
    mock_conn = MagicMock()
    mock_conn.execute_sql = AsyncMock(return_value=fake_rs)

    driver = SpannerAsyncDriver(connection=mock_conn)
    async with driver.select_stream("SELECT id FROM users WHERE FALSE") as active_stream:
        rows = [row async for row in active_stream]

    assert rows == []
    assert fake_rs.closed is True


async def test_async_driver_load_from_arrow_empty_and_multi_chunk(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify SpannerAsyncDriver.load_from_arrow handles empty tables and splits chunks exceeding 80,000 cells."""

    class _FakeAsyncMutationTxn:
        def __init__(self) -> None:
            self.insert_or_update_calls: list[tuple[str, list[str], int]] = []

        def insert_or_update(self, table: str, columns: Any, values: Any) -> None:
            self.insert_or_update_calls.append((table, list(columns), len(values)))

    monkeypatch.setattr(spanner_driver_module, "SpannerAsyncTransaction", _FakeAsyncMutationTxn)
    txn = _FakeAsyncMutationTxn()
    driver = SpannerAsyncDriver(
        connection=cast("Any", txn), driver_features={"storage_capabilities": _ARROW_CAPABILITIES}
    )

    empty_table = pa.table({"id": pa.array([], type=pa.int64()), "name": pa.array([], type=pa.string())})
    empty_job = await driver.load_from_arrow("users", empty_table)
    assert empty_job.telemetry["rows_processed"] == 0
    assert txn.insert_or_update_calls == []

    monkeypatch.setattr("sqlspec.adapters.spanner.core._MAX_MUTATIONS_PER_COMMIT", 4)
    multi_table = pa.table({"id": [1, 2, 3, 4, 5], "name": ["a", "b", "c", "d", "e"]})
    multi_job = await driver.load_from_arrow("users", multi_table)
    assert multi_job.telemetry["rows_processed"] == 5
    assert [count for _, _, count in txn.insert_or_update_calls] == [2, 2, 1]
