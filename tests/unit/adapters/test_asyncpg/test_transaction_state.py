"""asyncpg transaction control uses the native transaction handle."""

import inspect
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest

from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.adapters.asyncpg.core import AsyncpgStreamSource, build_postgres_storage_params_sql
from sqlspec.adapters.asyncpg.driver import AsyncpgDriver
from sqlspec.adapters.asyncpg.events.backend import AsyncpgEventsBackend, AsyncpgHybridEventsBackend
from sqlspec.adapters.asyncpg.litestar.store import AsyncpgStore
from sqlspec.core import SQL, StatementConfig, StatementStack
from sqlspec.exceptions import DatabaseConnectionError, DeadlockError, QueryTimeoutError

pytestmark = pytest.mark.anyio


class _FakeTransaction:
    def __init__(self, record: "list[str]", nested: bool = False) -> None:
        self._record = record
        self.nested = nested

    async def start(self) -> None:
        self._record.append("savepoint" if self.nested else "start")

    async def commit(self) -> None:
        self._record.append("commit")

    async def rollback(self) -> None:
        self._record.append("rollback")


class _FakeConnection:
    """Mirrors asyncpg's refusal to mix manual SQL with transaction handles."""

    def __init__(self) -> None:
        self.statements: list[str] = []
        self.transaction_calls: list[str] = []
        self._depth = 0

    async def execute(self, sql: str, *_args: Any, **_kwargs: Any) -> str:
        self.statements.append(sql)
        return "SELECT 0"

    def transaction(self) -> _FakeTransaction:
        nested = self._depth > 0
        self._depth += 1
        return _FakeTransaction(self.transaction_calls, nested=nested)

    def is_in_transaction(self) -> bool:
        return self._depth > 0


async def test_begin_uses_the_native_transaction_handle() -> None:
    connection = _FakeConnection()
    driver = AsyncpgDriver(cast("Any", connection))

    await driver.begin()

    assert connection.transaction_calls == ["start"]
    assert connection.statements == []


async def test_a_second_begin_does_not_open_a_savepoint() -> None:
    connection = _FakeConnection()
    driver = AsyncpgDriver(cast("Any", connection))

    await driver.begin()
    await driver.begin()

    assert connection.transaction_calls == ["start"]


async def test_commit_after_a_repeated_begin_commits_the_outer_transaction() -> None:
    connection = _FakeConnection()
    driver = AsyncpgDriver(cast("Any", connection))

    await driver.begin()
    await driver.begin()
    await driver.commit()

    assert connection.transaction_calls == ["start", "commit"]
    assert connection.statements == []


async def test_rollback_releases_the_handle_so_a_later_begin_starts_fresh() -> None:
    connection = _FakeConnection()
    driver = AsyncpgDriver(cast("Any", connection))

    await driver.begin()
    await driver.rollback()
    connection._depth = 0
    await driver.begin()

    assert connection.transaction_calls == ["start", "rollback", "start"]


async def test_a_stream_can_open_a_transaction_after_driver_begin() -> None:
    """asyncpg refuses transaction() inside a manually started transaction."""
    connection = _FakeConnection()
    driver = AsyncpgDriver(cast("Any", connection))

    await driver.begin()

    assert connection.statements == []
    assert connection.is_in_transaction() is True


async def test_stream_source_reuses_outer_transaction_without_savepoint() -> None:
    """AsyncpgStreamSource should not start a nested savepoint when already in a transaction."""
    connection = _FakeConnection()
    connection.cursor = AsyncMock(return_value=MagicMock(fetch=AsyncMock(return_value=[])))  # type: ignore[attr-defined]
    driver = AsyncpgDriver(cast("Any", connection))

    await driver.begin()
    source = AsyncpgStreamSource(driver, "SELECT 1", (), 10)
    await source.start()
    await source.close()

    assert connection.transaction_calls == ["start"]


async def test_stream_source_suppresses_rollback_error_on_cursor_failure() -> None:
    """AsyncpgStreamSource should suppress rollback errors when cursor creation fails."""
    tx = MagicMock()
    tx.start = AsyncMock()
    tx.rollback = AsyncMock(side_effect=RuntimeError("connection lost"))
    conn = MagicMock()
    conn.transaction = MagicMock(return_value=tx)
    conn.is_in_transaction = MagicMock(return_value=False)
    conn.cursor = AsyncMock(side_effect=ValueError("cursor failed"))
    driver = AsyncpgDriver(cast("Any", conn))

    source = AsyncpgStreamSource(driver, "SELECT 1", (), 10)
    with pytest.raises(ValueError, match="cursor failed"):
        await source.start()
    assert source._transaction is None  # pyright: ignore[reportPrivateUsage]


async def test_begin_resets_stale_transaction_and_maps_exceptions() -> None:
    """begin() should reset stale _transaction when connection is not in transaction and map errors."""
    connection = _FakeConnection()
    driver = AsyncpgDriver(cast("Any", connection))
    await driver.begin()
    connection._depth = 0
    await driver.begin()
    assert connection.transaction_calls == ["start", "start"]

    failing_tx = MagicMock()
    failing_tx.start = AsyncMock(side_effect=asyncpg.exceptions.DeadlockDetectedError("deadlock"))
    failing_conn = MagicMock()
    failing_conn.transaction = MagicMock(return_value=failing_tx)
    failing_conn.is_in_transaction = MagicMock(return_value=False)
    failing_driver = AsyncpgDriver(cast("Any", failing_conn))
    with pytest.raises(DeadlockError):
        await failing_driver.begin()


async def test_commit_and_rollback_skip_raw_sql_when_not_in_transaction() -> None:
    """commit() and rollback() should not execute raw COMMIT/ROLLBACK when connection is not in transaction."""
    connection = _FakeConnection()
    driver = AsyncpgDriver(cast("Any", connection))

    await driver.commit()
    await driver.rollback()

    assert connection.statements == []


async def test_execute_stack_uses_cached_statement_without_manual_prepare() -> None:
    """execute_stack should use _execute_cached_statement and parse affected rows without connection.prepare."""
    conn = MagicMock()
    conn.is_in_transaction = MagicMock(return_value=True)
    conn.execute = AsyncMock(return_value="UPDATE 3")
    conn.prepare = AsyncMock()
    driver = AsyncpgDriver(cast("Any", conn))

    stack = StatementStack().push_execute("UPDATE items SET name = :name WHERE id = :id", {"name": "a", "id": 1})
    results = await driver.execute_stack(stack)

    assert len(results) == 1
    assert results[0].rows_affected == 3
    conn.prepare.assert_not_awaited()
    conn.execute.assert_awaited_once()


async def test_copy_operation_parses_quoted_table_with_dot_and_forwards_timeout() -> None:
    """COPY FROM STDIN should preserve quoted table names containing periods and forward timeouts."""
    conn = MagicMock()
    conn.copy_to_table = AsyncMock()
    driver = AsyncpgDriver(cast("Any", conn))

    config = StatementConfig(dialect="postgres", execution_args={"postgres_copy_data": "1\talpha\n", "timeout": 7.5})
    stmt = SQL('COPY "my.schema"."my.table" (id, name) FROM STDIN', statement_config=config)
    await driver._handle_copy_operation(cast("Any", conn), stmt)  # pyright: ignore[reportPrivateUsage]

    conn.copy_to_table.assert_awaited_once()
    call = conn.copy_to_table.await_args
    assert call.args == ("my.table",)
    assert call.kwargs["schema_name"] == "my.schema"
    assert call.kwargs["timeout"] == 7.5


def test_litestar_store_ddl_folds_autovacuum_into_create_table() -> None:
    """AsyncpgStore._table_ddl should include autovacuum settings inside WITH (...) without ALTER TABLE."""
    config = AsyncpgConfig(
        extension_config={
            "litestar": {
                "fillfactor": 75,
                "autovacuum_vacuum_scale_factor": 0.04,
                "autovacuum_analyze_scale_factor": 0.01,
            }
        }
    )
    store = AsyncpgStore(config)
    ddl = store._table_ddl()  # pyright: ignore[reportPrivateUsage]

    assert "ALTER TABLE" not in ddl
    assert "fillfactor = 75" in ddl
    assert "autovacuum_vacuum_scale_factor = 0.04" in ddl
    assert "autovacuum_analyze_scale_factor = 0.01" in ddl


async def test_events_backends_manage_transactions_correctly() -> None:
    """Hybrid _publish_durable wraps in begin/commit while native publish omits spurious commit."""
    calls: list[str] = []
    driver = MagicMock()
    driver.begin = AsyncMock(side_effect=lambda: calls.append("begin"))
    driver.execute = AsyncMock(side_effect=lambda *_: calls.append("execute"))
    driver.commit = AsyncMock(side_effect=lambda: calls.append("commit"))
    driver.rollback = AsyncMock(side_effect=lambda: calls.append("rollback"))

    @asynccontextmanager
    async def fake_session(*_a: Any, **_k: Any) -> AsyncIterator[Any]:
        yield driver

    config = MagicMock()
    config.is_async = True
    config.get_observability_runtime.return_value = MagicMock()
    config.provide_session = fake_session

    queue = MagicMock()
    queue._insert_statement = "INSERT INTO q VALUES (:event_id)"
    queue._statement_config = StatementConfig(dialect="postgres")

    hybrid = AsyncpgHybridEventsBackend(config, queue)
    await hybrid.publish("chan", {"x": 1})
    assert calls == ["begin", "execute", "execute", "commit"]

    calls.clear()
    native = AsyncpgEventsBackend(config)
    await native.publish("chan", {"x": 1})
    assert calls == ["execute"]


async def test_create_connection_consumes_no_pool_slot(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel = MagicMock()
    connect = AsyncMock(return_value=sentinel)
    monkeypatch.setattr("sqlspec.adapters.asyncpg.config.asyncpg_connect", connect)
    monkeypatch.setattr(AsyncpgConfig, "_init_connection", AsyncMock())
    config = AsyncpgConfig(connection_config={"host": "localhost", "min_size": 1, "max_size": 1})

    connection = await config.create_connection()

    assert connection is sentinel
    assert config.connection_instance is None
    assert "min_size" not in connect.call_args.kwargs
    assert "max_size" not in connect.call_args.kwargs


async def test_create_connection_drops_every_pool_only_setting(monkeypatch: pytest.MonkeyPatch) -> None:
    """asyncpg.connect() takes no **kwargs, so a pool-only setting reaching it is a TypeError."""
    connect = AsyncMock(return_value=MagicMock())
    monkeypatch.setattr("sqlspec.adapters.asyncpg.config.asyncpg_connect", connect)
    monkeypatch.setattr(AsyncpgConfig, "_init_connection", AsyncMock())
    config = AsyncpgConfig(
        connection_config={
            "host": "localhost",
            "min_size": 1,
            "max_size": 2,
            "reset": AsyncMock(),
            "setup": AsyncMock(),
        }
    )

    await config.create_connection()

    accepted = set(inspect.signature(asyncpg.connect).parameters)
    assert set(connect.call_args.kwargs) <= accepted


async def test_asyncpg_begin_rollback_on_set_local_failure() -> None:
    """A failure during SET LOCAL in begin() must rollback the native transaction and raise mapped error."""
    conn = MagicMock()
    tx = AsyncMock()
    conn.transaction.return_value = tx
    conn.execute = AsyncMock(side_effect=RuntimeError("columnar scan configuration error"))
    conn.is_in_transaction.return_value = False

    driver = AsyncpgDriver(
        cast("Any", conn), driver_features={"enable_alloydb_columnar": True, "pooler_mode": "transaction"}
    )
    with pytest.raises(Exception):
        await driver.begin()
    assert driver._transaction is None
    tx.rollback.assert_awaited_once()


async def test_asyncpg_dispatch_execute_script_with_parameters_and_observers() -> None:
    """dispatch_execute_script executes sub-statements, emits observer events, and returns cursor."""
    conn = MagicMock()
    conn.execute = AsyncMock(return_value="CREATE 1")
    driver = AsyncpgDriver(cast("Any", conn))

    stmt = SQL("CREATE TABLE t1 (id int); CREATE TABLE t2 (id int);")
    result = await driver.dispatch_execute_script(cast("Any", conn), stmt)

    assert result.is_script_result is True
    assert result.statement_count == 2
    assert result.successful_statements == 2
    assert result.cursor_result is conn


async def test_asyncpg_execute_stack_in_outer_transaction_does_not_commit_or_rollback() -> None:
    """execute_stack with continue_on_error inside an outer transaction must not commit or rollback."""
    conn = MagicMock()
    conn.is_in_transaction = MagicMock(return_value=True)
    conn.execute = AsyncMock(side_effect=["UPDATE 1", RuntimeError("failed op")])

    driver = AsyncpgDriver(cast("Any", conn))
    driver._transaction = MagicMock()

    stack = StatementStack().push_execute("UPDATE t SET x = 1").push_execute("UPDATE t SET x = 2")

    results = await driver.execute_stack(stack, continue_on_error=True)
    assert len(results) == 2
    assert results[0].result is not None
    assert results[1].error is not None
    assert driver._transaction is not None
    conn.execute.assert_called()


async def test_asyncpg_exception_mapping_timeout_and_interface_errors() -> None:
    """AsyncpgExceptionHandler and create_mapped_exception map TimeoutError and InterfaceError/OSError."""
    driver = AsyncpgDriver(cast("Any", MagicMock()))

    handler = driver.handle_database_exceptions()
    with pytest.raises(QueryTimeoutError):
        async with handler:
            raise TimeoutError("query timed out")
        driver._check_pending_exception(handler)

    handler2 = driver.handle_database_exceptions()
    with pytest.raises(DatabaseConnectionError):
        async with handler2:
            raise OSError("connection reset by peer")
        driver._check_pending_exception(handler2)


async def test_asyncpg_truncate_and_copy_map_errors() -> None:
    """Truncate and copy operations map errors rather than leaking unmapped exceptions."""
    conn = MagicMock()
    conn.execute = AsyncMock(side_effect=RuntimeError("truncate table failed"))
    driver = AsyncpgDriver(cast("Any", conn))

    with pytest.raises(Exception) as exc_info:
        await driver.load_from_records("users", [{"id": 1}], overwrite=True)
    assert not isinstance(exc_info.value, RuntimeError) or isinstance(exc_info.value, DatabaseConnectionError)


def test_asyncpg_build_postgres_storage_params_sql() -> None:
    """build_postgres_storage_params_sql consolidates storage and autovacuum clauses."""
    settings = {"fillfactor": 85, "autovacuum_vacuum_scale_factor": 0.05, "autovacuum_analyze_scale_factor": 0.02}
    clause = build_postgres_storage_params_sql(settings, include_autovacuum=True)
    assert "WITH (" in clause
    assert "fillfactor = 85" in clause
    assert "autovacuum_vacuum_scale_factor = 0.05" in clause
    assert "autovacuum_analyze_scale_factor = 0.02" in clause


def test_asyncpg_events_validate_listener_pool_capacity_skips_when_direct_dsn() -> None:
    """_validate_listener_pool_capacity should not enforce pool size when direct_dsn is configured."""
    from sqlspec.adapters.asyncpg.events.backend import _validate_listener_pool_capacity

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test", "max_size": 1},
        driver_features={"direct_dsn": "postgresql://localhost/direct"},
    )
    _validate_listener_pool_capacity(config)


def test_asyncpg_events_parse_notify_payload_handles_malformed_json() -> None:
    """Malformed NOTIFY payload does not raise and returns None safely."""
    from sqlspec.adapters.asyncpg.events.backend import (
        _extract_batch_size,
        _extract_event_id,
        _extract_marker_id,
        _parse_notify_payload,
    )

    bad_payload = "not valid json {{"
    assert _parse_notify_payload(bad_payload) is None
    assert _extract_event_id(bad_payload) is None
    assert _extract_batch_size(bad_payload) == 0
    assert _extract_marker_id(bad_payload) is None

    good_payload = '{"event_id": "e1", "batch_size": 5, "marker_id": "m1"}'
    parsed = _parse_notify_payload(good_payload)
    assert parsed is not None
    assert _extract_event_id(parsed) == "e1"
    assert _extract_batch_size(parsed) == 5
    assert _extract_marker_id(parsed) == "m1"


def test_asyncpg_events_resolve_listener_config_allows_mapping_direct_dsn() -> None:
    """_resolve_listener_config accepts Mapping for direct_dsn without raising."""
    from sqlspec.adapters.asyncpg.events._hub import AsyncpgListenerHub

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        driver_features={"pooler_mode": "transaction", "direct_dsn": {"host": "localhost", "database": "direct_db"}},
    )
    hub = AsyncpgListenerHub(config)
    resolved = hub._resolve_listener_config()
    assert resolved is not None
