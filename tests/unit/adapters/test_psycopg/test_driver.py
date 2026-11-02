"""Unit tests for psycopg driver transaction behavior."""

import inspect
from asyncio import CancelledError
from contextlib import nullcontext
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock, MagicMock

import psycopg
import pytest

from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.psycopg.driver import PsycopgAsyncDriver, PsycopgSyncDriver
from sqlspec.core import SQL
from sqlspec.exceptions import SerializationConflictError, SQLSpecError, UniqueViolationError

if TYPE_CHECKING:
    from sqlspec.adapters.psycopg._typing import PsycopgAsyncConnection, PsycopgSyncConnection


@pytest.mark.parametrize("error", [None, ValueError("session failed"), CancelledError("session cancelled")])
def test_psycopg_sync_session_forwards_pool_exit_exception(error: BaseException | None) -> None:
    pool = MagicMock()
    pool_context = pool.connection.return_value
    pool_context.__exit__.return_value = False
    config = PsycopgSyncConfig(connection_instance=pool)

    with pytest.raises(type(error)) if error is not None else nullcontext():
        with config.provide_session():
            if error is not None:
                raise error

    pool_context.__exit__.assert_called_once_with(
        type(error) if error is not None else None, error, error.__traceback__ if error is not None else None
    )


@pytest.mark.parametrize("error", [None, ValueError("session failed"), CancelledError("session cancelled")])
async def test_psycopg_async_session_forwards_pool_exit_exception(error: BaseException | None) -> None:
    pool = MagicMock()
    pool_context = pool.connection.return_value
    pool_context.__aenter__ = AsyncMock(return_value=MagicMock())
    pool_context.__aexit__ = AsyncMock(return_value=False)
    config = PsycopgAsyncConfig(connection_instance=pool)

    with pytest.raises(type(error)) if error is not None else nullcontext():
        async with config.provide_session():
            if error is not None:
                raise error

    pool_context.__aexit__.assert_awaited_once_with(
        type(error) if error is not None else None, error, error.__traceback__ if error is not None else None
    )


def test_psycopg_sync_session_in_except_handler_exits_successfully() -> None:
    pool = MagicMock()
    config = PsycopgSyncConfig(connection_instance=pool)

    try:
        raise ValueError("previous operation failed")
    except ValueError:
        with config.provide_session():
            pass

    pool.connection.return_value.__exit__.assert_called_once_with(None, None, None)


async def test_psycopg_async_session_in_except_handler_exits_successfully() -> None:
    pool = MagicMock()
    config = PsycopgAsyncConfig(connection_instance=pool)

    try:
        raise ValueError("previous operation failed")
    except ValueError:
        async with config.provide_session():
            pass

    pool.connection.return_value.__aexit__.assert_awaited_once_with(None, None, None)


class _SyncTransactionConnection:
    def __init__(
        self,
        *,
        autocommit: bool = True,
        begin_error: Exception | None = None,
        commit_error: Exception | None = None,
        rollback_error: Exception | None = None,
        restore_error: Exception | None = None,
        transaction_status: int = 0,
    ) -> None:
        self._autocommit = autocommit
        self.autocommit_calls: list[bool] = []
        self.begin_error = begin_error
        self.commit_calls = 0
        self.commit_error = commit_error
        self.info = SimpleNamespace(transaction_status=transaction_status)
        self.restore_error = restore_error
        self.rollback_calls = 0
        self.rollback_error = rollback_error

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        self.autocommit_calls.append(value)
        error = self.begin_error if value is False else self.restore_error
        if error is not None:
            raise error
        self._autocommit = value

    def commit(self) -> None:
        self.commit_calls += 1
        if self.commit_error is not None:
            raise self.commit_error

    def rollback(self) -> None:
        self.rollback_calls += 1
        self.info.transaction_status = 0
        if self.rollback_error is not None:
            raise self.rollback_error


class _AsyncTransactionConnection:
    def __init__(
        self,
        *,
        autocommit: bool = True,
        begin_error: Exception | None = None,
        commit_error: Exception | None = None,
        rollback_error: Exception | None = None,
        restore_error: Exception | None = None,
        transaction_status: int = 0,
    ) -> None:
        self.autocommit = autocommit
        self.autocommit_calls: list[bool] = []
        self.begin_error = begin_error
        self.commit_calls = 0
        self.commit_error = commit_error
        self.info = SimpleNamespace(transaction_status=transaction_status)
        self.restore_error = restore_error
        self.rollback_calls = 0
        self.rollback_error = rollback_error

    async def set_autocommit(self, value: bool) -> None:
        self.autocommit_calls.append(value)
        error = self.begin_error if value is False else self.restore_error
        if error is not None:
            raise error
        self.autocommit = value

    async def commit(self) -> None:
        self.commit_calls += 1
        if self.commit_error is not None:
            raise self.commit_error

    async def rollback(self) -> None:
        self.rollback_calls += 1
        self.info.transaction_status = 0
        if self.rollback_error is not None:
            raise self.rollback_error


def _sync_driver(connection: _SyncTransactionConnection) -> PsycopgSyncDriver:
    return PsycopgSyncDriver(cast("PsycopgSyncConnection", connection))


def _async_driver(connection: _AsyncTransactionConnection) -> PsycopgAsyncDriver:
    return PsycopgAsyncDriver(cast("PsycopgAsyncConnection", connection))


def test_sync_transaction_state_is_inactive_initially() -> None:
    driver = _sync_driver(_SyncTransactionConnection())

    assert driver._transaction_active is False  # pyright: ignore[reportPrivateUsage]
    assert driver._restore_autocommit is False  # pyright: ignore[reportPrivateUsage]


async def test_async_transaction_state_is_inactive_initially() -> None:
    driver = _async_driver(_AsyncTransactionConnection())

    assert driver._transaction_active is False  # pyright: ignore[reportPrivateUsage]
    assert driver._restore_autocommit is False  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("method_name", ["begin", "commit", "rollback"])
def test_sync_transaction_control_propagates_non_native_errors(method_name: str) -> None:
    error_kwargs = {f"{method_name}_error": RuntimeError("internal bug")}
    driver = _sync_driver(_SyncTransactionConnection(**error_kwargs))  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="internal bug"):
        getattr(driver, method_name)()


@pytest.mark.parametrize("method_name", ["begin", "commit", "rollback"])
def test_sync_transaction_control_wraps_native_errors(method_name: str) -> None:
    error_kwargs = {f"{method_name}_error": psycopg.Error("native failure")}
    driver = _sync_driver(_SyncTransactionConnection(**error_kwargs))  # type: ignore[arg-type]

    with pytest.raises(SQLSpecError, match=f"Failed to {method_name} transaction"):
        getattr(driver, method_name)()


@pytest.mark.parametrize("method_name", ["begin", "commit", "rollback"])
async def test_async_transaction_control_propagates_non_native_errors(method_name: str) -> None:
    error_kwargs = {f"{method_name}_error": RuntimeError("internal bug")}
    driver = _async_driver(_AsyncTransactionConnection(**error_kwargs))  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="internal bug"):
        await getattr(driver, method_name)()


@pytest.mark.parametrize("method_name", ["begin", "commit", "rollback"])
async def test_async_transaction_control_wraps_native_errors(method_name: str) -> None:
    error_kwargs = {f"{method_name}_error": psycopg.Error("native failure")}
    driver = _async_driver(_AsyncTransactionConnection(**error_kwargs))  # type: ignore[arg-type]

    with pytest.raises(SQLSpecError, match=f"Failed to {method_name} transaction"):
        await getattr(driver, method_name)()


@pytest.mark.parametrize("autocommit", [True, False])
def test_sync_commit_restores_only_changed_autocommit(autocommit: bool) -> None:
    connection = _SyncTransactionConnection(autocommit=autocommit)
    driver = _sync_driver(connection)

    driver.begin()
    driver.commit()

    assert connection.autocommit_calls == ([False, True] if autocommit else [])
    assert connection.autocommit is autocommit
    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("autocommit", [True, False])
async def test_async_commit_restores_only_changed_autocommit(autocommit: bool) -> None:
    connection = _AsyncTransactionConnection(autocommit=autocommit)
    driver = _async_driver(connection)

    await driver.begin()
    await driver.commit()

    assert connection.autocommit_calls == ([False, True] if autocommit else [])
    assert connection.autocommit is autocommit
    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]


def test_sync_rollback_restores_changed_autocommit() -> None:
    connection = _SyncTransactionConnection()
    driver = _sync_driver(connection)

    driver.begin()
    driver.rollback()

    assert connection.autocommit_calls == [False, True]
    assert connection.autocommit is True


@pytest.mark.anyio
async def test_async_rollback_restores_changed_autocommit() -> None:
    connection = _AsyncTransactionConnection()
    driver = _async_driver(connection)

    await driver.begin()
    await driver.rollback()

    assert connection.autocommit_calls == [False, True]
    assert connection.autocommit is True


@pytest.mark.parametrize("is_async", [False, True])
@pytest.mark.parametrize(
    ("query", "parameters", "wire_sql"),
    [
        ("SELECT '100%' AS value", (), "SELECT '100%' AS value"),
        ("SELECT 10 % 3 AS value", (), "SELECT 10 % 3 AS value"),
        ("SELECT %s AS value WHERE '100%' LIKE '100%'", (1,), "SELECT %s AS value WHERE '100%%' LIKE '100%%'"),
    ],
)
async def test_cached_percent_sql_preserves_native_binding(
    is_async: bool, query: str, parameters: tuple[int, ...], wire_sql: str
) -> None:
    """Cached and uncached execution apply percent escaping only with native parameters."""
    cursor = MagicMock()
    cursor.description = [SimpleNamespace(name="value", type_code=23)]
    cursor.rowcount = 1
    if is_async:
        cursor.execute = AsyncMock()
        cursor.fetchall = AsyncMock(return_value=[(1,)])
        cursor.close = AsyncMock()
    else:
        cursor.fetchall.return_value = [(1,)]
    connection = MagicMock()
    connection.cursor.return_value = cursor
    driver = PsycopgAsyncDriver(connection) if is_async else PsycopgSyncDriver(connection)

    for _ in range(2):
        result = driver.execute(query, *parameters)
        if inspect.isawaitable(result):
            result = await result
        assert result.get_data() == [{"value": 1}]

    assert [call.args[0] for call in cursor.execute.call_args_list] == [wire_sql, wire_sql]
    bound = [tuple(call.args[1]) if len(call.args) > 1 else () for call in cursor.execute.call_args_list]
    assert bound == [parameters, parameters]


def test_psycopg_sync_stream_source_runs_with_exception_handler() -> None:
    """PsycopgSyncStreamSource delegates to _run_with_exception_handler inside active except block."""
    from sqlspec.adapters.psycopg.core import PsycopgSyncStreamSource

    connection = MagicMock()
    driver = PsycopgSyncDriver(connection=connection)
    source = PsycopgSyncStreamSource(driver, "SELECT 1", (), 10)

    try:
        raise ValueError("ambient error")
    except ValueError:
        source.start()

    assert source._cursor is not None
    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]


def test_sync_begin_is_idempotent_for_logically_owned_transaction() -> None:
    connection = _SyncTransactionConnection()
    driver = _sync_driver(connection)

    driver.begin()
    driver.begin()

    assert connection.autocommit_calls == [False]
    assert driver._connection_in_transaction() is True  # pyright: ignore[reportPrivateUsage]


async def test_async_begin_is_idempotent_for_logically_owned_transaction() -> None:
    connection = _AsyncTransactionConnection()
    driver = _async_driver(connection)

    await driver.begin()
    await driver.begin()

    assert connection.autocommit_calls == [False]
    assert driver._connection_in_transaction() is True  # pyright: ignore[reportPrivateUsage]


def test_sync_begin_is_idempotent_for_libpq_owned_transaction() -> None:
    connection = _SyncTransactionConnection(transaction_status=2)
    driver = _sync_driver(connection)

    driver.begin()

    assert connection.autocommit_calls == []
    assert driver._transaction_active is False  # pyright: ignore[reportPrivateUsage]


async def test_async_begin_is_idempotent_for_libpq_owned_transaction() -> None:
    connection = _AsyncTransactionConnection(transaction_status=2)
    driver = _async_driver(connection)

    await driver.begin()

    assert connection.autocommit_calls == []
    assert driver._transaction_active is False  # pyright: ignore[reportPrivateUsage]


def test_sync_begin_failure_preserves_inactive_state() -> None:
    driver = _sync_driver(_SyncTransactionConnection(begin_error=psycopg.Error("setter failed")))

    with pytest.raises(SQLSpecError, match="Failed to begin transaction"):
        driver.begin()

    assert driver._transaction_active is False  # pyright: ignore[reportPrivateUsage]
    assert driver._restore_autocommit is False  # pyright: ignore[reportPrivateUsage]


async def test_async_begin_failure_preserves_inactive_state() -> None:
    driver = _async_driver(_AsyncTransactionConnection(begin_error=psycopg.Error("setter failed")))

    with pytest.raises(SQLSpecError, match="Failed to begin transaction"):
        await driver.begin()

    assert driver._transaction_active is False  # pyright: ignore[reportPrivateUsage]
    assert driver._restore_autocommit is False  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("method_name", ["commit", "rollback"])
def test_sync_native_completion_failure_preserves_transaction_state(method_name: str) -> None:
    error_kwargs = {f"{method_name}_error": psycopg.Error("native failure")}
    driver = _sync_driver(_SyncTransactionConnection(**error_kwargs))  # type: ignore[arg-type]
    driver.begin()

    with pytest.raises(SQLSpecError, match=f"Failed to {method_name} transaction"):
        getattr(driver, method_name)()

    assert driver._transaction_active is True  # pyright: ignore[reportPrivateUsage]
    assert driver._restore_autocommit is True  # pyright: ignore[reportPrivateUsage]
    assert driver._connection_in_transaction() is True  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("method_name", ["commit", "rollback"])
async def test_async_native_completion_failure_preserves_transaction_state(method_name: str) -> None:
    error_kwargs = {f"{method_name}_error": psycopg.Error("native failure")}
    driver = _async_driver(_AsyncTransactionConnection(**error_kwargs))  # type: ignore[arg-type]
    await driver.begin()

    with pytest.raises(SQLSpecError, match=f"Failed to {method_name} transaction"):
        await getattr(driver, method_name)()

    assert driver._transaction_active is True  # pyright: ignore[reportPrivateUsage]
    assert driver._restore_autocommit is True  # pyright: ignore[reportPrivateUsage]
    assert driver._connection_in_transaction() is True  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("method_name", ["commit", "rollback"])
def test_sync_restoration_failure_reports_completed_transaction(method_name: str) -> None:
    connection = _SyncTransactionConnection(restore_error=psycopg.Error("restore failed"))
    driver = _sync_driver(connection)
    driver.begin()

    with pytest.raises(SQLSpecError, match="Failed to restore autocommit: restore failed"):
        getattr(driver, method_name)()

    assert connection.commit_calls == (1 if method_name == "commit" else 0)
    assert connection.rollback_calls == (1 if method_name == "rollback" else 0)
    assert driver._transaction_active is False  # pyright: ignore[reportPrivateUsage]
    assert driver._restore_autocommit is False  # pyright: ignore[reportPrivateUsage]
    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("method_name", ["commit", "rollback"])
async def test_async_restoration_failure_reports_completed_transaction(method_name: str) -> None:
    connection = _AsyncTransactionConnection(restore_error=psycopg.Error("restore failed"))
    driver = _async_driver(connection)
    await driver.begin()

    with pytest.raises(SQLSpecError, match="Failed to restore autocommit: restore failed"):
        await getattr(driver, method_name)()

    assert connection.commit_calls == (1 if method_name == "commit" else 0)
    assert connection.rollback_calls == (1 if method_name == "rollback" else 0)
    assert driver._transaction_active is False  # pyright: ignore[reportPrivateUsage]
    assert driver._restore_autocommit is False  # pyright: ignore[reportPrivateUsage]
    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]


def test_sync_logical_transaction_is_reported_while_libpq_is_idle() -> None:
    driver = _sync_driver(_SyncTransactionConnection(transaction_status=0))

    driver.begin()

    assert driver._connection_in_transaction() is True  # pyright: ignore[reportPrivateUsage]


async def test_async_logical_transaction_is_reported_while_libpq_is_idle() -> None:
    driver = _async_driver(_AsyncTransactionConnection(transaction_status=0))

    await driver.begin()

    assert driver._connection_in_transaction() is True  # pyright: ignore[reportPrivateUsage]


def test_sync_inerror_status_is_not_active_transaction_and_rolls_back_on_begin() -> None:
    """TRANSACTION_STATUS_INERROR (3) is reported in transaction state and rolls back on begin()."""
    connection = _SyncTransactionConnection(transaction_status=3)
    driver = _sync_driver(connection)

    assert driver._connection_in_transaction() is True  # pyright: ignore[reportPrivateUsage]
    driver.begin()

    assert connection.rollback_calls == 1
    assert connection.autocommit_calls == [False]
    assert driver._transaction_active is True  # pyright: ignore[reportPrivateUsage]


async def test_async_inerror_status_is_not_active_transaction_and_rolls_back_on_begin() -> None:
    """TRANSACTION_STATUS_INERROR (3) is reported in transaction state and rolls back on async begin()."""
    connection = _AsyncTransactionConnection(transaction_status=3)
    driver = _async_driver(connection)

    assert driver._connection_in_transaction() is True  # pyright: ignore[reportPrivateUsage]
    await driver.begin()

    assert connection.rollback_calls == 1
    assert connection.autocommit_calls == [False]
    assert driver._transaction_active is True  # pyright: ignore[reportPrivateUsage]


def test_sync_commit_maps_typed_psycopg_exception() -> None:
    """commit() should preserve typed SQLSpec exceptions mapped from psycopg errors."""
    connection = _SyncTransactionConnection(commit_error=psycopg.errors.SerializationFailure("could not serialize"))
    driver = _sync_driver(connection)
    driver.begin()

    with pytest.raises(SerializationConflictError, match="serialization failure"):
        driver.commit()


def test_sync_copy_to_stdout_decodes_memoryview_chunks_and_reports_rowcount() -> None:
    """COPY ... TO STDOUT should decode memoryview chunks without str(memoryview) corruption."""
    driver = _sync_driver(_SyncTransactionConnection())
    cursor = MagicMock()
    cursor.rowcount = 2
    copy_ctx = MagicMock()
    copy_ctx.__enter__.return_value = [memoryview(b"1\talice\n"), memoryview(b"2\tbob\n")]
    copy_ctx.__exit__.return_value = False
    cursor.copy.return_value = copy_ctx

    statement = SQL("COPY users TO STDOUT")
    statement.compile()
    result = driver.dispatch_special_handling(cursor, statement)
    assert result is not None
    assert result.get_data() == [{"copy_output": "1\talice\n2\tbob\n"}]
    assert result.rows_affected == 2


def test_sync_session_exit_rolls_back_uncommitted_implicit_transaction() -> None:
    """Session exit rolls back uncommitted implicit autocommit=False transactions."""
    connection = _SyncTransactionConnection(transaction_status=2)
    config = PsycopgSyncConfig()
    context = config.provide_session()
    object.__setattr__(context, "_acquire_connection", lambda: connection)
    with context:
        pass
    assert connection.rollback_calls == 1


@pytest.mark.anyio
async def test_async_session_exit_rolls_back_uncommitted_implicit_transaction() -> None:
    """Async session exit rolls back uncommitted implicit autocommit=False transactions."""
    connection = _AsyncTransactionConnection(transaction_status=2)
    config = PsycopgAsyncConfig()
    context = config.provide_session()
    object.__setattr__(context, "_acquire_connection", AsyncMock(return_value=connection))
    async with context:
        pass
    assert connection.rollback_calls == 1


@pytest.mark.anyio
async def test_psycopg_async_load_from_arrow_runs_with_exception_handler() -> None:
    """PsycopgAsyncDriver.load_from_arrow routes operations through exception handler inside active except block."""
    import pyarrow as pa

    connection = MagicMock()
    cursor_mock = MagicMock()
    cursor_mock.execute = AsyncMock()
    copy_ctx = AsyncMock()
    cursor_mock.copy.return_value = copy_ctx
    connection.cursor.return_value = cursor_mock

    driver = PsycopgAsyncDriver(
        connection=connection, driver_features={"storage_capabilities": {"arrow_import_enabled": True}}
    )
    table = pa.table({"id": [1, 2], "val": ["a", "b"]})

    try:
        raise ValueError("ambient error")
    except ValueError:
        job = await driver.load_from_arrow("test_table", table, overwrite=True)

    assert job.telemetry["rows_processed"] == 2


@pytest.mark.parametrize("is_async", [False, True])
@pytest.mark.parametrize("binary", [False, True])
@pytest.mark.parametrize("failure_phase", ["enter", "exit"])
async def test_copy_lifecycle_maps_native_errors(is_async: bool, binary: bool, failure_phase: str) -> None:
    """COPY setup and finalization errors retain SQLSpec mapping even inside an except block."""
    import pyarrow as pa

    native_error = psycopg.errors.UniqueViolation("duplicate key value violates unique constraint items_pk")
    copy_context = AsyncMock() if is_async else MagicMock()
    enter = copy_context.__aenter__ if is_async else copy_context.__enter__
    exit_ = copy_context.__aexit__ if is_async else copy_context.__exit__
    enter.return_value = copy_context
    if failure_phase == "enter":
        enter.side_effect = native_error
    else:
        exit_.side_effect = native_error
    cursor = MagicMock()
    cursor.copy.return_value = copy_context
    if is_async:
        cursor.close = AsyncMock()
    connection = MagicMock()
    connection.cursor.return_value = cursor
    features = {"storage_capabilities": {"arrow_import_enabled": True}, "enable_pgcopy_binary": binary}
    driver = (
        PsycopgAsyncDriver(connection, driver_features=features)
        if is_async
        else PsycopgSyncDriver(connection, driver_features=features)
    )

    try:
        raise ValueError("ambient exception")
    except ValueError:
        with pytest.raises(UniqueViolationError) as error:
            result = driver.load_from_arrow("items", pa.table({"id": [1, 1]}))
            if inspect.isawaitable(result):
                await result
    assert error.value.__cause__ is native_error


def test_sync_dispatch_execute_script_emits_statement_observer_events() -> None:
    """PsycopgSyncDriver.dispatch_execute_script emits script statement events to observers."""
    from sqlspec.core import SQL
    from sqlspec.observability import ObservabilityConfig, ObservabilityRuntime

    events: list[Any] = []
    connection = MagicMock()
    cursor = MagicMock()
    connection.cursor.return_value = cursor

    runtime = ObservabilityRuntime(ObservabilityConfig(statement_observers=(events.append,)))
    driver = PsycopgSyncDriver(connection=connection)
    driver._observability = runtime

    stmt = SQL("SELECT 1; SELECT 2;")
    result = driver.dispatch_execute_script(cursor, stmt)

    assert result.statement_count == 2
    assert len(events) == 2
    assert events[0].script_ordinal == 0
    assert events[1].script_ordinal == 1


@pytest.mark.anyio
async def test_async_dispatch_execute_script_emits_statement_observer_events() -> None:
    """PsycopgAsyncDriver.dispatch_execute_script emits script statement events to observers."""
    from sqlspec.core import SQL
    from sqlspec.observability import ObservabilityConfig, ObservabilityRuntime

    events: list[Any] = []
    connection = MagicMock()
    cursor = AsyncMock()
    connection.cursor.return_value = cursor

    runtime = ObservabilityRuntime(ObservabilityConfig(statement_observers=(events.append,)))
    driver = PsycopgAsyncDriver(connection=connection)
    driver._observability = runtime

    stmt = SQL("SELECT 1; SELECT 2;")
    result = await driver.dispatch_execute_script(cursor, stmt)

    assert result.statement_count == 2
    assert len(events) == 2
    assert events[0].script_ordinal == 0
    assert events[1].script_ordinal == 1


def test_sync_load_from_arrow_preserves_cause_on_failure() -> None:
    """PsycopgSyncDriver.load_from_arrow preserves underlying __cause__ on execution failure."""
    import psycopg
    import pyarrow as pa

    connection = MagicMock()
    cursor_mock = MagicMock()
    cursor_mock.execute.side_effect = psycopg.OperationalError("db error")
    connection.cursor.return_value = cursor_mock

    driver = PsycopgSyncDriver(
        connection=connection, driver_features={"storage_capabilities": {"arrow_import_enabled": True}}
    )
    table = pa.table({"id": [1, 2]})

    with pytest.raises(Exception) as exc_info:
        driver.load_from_arrow("test_table", table, overwrite=True)

    assert exc_info.value.__cause__ is not None
