"""Unit tests for psycopg driver transaction behavior."""

from asyncio import CancelledError
from contextlib import nullcontext
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast
from unittest.mock import AsyncMock, MagicMock

import psycopg
import pytest

from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.psycopg.driver import PsycopgAsyncDriver, PsycopgSyncDriver
from sqlspec.core import SQL
from sqlspec.exceptions import SerializationConflictError, SQLSpecError

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


def test_sync_cache_hit_with_literal_percent_avoids_fallback() -> None:
    """_execute_cache_hit for query with literal percent uses pre-escaped wire SQL."""
    from sqlspec.core.cache import CachedQuery
    from sqlspec.core.compiler import OperationProfile
    from sqlspec.core.parameters import ParameterInfo, ParameterProfile, ParameterStyle
    from sqlspec.core.statement import ProcessedState

    connection = MagicMock()
    cursor = MagicMock()
    cursor.description = [SimpleNamespace(name="id")]
    cursor.fetchall.return_value = [(1,)]
    cursor.rowcount = 1
    connection.cursor.return_value = cursor
    driver = PsycopgSyncDriver(connection=connection)

    cached = CachedQuery(
        compiled_sql="SELECT * FROM items WHERE name LIKE '%%test%%' AND id = %s",
        parameter_profile=ParameterProfile([ParameterInfo("id", ParameterStyle.POSITIONAL_PYFORMAT, 0, 1, "%s")]),
        input_named_parameters=(),
        applied_wrap_types=False,
        parameter_casts={},
        operation_type="SELECT",
        operation_profile=OperationProfile(returns_rows=True, modifies_rows=False),
        param_count=1,
        processed_state=ProcessedState(
            compiled_sql="SELECT * FROM items WHERE name LIKE '%%test%%' AND id = %s",
            execution_parameters=[],
            operation_type="SELECT",
        ),
    )
    result = driver._execute_cache_hit("SELECT * FROM items WHERE name LIKE '%test%' AND id = %s", (1,), cached)
    assert result is not None
    assert cursor.execute.called


@pytest.mark.anyio
async def test_async_cache_hit_with_literal_percent_avoids_fallback() -> None:
    """async _execute_cache_hit for query with literal percent uses pre-escaped wire SQL."""
    from sqlspec.core.cache import CachedQuery
    from sqlspec.core.compiler import OperationProfile
    from sqlspec.core.parameters import ParameterInfo, ParameterProfile, ParameterStyle
    from sqlspec.core.statement import ProcessedState

    connection = MagicMock()
    cursor = AsyncMock()
    cursor.description = [SimpleNamespace(name="id")]
    cursor.fetchall = AsyncMock(return_value=[(1,)])
    cursor.rowcount = 1
    connection.cursor.return_value = cursor
    driver = PsycopgAsyncDriver(connection=connection)

    cached = CachedQuery(
        compiled_sql="SELECT * FROM items WHERE name LIKE '%%test%%' AND id = %s",
        parameter_profile=ParameterProfile([ParameterInfo("id", ParameterStyle.POSITIONAL_PYFORMAT, 0, 1, "%s")]),
        input_named_parameters=(),
        applied_wrap_types=False,
        parameter_casts={},
        operation_type="SELECT",
        operation_profile=OperationProfile(returns_rows=True, modifies_rows=False),
        param_count=1,
        processed_state=ProcessedState(
            compiled_sql="SELECT * FROM items WHERE name LIKE '%%test%%' AND id = %s",
            execution_parameters=[],
            operation_type="SELECT",
        ),
    )
    result = await driver._execute_cache_hit("SELECT * FROM items WHERE name LIKE '%test%' AND id = %s", (1,), cached)
    assert result is not None
    assert cursor.execute.called


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
