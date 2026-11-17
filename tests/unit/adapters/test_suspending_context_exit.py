"""Async driver paths exit a suspending context exactly once and keep the managed block's outcome."""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, cast

import pytest

import sqlspec.adapters.mysqlconnector.driver as mysqlconnector_driver
from sqlspec.adapters.asyncpg.driver import AsyncpgDriver
from sqlspec.adapters.db2.driver import Db2AsyncDriver
from sqlspec.adapters.mysqlconnector._typing import MysqlConnectorError
from sqlspec.adapters.mysqlconnector.driver import MysqlConnectorAsyncDriver
from sqlspec.adapters.oracledb.driver import OracleAsyncDriver
from sqlspec.adapters.spanner.driver import SpannerAsyncDriver
from sqlspec.core import StatementStack
from sqlspec.exceptions import SQLSpecError, StackExecutionError

pytestmark = pytest.mark.anyio

_OUTCOMES = ("succeeds", "raises", "cancelled")
_STORAGE_CAPABILITIES: dict[str, Any] = {
    "arrow_export_enabled": True,
    "arrow_import_enabled": True,
    "parquet_export_enabled": True,
    "parquet_import_enabled": True,
    "partition_strategies": [],
}


class _SuspendingContext:
    """Async context manager whose exit yields to the event loop before recording its arguments."""

    def __init__(self) -> None:
        self.value: Any = None
        self.entered = 0
        self.exits: list[tuple[type[BaseException] | None, BaseException | None]] = []

    async def __aenter__(self) -> Any:
        self.entered += 1
        return self.value

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: object
    ) -> None:
        await asyncio.sleep(0)
        self.exits.append((exc_type, exc_val))


class _Body:
    """Managed block stand-in that returns, raises, or waits until it is cancelled."""

    def __init__(self, outcome: str, result: Any = None) -> None:
        self.outcome = outcome
        self.result = result
        self.error = RuntimeError("managed block failed")
        self.started = asyncio.Event()

    async def run(self, *_args: Any, **_kwargs: Any) -> Any:
        self.started.set()
        await asyncio.sleep(0)
        if self.outcome == "raises":
            raise self.error
        if self.outcome == "cancelled":
            await asyncio.Event().wait()
        return self.result


class _Cursor:
    """Cursor stand-in whose statement calls run the managed block."""

    def __init__(self, body: _Body) -> None:
        self.execute = body.run
        self.executemany = body.run

    async def fetchone(self) -> "tuple[str]":
        return ("APP",)


class _MutationGroups:
    """Spanner mutation-group stand-in whose batch write runs the managed block."""

    def __init__(self, body: _Body) -> None:
        self._body = body
        self.rows: list[Any] = []

    def group(self) -> "_MutationGroups":
        return self

    def insert_or_update(self, table: str, columns: "list[str]", values: "list[list[Any]]") -> None:
        self.rows.extend(values)

    async def batch_write(self) -> "AsyncIterator[Any]":
        await self._body.run()
        return _no_responses()


class _MutationGroupsDatabase:
    def __init__(self, context: _SuspendingContext) -> None:
        self._context = context

    def mutation_groups(self) -> _SuspendingContext:
        return self._context


class _AsyncpgConnection:
    """asyncpg connection stand-in that suspends on every round trip."""

    def __init__(self, body: _Body, transaction: "_SuspendingContext | None", *, in_transaction: bool) -> None:
        self._body = body
        self._transaction = transaction
        self._in_transaction = in_transaction
        self.commands: list[str] = []

    def is_in_transaction(self) -> bool:
        return self._in_transaction

    def transaction(self) -> "_SuspendingContext | None":
        return self._transaction

    async def execute(self, sql: str, *_args: Any, **_kwargs: Any) -> str:
        self.commands.append(sql.split()[0])
        if sql.startswith("UPDATE"):
            return cast("str", await self._body.run())
        await asyncio.sleep(0)
        return sql.split()[0]


async def _no_responses() -> "AsyncIterator[Any]":
    responses: tuple[Any, ...] = ()
    for response in responses:
        yield response


def _db2_driver(body: _Body, context: _SuspendingContext) -> Db2AsyncDriver:
    class _Driver(Db2AsyncDriver):
        def with_cursor(self, connection: Any) -> Any:
            return context

    context.value = _Cursor(body)
    return _Driver(connection=object())


def _db2_set_schema(
    body: _Body, context: _SuspendingContext, monkeypatch: pytest.MonkeyPatch
) -> "tuple[Callable[[], Awaitable[Any]], Any]":
    driver = _db2_driver(body, context)
    return lambda: driver.set_migration_session_schema("tenant"), None


def _db2_reset_schema(
    body: _Body, context: _SuspendingContext, monkeypatch: pytest.MonkeyPatch
) -> "tuple[Callable[[], Awaitable[Any]], Any]":
    driver = _db2_driver(body, context)
    driver._migration_schema_restore = "APP"  # pyright: ignore[reportPrivateUsage]
    return driver.reset_migration_session_schema, None


def _db2_has_schema(
    body: _Body, context: _SuspendingContext, monkeypatch: pytest.MonkeyPatch
) -> "tuple[Callable[[], Awaitable[Any]], Any]":
    driver = _db2_driver(body, context)
    return lambda: driver.has_schema("tenant"), True


def _oracle_load_without_indexes(
    body: _Body, context: _SuspendingContext, monkeypatch: pytest.MonkeyPatch
) -> "tuple[Callable[[], Awaitable[Any]], Any]":
    class _Driver(OracleAsyncDriver):
        def without_indexes(self, table: str, *, schema: "str | None" = None, concurrent: bool = False) -> Any:
            return context

        async def load_from_arrow(self, table: str, source: Any, **kwargs: Any) -> Any:
            if kwargs.get("drop_indexes"):
                return await super().load_from_arrow(table, source, **kwargs)
            return await body.run()

    body.result = object()
    driver = _Driver(cast("Any", object()), driver_features={"storage_capabilities": _STORAGE_CAPABILITIES})
    return lambda: driver.load_from_arrow("t", object(), drop_indexes=True), body.result


def _spanner_load_without_indexes(
    body: _Body, context: _SuspendingContext, monkeypatch: pytest.MonkeyPatch
) -> "tuple[Callable[[], Awaitable[Any]], Any]":
    class _Driver(SpannerAsyncDriver):
        def without_indexes(self, table: str, *, schema: "str | None" = None, concurrent: bool = False) -> Any:
            return context

        async def load_from_arrow(self, table: str, source: Any, **kwargs: Any) -> Any:
            if kwargs.get("drop_indexes"):
                return await super().load_from_arrow(table, source, **kwargs)
            return await body.run()

    body.result = object()
    driver = _Driver(cast("Any", object()), driver_features={"storage_capabilities": _STORAGE_CAPABILITIES})
    return lambda: driver.load_from_arrow("t", object(), drop_indexes=True), body.result


def _spanner_batch_write(
    body: _Body, context: _SuspendingContext, monkeypatch: pytest.MonkeyPatch
) -> "tuple[Callable[[], Awaitable[Any]], Any]":
    class _Driver(SpannerAsyncDriver):
        def _resolve_database(self) -> Any:
            return _MutationGroupsDatabase(context)

    context.value = _MutationGroups(body)
    driver = _Driver(cast("Any", object()))
    return lambda: driver._batch_write_mutations("t", ["id"], [[[1]], [[2]]]), None  # pyright: ignore[reportPrivateUsage]


def _mysqlconnector_driver(
    body: _Body, context: _SuspendingContext, monkeypatch: pytest.MonkeyPatch
) -> MysqlConnectorAsyncDriver:
    context.value = _Cursor(body)
    monkeypatch.setattr(mysqlconnector_driver, "MysqlConnectorAsyncCursor", lambda *_args: context)
    return MysqlConnectorAsyncDriver(cast("Any", object()))


def _mysqlconnector_begin(
    body: _Body, context: _SuspendingContext, monkeypatch: pytest.MonkeyPatch
) -> "tuple[Callable[[], Awaitable[Any]], Any]":
    return _mysqlconnector_driver(body, context, monkeypatch).begin, None


def _mysqlconnector_execute(
    body: _Body, context: _SuspendingContext, monkeypatch: pytest.MonkeyPatch
) -> "tuple[Callable[[], Awaitable[Any]], Any]":
    driver = _mysqlconnector_driver(body, context, monkeypatch)
    return lambda: driver._execute_cursor_statement("DELETE FROM t"), None  # pyright: ignore[reportPrivateUsage]


def _mysqlconnector_execute_unprepared(
    body: _Body, context: _SuspendingContext, monkeypatch: pytest.MonkeyPatch
) -> "tuple[Callable[[], Awaitable[Any]], Any]":
    driver = _mysqlconnector_driver(body, context, monkeypatch)
    return lambda: driver._execute_cursor_statement_unprepared("TRUNCATE TABLE t", ("x",)), None  # pyright: ignore[reportPrivateUsage]


def _mysqlconnector_executemany(
    body: _Body, context: _SuspendingContext, monkeypatch: pytest.MonkeyPatch
) -> "tuple[Callable[[], Awaitable[Any]], Any]":
    driver = _mysqlconnector_driver(body, context, monkeypatch)
    return lambda: driver._executemany_cursor_statement("INSERT INTO t VALUES (%s)", [(1,)]), None  # pyright: ignore[reportPrivateUsage]


_SCENARIOS = (
    _db2_set_schema,
    _db2_reset_schema,
    _db2_has_schema,
    _oracle_load_without_indexes,
    _spanner_load_without_indexes,
    _spanner_batch_write,
    _mysqlconnector_begin,
    _mysqlconnector_execute,
    _mysqlconnector_execute_unprepared,
    _mysqlconnector_executemany,
)


async def _cancel_once_started(task: "asyncio.Future[Any]", body: _Body) -> None:
    await asyncio.wait_for(body.started.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.parametrize("outcome", _OUTCOMES)
@pytest.mark.parametrize("scenario", _SCENARIOS, ids=lambda scenario: scenario.__name__.lstrip("_"))
async def test_suspending_context_exits_once_with_the_block_outcome(
    scenario: "Callable[..., tuple[Callable[[], Awaitable[Any]], Any]]", outcome: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    body = _Body(outcome)
    context = _SuspendingContext()
    call, expected = scenario(body, context, monkeypatch)
    task = asyncio.ensure_future(call())

    if outcome == "succeeds":
        assert await task is expected
        assert context.exits == [(None, None)]
    elif outcome == "raises":
        with pytest.raises(RuntimeError) as caught:
            await task
        assert caught.value is body.error
        assert context.exits == [(RuntimeError, body.error)]
    else:
        await _cancel_once_started(task, body)
        assert [exc_type for exc_type, _ in context.exits] == [asyncio.CancelledError]
    assert context.entered == 1


async def test_mysqlconnector_begin_maps_a_driver_error_after_closing_the_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = _Body("raises")
    driver_error = MysqlConnectorError("server has gone away")
    body.error = cast("Any", driver_error)
    context = _SuspendingContext()
    driver = _mysqlconnector_driver(body, context, monkeypatch)

    with pytest.raises(SQLSpecError) as caught:
        await driver.begin()

    assert caught.value.__cause__ is driver_error
    assert context.exits == [(type(driver_error), driver_error)]


@pytest.mark.parametrize("outcome", _OUTCOMES)
async def test_asyncpg_stack_exits_its_transaction_once_with_the_stack_outcome(outcome: str) -> None:
    body = _Body(outcome, result="UPDATE 1")
    transaction = _SuspendingContext()
    connection = _AsyncpgConnection(body, transaction, in_transaction=False)
    driver = AsyncpgDriver(cast("Any", connection))
    stack = StatementStack().push_execute("UPDATE t SET x = 1")
    task = asyncio.ensure_future(driver.execute_stack(stack))

    if outcome == "succeeds":
        results = await task
        assert [result.rows_affected for result in results] == [1]
        assert transaction.exits == [(None, None)]
    elif outcome == "raises":
        with pytest.raises(StackExecutionError) as caught:
            await task
        assert caught.value.__cause__ is body.error
        assert transaction.exits == [(StackExecutionError, caught.value)]
    else:
        await _cancel_once_started(task, body)
        assert [exc_type for exc_type, _ in transaction.exits] == [asyncio.CancelledError]
    assert transaction.entered == 1


@pytest.mark.parametrize("outcome", _OUTCOMES)
async def test_asyncpg_stack_savepoint_is_released_once_with_the_operation_outcome(outcome: str) -> None:
    body = _Body(outcome, result="UPDATE 1")
    connection = _AsyncpgConnection(body, None, in_transaction=True)
    driver = AsyncpgDriver(cast("Any", connection))
    stack = StatementStack().push_execute("UPDATE t SET x = 1")
    task = asyncio.ensure_future(driver.execute_stack(stack, continue_on_error=True))

    if outcome == "succeeds":
        results = await task
        assert [result.rows_affected for result in results] == [1]
        assert connection.commands == ["SAVEPOINT", "UPDATE", "RELEASE"]
        return
    if outcome == "raises":
        results = await task
        error = results[0].error
        assert isinstance(error, StackExecutionError)
        assert error.original_error is body.error
    else:
        await _cancel_once_started(task, body)
    assert connection.commands == ["SAVEPOINT", "UPDATE", "ROLLBACK", "RELEASE"]
