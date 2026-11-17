"""Async psycopg paths finish when a managed context suspends while it exits."""

import asyncio
from collections.abc import AsyncIterator, Awaitable
from types import SimpleNamespace
from typing import Any

import pytest

from sqlspec.adapters.cockroach_psycopg.driver import CockroachPsycopgAsyncDriver
from sqlspec.adapters.psycopg.driver import PsycopgAsyncDriver
from sqlspec.core import SQL
from sqlspec.core.stack import StatementStack
from sqlspec.exceptions import StackExecutionError

_SETTLE_TIMEOUT = 5.0
_DRIVER_TYPES = [PsycopgAsyncDriver, CockroachPsycopgAsyncDriver]


class _Outcome:
    """Decides how the awaited operation inside a managed block ends."""

    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.started = asyncio.Event()
        self.error = RuntimeError("delivered through a future")

    async def run(self, *_: Any) -> None:
        self.started.set()
        if self.mode == "success":
            await asyncio.sleep(0)
            return
        loop = asyncio.get_running_loop()
        pending = loop.create_future()
        if self.mode == "error":
            loop.call_soon(pending.set_exception, self.error)
        await pending


class _SuspendingContext:
    """Async context manager whose exit yields to the event loop before finishing."""

    def __init__(self, value: Any, events: "list[str]", label: str) -> None:
        self._value = value
        self._events = events
        self._label = label
        self.exit_errors: list[BaseException | None] = []

    async def __aenter__(self) -> Any:
        return self._value

    async def __aexit__(self, exc_type: Any, exc: "BaseException | None", traceback: Any) -> bool:
        await asyncio.sleep(0)
        self.exit_errors.append(exc)
        self._events.append(self._label)
        return False


class _Cursor:
    description = None
    rowcount = 1
    statusmessage = "INSERT 0 1"

    def __init__(self, connection: "_Connection", index: int) -> None:
        self._connection = connection
        self._index = index

    def copy(self, _sql: Any) -> _SuspendingContext:
        return self._connection.copy_context

    async def execute(self, _sql: Any, _parameters: Any = None) -> None:
        await self._connection.outcome.run()

    async def fetchall(self) -> "list[Any]":
        return []

    async def close(self) -> None:
        await asyncio.sleep(0)
        self._connection.events.append(f"cursor-{self._index}")


class _CopyRows:
    """Copy handle whose writes and row iteration follow the outcome."""

    def __init__(self, outcome: _Outcome) -> None:
        self._outcome = outcome
        self.write = outcome.run
        self.write_row = outcome.run

    async def __aiter__(self) -> "AsyncIterator[Any]":
        yield b"1\talice\n"
        await self._outcome.run()
        yield memoryview(b"2\tbob\n")


class _Connection:
    def __init__(self, outcome: _Outcome) -> None:
        self.autocommit = False
        self.info = SimpleNamespace(transaction_status=0)
        self.outcome = outcome
        self.events: list[str] = []
        self.copy_context = _SuspendingContext(_CopyRows(outcome), self.events, "copy")
        self.pipeline_context = _SuspendingContext(SimpleNamespace(sync=self._sync), self.events, "pipeline")
        self._cursor_count = 0

    def cursor(self) -> _Cursor:
        cursor = _Cursor(self, self._cursor_count)
        self._cursor_count += 1
        return cursor

    def pipeline(self) -> _SuspendingContext:
        return self.pipeline_context

    async def commit(self) -> None:
        self.events.append("commit")

    async def rollback(self) -> None:
        self.events.append("rollback")

    async def _sync(self) -> None:
        return None


def _copy_from(driver: Any, connection: _Connection) -> Any:
    statement = SQL("COPY users FROM STDIN", "1\talice\n")
    statement.compile()
    return driver.dispatch_special_handling(connection.cursor(), statement)


def _copy_to(driver: Any, connection: _Connection) -> Any:
    statement = SQL("COPY users TO STDOUT")
    statement.compile()
    return driver.dispatch_special_handling(connection.cursor(), statement)


def _arrow_copy(driver: Any, _connection: _Connection) -> Any:
    import pyarrow as pa

    return driver.load_from_arrow("users", pa.table({"id": [1]}))


def _stack_pipeline(driver: Any, _connection: _Connection) -> Any:
    return driver.execute_stack(StatementStack().push_execute("INSERT INTO users (id) VALUES (:id)", id=1))


_SITES: "dict[str, tuple[Any, str, list[str], list[str]]]" = {
    "copy-from": (_copy_from, "copy", ["copy"], ["copy"]),
    "copy-to": (_copy_to, "copy", ["copy"], ["copy"]),
    "arrow-copy": (_arrow_copy, "copy", ["copy", "cursor-0"], ["copy", "cursor-0"]),
    "stack-pipeline": (
        _stack_pipeline,
        "pipeline",
        ["cursor-0", "pipeline", "commit"],
        ["cursor-0", "pipeline", "rollback"],
    ),
}


async def _settle(awaitable: "Awaitable[Any]", outcome: _Outcome) -> "asyncio.Task[Any]":
    """Run the driver call to completion, cancelling it mid-block when the outcome asks for it."""
    task = asyncio.ensure_future(awaitable)
    await asyncio.wait_for(outcome.started.wait(), _SETTLE_TIMEOUT)
    if outcome.mode == "cancel":
        task.cancel()
    done, _ = await asyncio.wait({task}, timeout=_SETTLE_TIMEOUT)
    assert task in done
    return task


def _driver(driver_type: Any, connection: _Connection) -> Any:
    return driver_type(connection, driver_features={"storage_capabilities": {"arrow_import_enabled": True}})


@pytest.mark.parametrize("driver_type", _DRIVER_TYPES)
@pytest.mark.parametrize("site", list(_SITES))
async def test_block_success_exits_each_context_once(driver_type: Any, site: str) -> None:
    """A completed block exits its contexts once, inner first, without an error."""
    start, context_name, success_events, _ = _SITES[site]
    outcome = _Outcome("success")
    connection = _Connection(outcome)

    task = await _settle(start(_driver(driver_type, connection), connection), outcome)

    assert task.exception() is None
    assert getattr(connection, f"{context_name}_context").exit_errors == [None]
    assert connection.events == success_events


@pytest.mark.parametrize("driver_type", _DRIVER_TYPES)
@pytest.mark.parametrize("site", list(_SITES))
async def test_block_error_delivered_through_a_future_propagates_after_exit(driver_type: Any, site: str) -> None:
    """An error raised while the block awaits reaches the context exit once and then the caller."""
    start, context_name, _, failure_events = _SITES[site]
    outcome = _Outcome("error")
    connection = _Connection(outcome)

    task = await _settle(start(_driver(driver_type, connection), connection), outcome)

    raised = task.exception()
    if site == "stack-pipeline":
        assert isinstance(raised, StackExecutionError)
        assert raised.__cause__ is outcome.error
    else:
        assert raised is outcome.error
    assert getattr(connection, f"{context_name}_context").exit_errors == [raised]
    assert connection.events == failure_events


@pytest.mark.parametrize("driver_type", _DRIVER_TYPES)
@pytest.mark.parametrize("site", list(_SITES))
async def test_block_cancellation_propagates_after_exit(driver_type: Any, site: str) -> None:
    """Cancelling the block runs the context exit once and leaves the task cancelled."""
    start, context_name, _, failure_events = _SITES[site]
    outcome = _Outcome("cancel")
    connection = _Connection(outcome)

    task = await _settle(start(_driver(driver_type, connection), connection), outcome)

    assert task.cancelled()
    exit_errors = getattr(connection, f"{context_name}_context").exit_errors
    assert [type(error) for error in exit_errors] == [asyncio.CancelledError]
    assert connection.events == [event for event in failure_events if event != "rollback"]


@pytest.mark.parametrize("driver_type", _DRIVER_TYPES)
async def test_copy_to_returns_rows_read_before_the_context_exits(driver_type: Any) -> None:
    """COPY TO STDOUT output is collected inside the block and returned after the exit."""
    outcome = _Outcome("success")
    connection = _Connection(outcome)

    result = await _copy_to(_driver(driver_type, connection), connection)

    assert result.get_data() == [{"copy_output": "1\talice\n2\tbob\n"}]
    assert result.rows_affected == 1


@pytest.mark.parametrize("driver_type", _DRIVER_TYPES)
async def test_stack_pipeline_closes_cursors_in_reverse_order_before_the_pipeline(driver_type: Any) -> None:
    """Pipeline cursors close last-opened first, then the pipeline exits, then the transaction commits."""
    outcome = _Outcome("success")
    connection = _Connection(outcome)
    stack = (
        StatementStack()
        .push_execute("INSERT INTO users (id) VALUES (:id)", id=1)
        .push_execute("INSERT INTO users (id) VALUES (:id)", id=2)
    )

    results = await _driver(driver_type, connection).execute_stack(stack)

    assert [result.rows_affected for result in results] == [1, 1]
    assert connection.events == ["cursor-1", "cursor-0", "pipeline", "commit"]
