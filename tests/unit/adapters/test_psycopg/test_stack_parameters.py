"""Pipeline stack preparation hands psycopg the parameters each statement shape needs."""

import contextlib
import inspect
from collections.abc import AsyncIterator, Iterator
from types import SimpleNamespace
from typing import Any

import pytest

from sqlspec.adapters.cockroach_psycopg.driver import CockroachPsycopgAsyncDriver, CockroachPsycopgSyncDriver
from sqlspec.adapters.psycopg.driver import PsycopgAsyncDriver, PsycopgSyncDriver
from sqlspec.core.stack import StatementStack

_POSITIONAL_SQL = "INSERT INTO t (id) VALUES (%s)"

_STACK_CASES = [
    pytest.param(_POSITIONAL_SQL, ((1,),), {}, _POSITIONAL_SQL, [1], id="tuple"),
    pytest.param(_POSITIONAL_SQL, ([1],), {}, _POSITIONAL_SQL, [1], id="list"),
    pytest.param(_POSITIONAL_SQL, (1,), {}, _POSITIONAL_SQL, [1], id="scalar"),
    pytest.param(
        "INSERT INTO t (id, name) VALUES (%s, %s)",
        (1, "a"),
        {},
        "INSERT INTO t (id, name) VALUES (%s, %s)",
        [1, "a"],
        id="scalars",
    ),
    pytest.param("INSERT INTO t (id) VALUES (:id)", (), {"id": 1}, _POSITIONAL_SQL, [1], id="named"),
    pytest.param(
        "INSERT INTO t (id) VALUES (%(id)s)",
        (),
        {"id": 1},
        "INSERT INTO t (id) VALUES (%(id)s)",
        {"id": 1},
        id="named-pyformat",
    ),
    pytest.param("INSERT INTO t (id) VALUES (1)", (), {}, "INSERT INTO t (id) VALUES (1)", None, id="no-parameters"),
]

_DRIVER_TYPES = [PsycopgSyncDriver, PsycopgAsyncDriver, CockroachPsycopgSyncDriver, CockroachPsycopgAsyncDriver]


class _Cursor:
    description = None
    rowcount = 1
    statusmessage = "INSERT 0 1"

    def __init__(self, sent: "list[tuple[str, Any]]") -> None:
        self._sent = sent

    def execute(self, sql: Any, params: Any = None) -> None:
        self._sent.append((str(sql), params))

    def fetchall(self) -> "list[Any]":
        return []

    def close(self) -> None:
        return None


class _AsyncCursor(_Cursor):
    async def execute(self, sql: Any, params: Any = None) -> None:  # type: ignore[override]
        self._sent.append((str(sql), params))

    async def fetchall(self) -> "list[Any]":  # type: ignore[override]
        return []

    async def close(self) -> None:  # type: ignore[override]
        return None


class _RowCursor(_Cursor):
    """Cursor returning one row whose description builds its column list on every read, as psycopg does."""

    description_reads = 0

    @property
    def description(self) -> "list[SimpleNamespace]":  # type: ignore[override]
        self.description_reads += 1
        return [SimpleNamespace(name="id", type_code=23), SimpleNamespace(name="name", type_code=25)]

    def fetchall(self) -> "list[Any]":
        return [(1, "a")]


class _AsyncRowCursor(_RowCursor):
    async def execute(self, sql: Any, params: Any = None) -> None:  # type: ignore[override]
        self._sent.append((str(sql), params))

    async def fetchall(self) -> "list[Any]":  # type: ignore[override]
        return [(1, "a")]

    async def close(self) -> None:  # type: ignore[override]
        return None


class _SyncConnection:
    def __init__(self, cursor_type: "type[_Cursor]" = _Cursor) -> None:
        self.autocommit = False
        self.info = SimpleNamespace(transaction_status=0)
        self.sent: list[tuple[str, Any]] = []
        self.transaction_calls: list[str] = []
        self.cursors: list[Any] = []
        self._cursor_type = cursor_type

    def cursor(self) -> _Cursor:
        cursor = self._cursor_type(self.sent)
        self.cursors.append(cursor)
        return cursor

    @contextlib.contextmanager
    def pipeline(self) -> "Iterator[Any]":
        yield SimpleNamespace(sync=lambda: None)

    def commit(self) -> None:
        self.transaction_calls.append("commit")

    def rollback(self) -> None:
        self.transaction_calls.append("rollback")


class _AsyncPipeline:
    async def sync(self) -> None:
        return None


class _AsyncConnection:
    def __init__(self, cursor_type: "type[_Cursor]" = _AsyncCursor) -> None:
        self.autocommit = False
        self.info = SimpleNamespace(transaction_status=0)
        self.sent: list[tuple[str, Any]] = []
        self.transaction_calls: list[str] = []
        self.cursors: list[Any] = []
        self._cursor_type = cursor_type

    def cursor(self) -> _Cursor:
        cursor = self._cursor_type(self.sent)
        self.cursors.append(cursor)
        return cursor

    @contextlib.asynccontextmanager
    async def pipeline(self) -> "AsyncIterator[Any]":
        yield _AsyncPipeline()

    async def commit(self) -> None:
        self.transaction_calls.append("commit")

    async def rollback(self) -> None:
        self.transaction_calls.append("rollback")


def _driver(driver_type: Any, *, rows: bool = False) -> "tuple[Any, Any]":
    connection: Any
    if inspect.iscoroutinefunction(driver_type.execute_stack):
        connection = _AsyncConnection(_AsyncRowCursor) if rows else _AsyncConnection()
    else:
        connection = _SyncConnection(_RowCursor) if rows else _SyncConnection()
    return driver_type(connection), connection


def _psycopg_arguments(parameters: Any) -> Any:
    """Return the value psycopg receives: a list for sequences, the mapping itself, or None when omitted."""
    if not parameters:
        return None
    if isinstance(parameters, dict):
        return parameters
    assert isinstance(parameters, (list, tuple))
    return list(parameters)


@pytest.mark.parametrize("driver_type", _DRIVER_TYPES)
@pytest.mark.parametrize(("sql", "arguments", "keyword_arguments", "expected_sql", "expected_parameters"), _STACK_CASES)
def test_prepared_stack_operation_holds_parameters_psycopg_accepts(
    driver_type: Any,
    sql: str,
    arguments: "tuple[Any, ...]",
    keyword_arguments: "dict[str, Any]",
    expected_sql: str,
    expected_parameters: Any,
) -> None:
    """Sequence placeholders get a list or tuple, named placeholders a dict, and no placeholders nothing."""
    driver, _ = _driver(driver_type)
    stack = StatementStack().push_execute(sql, *arguments, **keyword_arguments)

    prepared = driver._prepare_pipeline_operations(stack) or []

    assert len(prepared) == 1
    assert prepared[0].operation_index == 0
    assert prepared[0].sql == expected_sql
    assert _psycopg_arguments(prepared[0].parameters) == expected_parameters


@pytest.mark.parametrize("driver_type", _DRIVER_TYPES)
@pytest.mark.parametrize(("sql", "arguments", "keyword_arguments", "expected_sql", "expected_parameters"), _STACK_CASES)
async def test_execute_stack_sends_prepared_parameters_through_the_pipeline(
    driver_type: Any,
    sql: str,
    arguments: "tuple[Any, ...]",
    keyword_arguments: "dict[str, Any]",
    expected_sql: str,
    expected_parameters: Any,
) -> None:
    """Every statement shape reaches the pipeline cursor and commits the stack transaction."""
    driver, connection = _driver(driver_type)
    stack = StatementStack().push_execute(sql, *arguments, **keyword_arguments)

    results = driver.execute_stack(stack)
    if inspect.isawaitable(results):
        results = await results

    assert [result.rows_affected for result in results] == [1]
    assert [(sent_sql, _psycopg_arguments(sent)) for sent_sql, sent in connection.sent] == [
        (expected_sql, expected_parameters)
    ]
    assert connection.transaction_calls == ["commit"]


@pytest.mark.parametrize("driver_type", _DRIVER_TYPES)
async def test_execute_stack_keeps_each_operation_with_its_own_parameters(driver_type: Any) -> None:
    """A stack mixing parameter shapes sends each statement with its own prepared value."""
    driver, connection = _driver(driver_type)
    stack = (
        StatementStack()
        .push_execute(_POSITIONAL_SQL, (1,))
        .push_execute("DELETE FROM t")
        .push_execute("UPDATE t SET name = :name WHERE id = :id", name="a", id=2)
        .push_execute("INSERT INTO t (id) VALUES (%(id)s)", id=3)
    )

    results = driver.execute_stack(stack)
    if inspect.isawaitable(results):
        results = await results

    assert len(results) == 4
    assert [(sent_sql, _psycopg_arguments(sent)) for sent_sql, sent in connection.sent] == [
        (_POSITIONAL_SQL, [1]),
        ("DELETE FROM t", None),
        ("UPDATE t SET name = %s WHERE id = %s", ["a", 2]),
        ("INSERT INTO t (id) VALUES (%(id)s)", {"id": 3}),
    ]


@pytest.mark.parametrize("driver_type", _DRIVER_TYPES)
async def test_execute_stack_select_resolves_columns_from_one_description_read(driver_type: Any) -> None:
    """A row-returning stack statement builds its result from a single cursor description."""
    driver, connection = _driver(driver_type, rows=True)
    stack = StatementStack().push_execute("SELECT id, name FROM t WHERE id = :id", id=1)

    results = driver.execute_stack(stack)
    if inspect.isawaitable(results):
        results = await results

    assert results[0].get_result().get_data() == [{"id": 1, "name": "a"}]
    assert [cursor.description_reads for cursor in connection.cursors] == [1]
