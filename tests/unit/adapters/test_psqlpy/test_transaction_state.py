"""psqlpy transaction state follows begin/commit/rollback."""

from types import SimpleNamespace
from typing import Any, cast

import pytest

from sqlspec.adapters.psqlpy._typing import PsqlpyDatabaseError
from sqlspec.adapters.psqlpy.config import PsqlpyConfig
from sqlspec.adapters.psqlpy.core import _DML_COUNT_COLUMN, PsqlpyStreamSource
from sqlspec.adapters.psqlpy.driver import PsqlpyDriver
from sqlspec.core import StatementStack
from sqlspec.exceptions import SQLSpecError, StackExecutionError

pytestmark = pytest.mark.anyio


class _FakeConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    async def execute(self, sql: str, *args: Any, **kwargs: Any) -> None:
        self.statements.append(sql)

    def in_transaction(self) -> Any:
        msg = "in_transaction() is not a reliable transaction predicate"
        raise AssertionError(msg)


async def test_connection_in_transaction_tracks_begin_commit_rollback() -> None:
    connection = _FakeConnection()
    driver = PsqlpyDriver(cast("Any", connection))
    assert driver._connection_in_transaction() is False

    await driver.begin()
    assert driver._connection_in_transaction() is True
    await driver.commit()
    assert driver._connection_in_transaction() is False

    await driver.begin()
    assert driver._connection_in_transaction() is True
    await driver.rollback()
    assert driver._connection_in_transaction() is False
    assert connection.statements == ["BEGIN", "COMMIT", "BEGIN", "ROLLBACK"]


@pytest.mark.parametrize("operation", ["commit", "rollback"])
async def test_failed_transaction_end_clears_the_flag(operation: str) -> None:
    connection = _FakeConnection()
    driver = PsqlpyDriver(cast("Any", connection))
    await driver.begin()

    async def fail(sql: str, *args: Any, **kwargs: Any) -> None:
        raise PsqlpyDatabaseError(sql)

    connection.execute = fail  # type: ignore[method-assign]
    with pytest.raises(SQLSpecError):
        await getattr(driver, operation)()
    assert driver._connection_in_transaction() is False


class _FakeCursor:
    def __init__(self, owner: str) -> None:
        self.owner = owner
        self.closed = False

    async def start(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True


class _FakeTransaction:
    def __init__(self, record: "list[str]") -> None:
        self._record = record

    async def begin(self) -> None:
        self._record.append("begin")

    async def commit(self) -> None:
        self._record.append("commit")

    async def rollback(self) -> None:
        self._record.append("rollback")

    def cursor(self, *_args: Any, **_kwargs: Any) -> _FakeCursor:
        return _FakeCursor("transaction")


class _StreamingConnection(_FakeConnection):
    def __init__(self) -> None:
        super().__init__()
        self.transaction_calls: list[str] = []

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction(self.transaction_calls)

    def cursor(self, *_args: Any, **_kwargs: Any) -> _FakeCursor:
        return _FakeCursor("connection")


async def test_stream_inside_a_user_transaction_does_not_commit_it() -> None:
    """A stream must never end a transaction it did not open."""
    connection = _StreamingConnection()
    driver = PsqlpyDriver(cast("Any", connection))
    await driver.begin()

    source = PsqlpyStreamSource(driver, "SELECT 1", None, 10)
    await source.start()
    await source.close()

    assert connection.transaction_calls == []
    assert driver._connection_in_transaction() is True
    assert connection.statements == ["BEGIN"]


async def test_stream_outside_a_transaction_manages_its_own() -> None:
    """With no caller transaction the stream still opens and commits its own."""
    connection = _StreamingConnection()
    driver = PsqlpyDriver(cast("Any", connection))

    source = PsqlpyStreamSource(driver, "SELECT 1", None, 10)
    await source.start()
    await source.close()

    assert connection.transaction_calls == ["begin", "commit"]


async def test_a_stream_transaction_is_not_offered_to_the_driver_as_its_own() -> None:
    """Claiming it would let the driver's own transaction block commit the stream away."""
    connection = _StreamingConnection()
    driver = PsqlpyDriver(cast("Any", connection))

    source = PsqlpyStreamSource(driver, "SELECT 1", None, 10)
    await source.start()
    try:
        assert driver._connection_in_transaction() is False
    finally:
        await source.close()


async def test_stream_error_close_rolls_back_only_its_own_transaction() -> None:
    """An errored stream rolls back the transaction it owns, not the caller's."""
    connection = _StreamingConnection()
    driver = PsqlpyDriver(cast("Any", connection))

    source = PsqlpyStreamSource(driver, "SELECT 1", None, 10)
    await source.start()
    await source.close(error=True)

    assert connection.transaction_calls == ["begin", "rollback"]
    assert driver._connection_in_transaction() is False


class _CopyConnection(_FakeConnection):
    """Records the native COPY call and fails loudly on a row-by-row fallback."""

    def __init__(self, json_columns: "tuple[str, ...]" = ()) -> None:
        super().__init__()
        self.copy_calls: list[tuple[str, list[Any], dict[str, Any]]] = []
        self._json_columns = json_columns

    async def fetch(self, _sql: str, _parameters: Any = None) -> Any:
        return SimpleNamespace(result=lambda: [{"column_name": name} for name in self._json_columns])

    async def copy_records_to_table(self, table_name: str, records: Any, **kwargs: Any) -> int:
        self.copy_calls.append((table_name, list(records), kwargs))
        return len(self.copy_calls)

    def execute_many(self, *_args: Any, **_kwargs: Any) -> None:
        msg = "load_from_arrow must not fall back to row-by-row inserts"
        raise AssertionError(msg)


async def test_load_from_arrow_uses_the_native_copy_path() -> None:
    """Arrow ingest must go through copy_records_to_table, never execute_many."""
    pa = pytest.importorskip("pyarrow")
    connection = _CopyConnection()
    config = PsqlpyConfig()
    driver = PsqlpyDriver(
        cast("Any", connection), driver_features={"storage_capabilities": config.storage_capabilities()}
    )
    table = pa.table({"id": [1, 2], "name": ["a", "b"]})

    await driver.load_from_arrow("analytics.events", table)

    assert len(connection.copy_calls) == 1
    table_name, records, kwargs = connection.copy_calls[0]
    assert table_name == "events"
    assert records == [(1, "a"), (2, "b")]
    assert kwargs == {"columns": ["id", "name"], "schema_name": "analytics"}


async def test_load_from_arrow_decodes_json_text_for_json_columns() -> None:
    """psqlpy binds by destination column type, so a JSON column needs an object."""
    pa = pytest.importorskip("pyarrow")
    connection = _CopyConnection(json_columns=("payload",))
    config = PsqlpyConfig()
    driver = PsqlpyDriver(
        cast("Any", connection), driver_features={"storage_capabilities": config.storage_capabilities()}
    )
    table = pa.table({"id": [1], "payload": ['{"name": "alpha"}'], "note": ['{"not": "json"}']})

    await driver.load_from_arrow("events", table)

    _table_name, records, _kwargs = connection.copy_calls[0]
    assert records == [(1, {"name": "alpha"}, '{"not": "json"}')]


class _PipelineTransaction:
    def __init__(
        self,
        pipeline_calls: "list[tuple[list[tuple[str, list[Any] | None]], bool]]",
        results: "list[Any]",
        error: "Exception | None" = None,
    ) -> None:
        self._pipeline_calls = pipeline_calls
        self._results = results
        self._error = error

    async def pipeline(self, queries: "list[tuple[str, list[Any] | None]]", prepared: bool = True) -> "list[Any]":
        self._pipeline_calls.append((queries, prepared))
        if self._error is not None:
            raise self._error
        return self._results


class _PipelineConnection(_FakeConnection):
    def __init__(self, results: "list[Any] | None" = None, error: "Exception | None" = None) -> None:
        super().__init__()
        self.pipeline_calls: list[tuple[list[tuple[str, list[Any] | None]], bool]] = []
        self.fetch_calls: list[tuple[str, Any]] = []
        self.execute_many_calls: list[tuple[str, Any]] = []
        self._results = results or []
        self._error = error

    def transaction(self) -> _PipelineTransaction:
        return _PipelineTransaction(self.pipeline_calls, self._results, self._error)

    async def fetch(self, sql: str, parameters: Any = None) -> Any:
        self.fetch_calls.append((sql, parameters))
        if _DML_COUNT_COLUMN in sql:
            return SimpleNamespace(result=lambda: [{_DML_COUNT_COLUMN: 1}])
        return SimpleNamespace(result=lambda: [{"id": 1}])

    async def execute_many(self, sql: str, parameters: Any) -> None:
        self.execute_many_calls.append((sql, parameters))


async def test_execute_stack_uses_native_transaction_pipeline() -> None:
    """Supported execute stacks should run through connection.transaction().pipeline."""
    connection = _PipelineConnection(
        results=[
            SimpleNamespace(result=lambda: [{_DML_COUNT_COLUMN: 2}]),
            SimpleNamespace(result=lambda: [{"id": 1, "name": "alpha"}]),
        ]
    )
    driver = PsqlpyDriver(cast("Any", connection))
    stack = (
        StatementStack()
        .push_execute("INSERT INTO items (name) VALUES ($1)", "alpha")
        .push_execute("SELECT id, name FROM items WHERE name = $1", "alpha")
    )

    results = await driver.execute_stack(stack)

    assert len(results) == 2
    assert results[0].rows_affected == 2
    assert results[1].result is not None
    assert results[1].result.get_data() == [{"id": 1, "name": "alpha"}]
    assert len(connection.pipeline_calls) == 1
    queries, prepared = connection.pipeline_calls[0]
    assert prepared is True
    assert len(queries) == 2
    assert _DML_COUNT_COLUMN in queries[0][0]
    assert queries[0][1] == ["alpha"]
    assert queries[1] == ("SELECT id, name FROM items WHERE name = $1", ["alpha"])
    assert connection.statements == ["BEGIN", "COMMIT"]


async def test_execute_stack_falls_back_when_continue_on_error_or_non_execute() -> None:
    """Stacks with continue_on_error or non-execute operations must fall back to sequential execution."""
    connection = _PipelineConnection()
    driver = PsqlpyDriver(cast("Any", connection))

    continue_stack = StatementStack().push_execute("SELECT 1")
    await driver.execute_stack(continue_stack, continue_on_error=True)
    assert connection.pipeline_calls == []
    assert len(connection.fetch_calls) == 1

    many_stack = StatementStack().push_execute_many("INSERT INTO items (name) VALUES ($1)", [("a",), ("b",)])
    await driver.execute_stack(many_stack)
    assert connection.pipeline_calls == []
    assert len(connection.execute_many_calls) == 1


async def test_execute_stack_falls_back_when_native_stack_disabled() -> None:
    """Native stack disablement must bypass connection.transaction().pipeline."""
    connection = _PipelineConnection()
    driver = PsqlpyDriver(cast("Any", connection), driver_features={"stack_native_disabled": True})
    stack = StatementStack().push_execute("SELECT 1")

    await driver.execute_stack(stack)

    assert connection.pipeline_calls == []
    assert len(connection.fetch_calls) == 1


async def test_execute_stack_native_pipeline_error_rolls_back_and_wraps() -> None:
    """Pipeline failures should roll back owned transactions and raise StackExecutionError."""
    connection = _PipelineConnection(error=PsqlpyDatabaseError("unique constraint violation"))
    driver = PsqlpyDriver(cast("Any", connection))
    stack = StatementStack().push_execute("INSERT INTO items (name) VALUES ($1)", "dup")

    with pytest.raises(StackExecutionError) as exc_info:
        await driver.execute_stack(stack)

    assert exc_info.value.native_pipeline is True
    assert connection.statements == ["BEGIN", "ROLLBACK"]
    assert driver._connection_in_transaction() is False
