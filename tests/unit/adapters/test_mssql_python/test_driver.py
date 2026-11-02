"""Unit tests for mssql_python driver wiring."""

from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from mssql_python import Error as MssqlPythonError

from sqlspec.adapters.mssql_python.core import default_statement_config
from sqlspec.adapters.mssql_python.data_dictionary import MssqlPythonSyncDataDictionary
from sqlspec.adapters.mssql_python.driver import MssqlPythonDriver
from sqlspec.core import SQL, StatementStack
from sqlspec.exceptions import DeadlockError


class DummyCursor:
    """Minimal cursor for driver dispatch tests."""

    description: list[tuple[str]] | None = None
    rowcount = 1

    def __init__(self) -> None:
        self.closed = False
        self.executed: list[tuple[str, Any]] = []
        self.rows: list[Any] = []

    def execute(self, sql: str, parameters: Any = None) -> None:
        self.executed.append((sql, parameters))

    def executemany(self, sql: str, parameters: Any = None) -> None:
        self.executed.append((sql, parameters))

    def fetchall(self) -> list[Any]:
        return list(self.rows)

    def fetchmany(self, size: int = 1) -> list[Any]:
        result = self.rows[:size]
        self.rows = self.rows[size:]
        return result

    def close(self) -> None:
        self.closed = True


class DummyConnection:
    """Minimal connection for driver construction."""

    def __init__(self) -> None:
        self.cursor_obj = DummyCursor()
        self.commits = 0
        self.rollbacks = 0
        self.autocommit = False

    def cursor(self) -> DummyCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class _FakeRow:
    """Model an iterable mssql-python row that is not a tuple subclass."""

    def __init__(self, *values: Any) -> None:
        self.values = values

    def __iter__(self):
        return iter(self.values)


def test_sync_driver_lazily_initializes_data_dictionary() -> None:
    """The sync driver should expose the MSSQL data dictionary."""
    driver = MssqlPythonDriver(cast("Any", DummyConnection()))

    assert isinstance(driver.data_dictionary, MssqlPythonSyncDataDictionary)
    assert driver.data_dictionary is driver.data_dictionary


def test_sync_driver_execute_stack_uses_dbapi_transaction_fallback() -> None:
    """Stack execution should use and commit the DBAPI transaction."""
    connection = DummyConnection()
    driver = MssqlPythonDriver(cast("Any", connection))
    stack = StatementStack().push_execute("UPDATE queue SET state = ? WHERE id = ?", "ready", 1)

    results = driver.execute_stack(stack)

    assert len(results) == 1
    assert connection.cursor_obj.executed == [("UPDATE queue SET state = ? WHERE id = ?", ["ready", 1])]
    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_collect_rows_materializes_mssql_python_rows_as_tuples() -> None:
    """The cache-hit collection hook must retain the declared tuple row format."""
    driver = MssqlPythonDriver(cast("Any", DummyConnection()))
    cursor = DummyCursor()
    cursor.description = [("id",), ("name",)]

    rows, column_names, row_count = driver.collect_rows(cast("Any", cursor), [_FakeRow(1, "Ada")])

    assert rows == [(1, "Ada")]
    assert type(rows[0]) is tuple
    assert column_names == ["id", "name"]
    assert row_count == 1


def test_dispatch_execute_many_falls_back_to_parameter_count_when_rowcount_negative() -> None:
    """dispatch_execute_many should fall back to len(parameters) when cursor.rowcount is -1."""
    from sqlspec.adapters.mssql_python.core import default_statement_config
    from sqlspec.core import SQL

    connection = DummyConnection()
    connection.cursor_obj.rowcount = -1
    driver = MssqlPythonDriver(cast("Any", connection), statement_config=default_statement_config)
    statement = SQL(
        "INSERT INTO dbo.users (id) VALUES (?)",
        [(1,), (2,), (3,)],
        statement_config=default_statement_config,
        is_many=True,
    )

    result = driver.dispatch_execute_many(cast("Any", connection.cursor_obj), statement)

    assert result.rowcount_override == 3
    assert result.is_many_result is True


def test_execute_cache_hit_collects_rows_and_driver_profile_is_registered() -> None:
    """_execute_cache_hit should normalize parameters, collect rows, and driver profile 'mssql_python' is registered."""
    from types import SimpleNamespace

    from sqlspec.core import get_driver_profile
    from sqlspec.core.statement import ProcessedState

    connection = DummyConnection()
    connection.cursor_obj.description = [("id",), ("name",)]
    connection.cursor_obj.rows = [_FakeRow(1, "Ada")]
    driver = MssqlPythonDriver(cast("Any", connection))
    cached = SimpleNamespace(
        compiled_sql="SELECT id, name FROM dbo.users WHERE id = ?",
        expression=None,
        operation_type="SELECT",
        operation_profile=SimpleNamespace(returns_rows=True, modifies_rows=False),
        input_named_parameters=(),
        processed_state=ProcessedState("", ()),
    )

    result = driver._execute_cache_hit("SELECT id, name FROM dbo.users WHERE id = ?", (1,), cast("Any", cached))

    assert result.get_data() == [{"id": 1, "name": "Ada"}]
    assert connection.cursor_obj.executed == [("SELECT id, name FROM dbo.users WHERE id = ?", (1,))]
    assert get_driver_profile("mssql_python") is not None


def test_dispatch_execute_fetches_rows_whenever_the_cursor_has_a_result_set() -> None:
    """EXEC returns rows the statement profile cannot predict, so the cursor description decides."""
    driver = MssqlPythonDriver(cast("Any", object()))
    cursor = DummyCursor()
    cursor.description = [("id",)]
    cursor.rows = [(1,), (2,)]
    statement = SQL("EXEC dbo.list_ids", statement_config=driver.statement_config)

    result = driver.dispatch_execute(cast("Any", cursor), statement)

    assert result.selected_data is not None
    assert len(result.selected_data) == 2


def test_dispatch_execute_script_slices_parameters_and_emits_telemetry() -> None:
    """Multi-statement scripts should slice parameters and emit statement events."""
    connection = DummyConnection()
    driver = MssqlPythonDriver(cast("Any", connection), statement_config=default_statement_config)
    obs = MagicMock()
    obs.has_statement_observers = True
    driver._observability = obs

    script = SQL(
        "INSERT INTO t (id) VALUES (?); INSERT INTO t (id) VALUES (?);",
        [1, 2],
        statement_config=default_statement_config,
        is_script=True,
    )
    result = driver.dispatch_execute_script(cast("Any", connection.cursor_obj), script)
    assert result.statement_count == 2
    assert result.successful_statements == 2
    assert obs.emit_statement_event.call_count == 2


def test_transaction_commit_deadlock_maps_to_deadlock_error() -> None:
    """A 1205 deadlock error during commit should map to DeadlockError."""
    connection = DummyConnection()

    def _fail_commit() -> None:
        raise MssqlPythonError("Transaction was deadlocked (1205)", "40001")

    connection.commit = _fail_commit
    driver = MssqlPythonDriver(cast("Any", connection))
    driver.begin()

    with pytest.raises(DeadlockError):
        driver.commit()


def test_stream_source_runs_inside_active_except_handler() -> None:
    """Stream source start and fetch_chunk execute cleanly inside an active except block."""
    cursor = DummyCursor()
    cursor.description = [("id",)]
    cursor.rows = [(1,), (2,)]
    connection = DummyConnection()
    connection.cursor_obj = cursor
    driver = MssqlPythonDriver(cast("Any", connection))

    try:
        raise ValueError("ambient exception")
    except ValueError:
        stream = driver.select_stream("SELECT id FROM t", chunk_size=1)
        assert stream is not None
        rows = list(stream)
        assert len(rows) == 2
