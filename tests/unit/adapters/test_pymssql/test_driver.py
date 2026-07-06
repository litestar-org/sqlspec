"""pymssql driver tests."""

from typing import cast

import pytest

from sqlspec.adapters.pymssql._typing import PymssqlConnection, PymssqlRawCursor
from sqlspec.core import SQL
from sqlspec.exceptions import SQLSpecError, UniqueViolationError
from tests.unit.adapters.test_pymssql.conftest import (
    FakeConnection,
    FakeCursor,
    FakePymssqlIntegrityError,
    FakePymssqlModule,
)


def test_dispatch_execute_select_compiles_to_pyformat_and_collects_rows() -> None:
    """SELECT dispatch should execute pyformat SQL and return fetched rows."""
    from sqlspec.adapters.pymssql.core import default_statement_config
    from sqlspec.adapters.pymssql.driver import PymssqlDriver

    cursor = FakeCursor(rows=[(1, "Ada")], description=[("id",), ("name",)])
    driver = PymssqlDriver(cast("PymssqlConnection", FakeConnection(cursor)), statement_config=default_statement_config)
    statement = SQL("SELECT id, name FROM dbo.users WHERE id = ?", 1, statement_config=default_statement_config)

    result = driver.dispatch_execute(cast("PymssqlRawCursor", cursor), statement)

    assert cursor.calls == [("SELECT id, name FROM dbo.users WHERE id = %s", (1,))]
    assert result.selected_data == [(1, "Ada")]
    assert result.column_names == ["id", "name"]
    assert result.data_row_count == 1


def test_dispatch_execute_many_uses_executemany_and_rowcount() -> None:
    """execute_many dispatch should forward batch parameters to pymssql."""
    from sqlspec.adapters.pymssql.core import default_statement_config
    from sqlspec.adapters.pymssql.driver import PymssqlDriver

    cursor = FakeCursor(rowcount=2)
    driver = PymssqlDriver(cast("PymssqlConnection", FakeConnection(cursor)), statement_config=default_statement_config)
    statement = SQL(
        "INSERT INTO dbo.users (id) VALUES (?)", [(1,), (2,)], statement_config=default_statement_config, is_many=True
    )

    result = driver.dispatch_execute_many(cast("PymssqlRawCursor", cursor), statement)

    assert cursor.many_calls == [("INSERT INTO dbo.users (id) VALUES (%s)", [(1,), (2,)])]
    assert result.rowcount_override == 2
    assert result.is_many_result is True


def test_transaction_methods_use_tsql_begin_and_connection_commit_rollback() -> None:
    """Transaction operations should use pymssql-compatible calls."""
    from sqlspec.adapters.pymssql.driver import PymssqlDriver

    cursor = FakeCursor()
    connection = FakeConnection(cursor)
    driver = PymssqlDriver(cast("PymssqlConnection", connection))

    driver.begin()
    driver.commit()
    driver.rollback()

    assert cursor.calls == [("BEGIN TRANSACTION", None)]
    assert connection.commits == 1
    assert connection.rollbacks == 1


def test_exception_handler_maps_pymssql_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """pymssql exception handlers should surface mapped SQLSpec exceptions."""
    import sqlspec.adapters.pymssql.driver as driver_module
    from sqlspec.adapters.pymssql.driver import PymssqlExceptionHandler

    monkeypatch.setattr(driver_module, "pymssql", FakePymssqlModule())
    handler = PymssqlExceptionHandler()

    handled = handler._handle_exception(
        FakePymssqlIntegrityError, FakePymssqlIntegrityError("Violation of UNIQUE KEY constraint (2627)")
    )

    assert handled is True
    assert isinstance(handler.pending_exception, UniqueViolationError)


def test_commit_wraps_driver_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Commit failures should be wrapped in SQLSpecError."""
    import sqlspec.adapters.pymssql.driver as driver_module
    from sqlspec.adapters.pymssql.driver import PymssqlDriver

    class FailingConnection(FakeConnection):
        def commit(self) -> None:
            raise FakePymssqlIntegrityError("commit failed")

    monkeypatch.setattr(driver_module, "pymssql", FakePymssqlModule())
    driver = PymssqlDriver(cast("PymssqlConnection", FailingConnection()))

    with pytest.raises(SQLSpecError, match="Failed to commit SQL Server transaction"):
        driver.commit()


def test_collect_rows_returns_column_names() -> None:
    """The direct row collection hook should match SyncDriverAdapterBase expectations."""
    from sqlspec.adapters.pymssql.driver import PymssqlDriver

    cursor = FakeCursor(description=[("id",), ("name",)])
    driver = PymssqlDriver(cast("PymssqlConnection", FakeConnection(cursor)))

    rows, column_names, row_count = driver.collect_rows(cast("PymssqlRawCursor", cursor), [(1, "Ada")])

    assert rows == [(1, "Ada")]
    assert column_names == ["id", "name"]
    assert row_count == 1


def test_select_stream_uses_fetchmany_chunks() -> None:
    """The pymssql driver should stream rows with cursor.fetchmany()."""
    from sqlspec.adapters.pymssql.driver import PymssqlDriver

    cursor = FakeCursor(rows=[(1, "Ada"), (2, "Grace"), (3, "Linus")], description=[("id",), ("name",)])
    driver = PymssqlDriver(cast("PymssqlConnection", FakeConnection(cursor)))

    with driver.select_stream("SELECT id, name FROM dbo.users", native_only=True, chunk_size=2) as stream:
        rows = list(stream)

    assert rows == [
        {"id": 1, "name": "Ada"},
        {"id": 2, "name": "Grace"},
        {"id": 3, "name": "Linus"},
    ]
    assert cursor.calls == [("SELECT id, name FROM dbo.users", ())]
    assert cursor.fetchmany_sizes == [2, 2, 2]
    assert cursor.closed is True


def test_connection_in_transaction_is_false_without_supported_state() -> None:
    """pymssql does not expose a reliable transaction-state flag."""
    from sqlspec.adapters.pymssql.driver import PymssqlDriver

    assert PymssqlDriver(cast("PymssqlConnection", FakeConnection()))._connection_in_transaction() is False
