"""pymssql driver tests."""

from typing import cast

import pytest

from sqlspec import StatementStack
from sqlspec.adapters.pymssql._typing import PymssqlConnection, PymssqlRawCursor
from sqlspec.core import SQL
from sqlspec.exceptions import SQLSpecError, StackExecutionError, TransactionError, UniqueViolationError
from tests.unit.adapters.test_pymssql._fakes import (
    FakeConnection,
    FakeCursor,
    FakePymssqlIntegrityError,
    FakePymssqlModule,
)

UNSAFE_SAVEPOINT_NAMES = ["1; DROP TABLE users", "sp-1", "sp 1", "", '"sp"']


@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        (
            [{"name": "Ada", "id": 1}, {"name": "Grace", "id": 2}],
            [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}],
        ),
        ([(1, "Ada"), (2, "Grace")], [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]),
        ([], []),
    ],
    ids=["dict", "tuple", "empty"],
)
def test_execute_maps_pymssql_row_formats(
    rows: list[tuple[int, str] | dict[str, int | str]], expected: list[dict[str, int | str]]
) -> None:
    from sqlspec.adapters.pymssql.driver import PymssqlDriver

    cursor = FakeCursor(rows=rows, description=[("id",), ("name",)])
    driver = PymssqlDriver(cast("PymssqlConnection", FakeConnection(cursor)))

    result = driver.execute("SELECT id, name FROM dbo.users")

    assert result.get_data() == expected
    assert result.column_names == ["id", "name"]
    assert len(cursor.calls) == 1


@pytest.mark.parametrize("bad_name", UNSAFE_SAVEPOINT_NAMES)
def test_pymssql_savepoint_overrides_reject_unsafe_names(bad_name: str) -> None:
    """The T-SQL savepoint overrides must reject unsafe identifiers before interpolation."""
    from sqlspec.adapters.pymssql.driver import PymssqlDriver

    driver = PymssqlDriver(cast("PymssqlConnection", FakeConnection()))

    with pytest.raises(TransactionError):
        driver.create_savepoint(bad_name)
    with pytest.raises(TransactionError):
        driver.release_savepoint(bad_name)
    with pytest.raises(TransactionError):
        driver.rollback_to_savepoint(bad_name)


def test_pymssql_savepoint_overrides_accept_valid_name() -> None:
    """A safe savepoint name should pass validation and reach the underlying execute path."""
    from sqlspec.adapters.pymssql.driver import PymssqlDriver

    cursor = FakeCursor()
    connection = FakeConnection(cursor)
    driver = PymssqlDriver(cast("PymssqlConnection", connection))

    driver.create_savepoint("sp1")
    driver.rollback_to_savepoint("sp1")

    executed_sql = [call[0] for call in cursor.calls]
    assert "SAVE TRANSACTION sp1" in executed_sql
    assert "ROLLBACK TRANSACTION sp1" in executed_sql


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


@pytest.mark.parametrize("finish", ["commit", "rollback"])
def test_autocommit_transaction_is_ended_with_tsql(finish: str) -> None:
    """pymssql ignores commit() and rollback() under autocommit, so the driver ends its own transaction."""
    from sqlspec.adapters.pymssql.driver import PymssqlDriver

    cursor = FakeCursor()
    connection = FakeConnection(cursor)
    driver = PymssqlDriver(cast("PymssqlConnection", connection))

    driver.begin()
    getattr(driver, finish)()

    statement = "COMMIT TRANSACTION" if finish == "commit" else "ROLLBACK TRANSACTION"
    assert cursor.calls == [("BEGIN TRANSACTION", None), (f"IF @@TRANCOUNT > 0 {statement}", None)]
    assert driver._connection_in_transaction() is False


@pytest.mark.parametrize("finish", ["commit", "rollback"])
def test_non_autocommit_transaction_uses_connection_boundaries(finish: str) -> None:
    """Without autocommit, pymssql's connection commit() and rollback() end the open transaction."""
    from sqlspec.adapters.pymssql.driver import PymssqlDriver

    cursor = FakeCursor()
    connection = FakeConnection(cursor)
    connection.autocommit(False)
    driver = PymssqlDriver(cast("PymssqlConnection", connection))

    driver.begin()
    getattr(driver, finish)()

    assert cursor.calls == []
    assert (connection.commits, connection.rollbacks) == ((1, 0) if finish == "commit" else (0, 1))


def test_begin_reuses_the_open_transaction_without_autocommit() -> None:
    """A connection with autocommit disabled already holds a transaction, so begin issues no SQL."""
    from sqlspec.adapters.pymssql.driver import PymssqlDriver

    cursor = FakeCursor()
    connection = FakeConnection(cursor)
    connection.autocommit(False)
    driver = PymssqlDriver(cast("PymssqlConnection", connection))

    driver.begin()

    assert cursor.calls == []
    assert driver._connection_in_transaction() is True
    driver.commit()
    assert driver._connection_in_transaction() is False


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

    assert rows == [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}, {"id": 3, "name": "Linus"}]
    assert cursor.calls == [("SELECT id, name FROM dbo.users", ())]
    assert cursor.fetchmany_sizes == [2, 2, 2]
    assert cursor.closed is True


@pytest.mark.parametrize("finish", ["commit", "rollback"])
def test_connection_in_transaction_tracks_successful_boundaries(finish: str) -> None:
    from sqlspec.adapters.pymssql.driver import PymssqlDriver

    driver = PymssqlDriver(cast("PymssqlConnection", FakeConnection()))
    assert driver._connection_in_transaction() is False
    driver.begin()
    assert driver._connection_in_transaction() is True
    getattr(driver, finish)()
    assert driver._connection_in_transaction() is False


@pytest.mark.parametrize("operation", ["begin", "commit", "rollback"])
def test_failed_transaction_boundary_preserves_state(operation: str, monkeypatch: pytest.MonkeyPatch) -> None:
    import sqlspec.adapters.pymssql.driver as driver_module

    connection = FakeConnection()
    driver = driver_module.PymssqlDriver(cast("PymssqlConnection", connection))
    if operation != "begin":
        driver.begin()
    failure = FakePymssqlIntegrityError("boundary failed")

    def fail(*_args: object) -> None:
        raise failure

    monkeypatch.setattr(driver_module, "pymssql", FakePymssqlModule())
    monkeypatch.setattr(connection.cursor_obj, "execute", fail)
    with pytest.raises(SQLSpecError) as caught:
        getattr(driver, operation)()
    assert caught.value.__cause__ is failure
    assert driver._connection_in_transaction() is (operation != "begin")


@pytest.mark.parametrize("fails", [False, True])
def test_execute_stack_preserves_caller_transaction(fails: bool, monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.adapters.pymssql.driver import PymssqlDriver

    connection = FakeConnection(FakeCursor(rowcount=1))
    driver = PymssqlDriver(cast("PymssqlConnection", connection))
    driver.begin()
    stack = StatementStack().push_execute("INSERT INTO users (id) VALUES (1)")
    if fails:
        failure = RuntimeError("statement failed")
        execute = connection.cursor_obj.execute

        def fail(sql: str, parameters: object = None) -> None:
            execute(sql, parameters)
            if sql.startswith("INSERT"):
                raise failure

        monkeypatch.setattr(connection.cursor_obj, "execute", fail)
        with pytest.raises(StackExecutionError) as caught:
            driver.execute_stack(stack)
        assert caught.value.__cause__ is failure
    else:
        result = driver.execute_stack(stack)
        assert len(result) == 1
        assert result[0].rows_affected == 1
    assert not any("COMMIT" in sql or "ROLLBACK" in sql for sql, _ in connection.cursor_obj.calls)
    assert driver._connection_in_transaction() is True
    assert sum(sql == "BEGIN TRANSACTION" for sql, _ in connection.cursor_obj.calls) == 1
    driver.rollback()
    assert connection.cursor_obj.calls[-1] == ("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION", None)
