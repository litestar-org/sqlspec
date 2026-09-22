"""Tests for IBM Db2 database driver."""

from typing import Any

import pytest

from sqlspec.adapters.db2.core import default_statement_config
from sqlspec.adapters.db2.driver import Db2Driver, Db2ExceptionHandler
from sqlspec.core import SQL
from sqlspec.exceptions import TransactionError, UniqueViolationError

UNSAFE_SAVEPOINT_NAMES = ["1; DROP TABLE users", "sp-1", "sp 1", "", '"sp"']


class FakeCursor:
    """Fake Db2 cursor for driver testing."""

    def __init__(
        self,
        rows: list[Any] | None = None,
        description: list[tuple[str, ...]] | None = None,
        rowcount: int = 0,
    ) -> None:
        self.rows = list(rows) if rows is not None else []
        self.description = description
        self.rowcount = rowcount
        self.calls: list[tuple[str, Any]] = []
        self.many_calls: list[tuple[str, Any]] = []
        self.closed = False

    def execute(self, operation: str, parameters: Any = None) -> Any:
        self.calls.append((operation, parameters))
        return self

    def executemany(self, operation: str, seq_of_parameters: Any) -> Any:
        self.many_calls.append((operation, seq_of_parameters))
        return self

    def fetchall(self) -> list[Any]:
        return list(self.rows)

    def fetchmany(self, size: int = 1) -> list[Any]:
        chunk = self.rows[:size]
        self.rows = self.rows[size:]
        return chunk

    def fetchone(self) -> Any:
        return self.rows.pop(0) if self.rows else None

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    """Fake Db2 connection for driver testing."""

    def __init__(self, cursor: FakeCursor | None = None, autocommit_state: bool = False) -> None:
        self.cursor_instance = cursor or FakeCursor()
        self.autocommit_state = autocommit_state
        self.committed = False
        self.rolled_back = False

    def cursor(self) -> FakeCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


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
def test_execute_maps_db2_row_formats(
    rows: list[tuple[int, str] | dict[str, int | str]], expected: list[dict[str, int | str]]
) -> None:
    """Driver formats both dictionary and tuple rows into standardized dictionaries."""
    cursor = FakeCursor(rows=rows, description=[("id",), ("name",)])
    driver = Db2Driver(FakeConnection(cursor))

    result = driver.execute("SELECT id, name FROM users")

    assert result.get_data() == expected
    assert result.column_names == ["id", "name"]
    assert len(cursor.calls) == 1


@pytest.mark.parametrize("bad_name", UNSAFE_SAVEPOINT_NAMES)
def test_db2_savepoint_overrides_reject_unsafe_names(bad_name: str) -> None:
    """Savepoint operations must reject unsafe identifiers."""
    driver = Db2Driver(FakeConnection())

    with pytest.raises(TransactionError):
        driver.create_savepoint(bad_name)
    with pytest.raises(TransactionError):
        driver.release_savepoint(bad_name)
    with pytest.raises(TransactionError):
        driver.rollback_to_savepoint(bad_name)


def test_db2_savepoint_overrides_accept_valid_name() -> None:
    """Valid savepoint names format proper Db2 savepoint statements."""
    cursor = FakeCursor()
    connection = FakeConnection(cursor)
    driver = Db2Driver(connection)

    driver.create_savepoint("sp1")
    driver.release_savepoint("sp1")
    driver.rollback_to_savepoint("sp1")

    executed_sql = [call[0] for call in cursor.calls]
    assert "SAVEPOINT sp1 ON ROLLBACK RETAIN CURSORS" in executed_sql
    assert "RELEASE SAVEPOINT sp1" in executed_sql
    assert "ROLLBACK TO SAVEPOINT sp1" in executed_sql


def test_dispatch_execute_select_compiles_and_collects_rows() -> None:
    """SELECT statement execution compiles with qmark positional style and populates SQLResult."""
    cursor = FakeCursor(rows=[(1, "Ada")], description=[("id",), ("name",)])
    driver = Db2Driver(FakeConnection(cursor), statement_config=default_statement_config)
    statement = SQL("SELECT id, name FROM users WHERE id = ?", 1, statement_config=default_statement_config)

    result = driver.dispatch_execute(cursor, statement)

    assert len(cursor.calls) == 1
    assert cursor.calls[0][0] == "SELECT id, name FROM users WHERE id = ?"
    assert cursor.calls[0][1] == (1,)
    assert result.selected_data == [(1, "Ada")]
    assert result.column_names == ["id", "name"]
    assert result.data_row_count == 1


def test_dispatch_execute_many_uses_executemany_and_rowcount() -> None:
    """execute_many delegates batch parameter sequences to cursor.executemany."""
    cursor = FakeCursor(rowcount=2)
    driver = Db2Driver(FakeConnection(cursor), statement_config=default_statement_config)
    statement = SQL(
        "INSERT INTO users (id) VALUES (?)", [(1,), (2,)], statement_config=default_statement_config, is_many=True
    )

    result = driver.dispatch_execute_many(cursor, statement)

    assert len(cursor.many_calls) == 1
    assert cursor.many_calls[0][0] == "INSERT INTO users (id) VALUES (?)"
    assert list(cursor.many_calls[0][1]) == [(1,), (2,)]
    assert result.rowcount_override == 2


def test_dispatch_execute_script() -> None:
    """dispatch_execute_script splits and executes multi-statement scripts."""
    cursor = FakeCursor()
    driver = Db2Driver(FakeConnection(cursor), statement_config=default_statement_config)
    statement = SQL(
        "CREATE TABLE t1 (id INT); CREATE TABLE t2 (id INT);",
        statement_config=default_statement_config,
        is_script=True,
    )

    result = driver.dispatch_execute_script(cursor, statement)

    assert result.is_script_result is True
    assert len(cursor.calls) == 2
    assert cursor.calls[0][0] == "CREATE TABLE t1 (id INT)"
    assert cursor.calls[1][0] == "CREATE TABLE t2 (id INT)"


def test_begin_commit_rollback_lifecycle() -> None:
    """Driver properly manages transaction states and connection commit/rollback."""
    conn = FakeConnection(autocommit_state=True)
    driver = Db2Driver(conn)

    assert driver._connection_in_transaction() is False

    driver.begin()
    assert driver._connection_in_transaction() is True
    assert conn.autocommit_state is False

    driver.commit()
    assert driver._connection_in_transaction() is False
    assert conn.committed is True
    assert conn.autocommit_state is True

    driver.begin()
    assert driver._connection_in_transaction() is True

    driver.rollback()
    assert driver._connection_in_transaction() is False
    assert conn.rolled_back is True


def test_dispatch_select_stream() -> None:
    """dispatch_select_stream provides a row stream iterating rows through fetchmany."""
    rows = [(1, "Ada"), (2, "Grace"), (3, "Margaret")]
    cursor = FakeCursor(rows=rows, description=[("id",), ("name",)])
    driver = Db2Driver(FakeConnection(cursor), statement_config=default_statement_config)
    statement = SQL("SELECT id, name FROM users", statement_config=default_statement_config)

    stream = driver.dispatch_select_stream(statement, chunk_size=2)
    assert stream is not None

    with stream:
        result_rows = list(stream)

    assert len(result_rows) == 3
    assert result_rows == [
        {"id": 1, "name": "Ada"},
        {"id": 2, "name": "Grace"},
        {"id": 3, "name": "Margaret"},
    ]


def test_exception_handler_maps_db2_error() -> None:
    """Db2ExceptionHandler translates Db2 driver errors into mapped SQLSpecError."""
    handler = Db2ExceptionHandler()

    class FakeDb2Error(Exception):
        pass

    fake_err = FakeDb2Error("DB2 SQL Error: SQLCODE=-803, SQLSTATE=23505, SQL0803N")

    with handler:
        raise fake_err

    assert isinstance(handler.pending_exception, UniqueViolationError)


def test_driver_execute_raises_mapped_exception() -> None:
    """driver.execute translates database errors into mapped SQLSpecError."""
    class FakeDb2Error(Exception):
        pass

    cursor = FakeCursor()

    def fail(*args: Any, **kwargs: Any) -> None:
        raise FakeDb2Error("DB2 SQL Error: SQLCODE=-803, SQLSTATE=23505, SQL0803N")

    cursor.execute = fail
    driver = Db2Driver(FakeConnection(cursor))

    with pytest.raises(UniqueViolationError):
        driver.execute("INSERT INTO users VALUES (1)")


def test_select_to_arrow_conversion() -> None:
    """select_to_arrow returns Arrow Table via in-memory conversion."""
    pytest.importorskip("pyarrow")
    rows = [(1, "Ada"), (2, "Grace")]
    cursor = FakeCursor(rows=rows, description=[("id",), ("name",)])
    driver = Db2Driver(FakeConnection(cursor), statement_config=default_statement_config)

    arrow_result = driver.select_to_arrow("SELECT id, name FROM users")

    table = arrow_result.get_data()
    assert table.num_rows == 2
    assert table.column_names == ["id", "name"]
