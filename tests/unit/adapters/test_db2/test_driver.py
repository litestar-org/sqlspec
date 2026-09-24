"""Tests for IBM Db2 database driver."""

import pytest

from sqlspec.adapters.db2.core import default_statement_config
from sqlspec.adapters.db2.driver import Db2SyncDriver, Db2SyncExceptionHandler
from sqlspec.core import SQL
from sqlspec.exceptions import TransactionError, UniqueViolationError
from tests.unit.adapters.test_db2._fakes import (
    FakeDb2Connection,
    FakeDb2Cursor,
    FakeDb2IntegrityError,
    db2_description,
    db2_error,
)

UNSAFE_SAVEPOINT_NAMES = ["1; DROP TABLE users", "sp-1", "sp 1", "", '"sp"']
DUPLICATE_KEY_TEXT = (
    "One or more values in the INSERT statement, UPDATE statement, or foreign key update caused by a DELETE "
    'statement are not valid because the primary key, unique constraint or unique index identified by "1" '
    'constrains table "DB2INST1.USERS" from having duplicate values for the index key.'
)


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
    cursor = FakeDb2Cursor(rows=rows, description=[("id",), ("name",)])
    driver = Db2SyncDriver(FakeDb2Connection(lambda: cursor))

    result = driver.execute("SELECT id, name FROM users")

    assert result.get_data() == expected
    assert result.column_names == ["id", "name"]
    assert len(cursor.executed) == 1


@pytest.mark.parametrize("bad_name", UNSAFE_SAVEPOINT_NAMES)
def test_db2_savepoint_overrides_reject_unsafe_names(bad_name: str) -> None:
    """Savepoint operations must reject unsafe identifiers."""
    driver = Db2SyncDriver(FakeDb2Connection())

    with pytest.raises(TransactionError):
        driver.create_savepoint(bad_name)
    with pytest.raises(TransactionError):
        driver.release_savepoint(bad_name)
    with pytest.raises(TransactionError):
        driver.rollback_to_savepoint(bad_name)


def test_db2_savepoint_overrides_accept_valid_name() -> None:
    """Valid savepoint names format proper Db2 savepoint statements."""
    cursor = FakeDb2Cursor()
    connection = FakeDb2Connection(lambda: cursor)
    driver = Db2SyncDriver(connection)

    driver.create_savepoint("sp1")
    driver.release_savepoint("sp1")
    driver.rollback_to_savepoint("sp1")

    executed_sql = [call[0] for call in cursor.executed]
    assert "SAVEPOINT sp1 ON ROLLBACK RETAIN CURSORS" in executed_sql
    assert "RELEASE SAVEPOINT sp1" in executed_sql
    assert "ROLLBACK TO SAVEPOINT sp1" in executed_sql


def test_dispatch_execute_select_compiles_and_collects_rows() -> None:
    """SELECT statement execution compiles with qmark positional style and populates SQLResult."""
    cursor = FakeDb2Cursor(rows=[(1, "Ada")], description=[("id",), ("name",)])
    driver = Db2SyncDriver(FakeDb2Connection(lambda: cursor), statement_config=default_statement_config)
    statement = SQL("SELECT id, name FROM users WHERE id = ?", 1, statement_config=default_statement_config)

    result = driver.dispatch_execute(cursor, statement)

    assert len(cursor.executed) == 1
    assert cursor.executed[0][0] == "SELECT id, name FROM users WHERE id = ?"
    assert cursor.executed[0][1] == (1,)
    assert result.selected_data == [(1, "Ada")]
    assert result.column_names == ["id", "name"]
    assert result.data_row_count == 1


def test_dispatch_execute_many_uses_executemany_and_rowcount() -> None:
    """execute_many delegates batch parameter sequences to cursor.executemany."""
    cursor = FakeDb2Cursor(rowcount=2)
    driver = Db2SyncDriver(FakeDb2Connection(lambda: cursor), statement_config=default_statement_config)
    statement = SQL(
        "INSERT INTO users (id) VALUES (?)", [(1,), (2,)], statement_config=default_statement_config, is_many=True
    )

    result = driver.dispatch_execute_many(cursor, statement)

    assert len(cursor.executed) == 1
    assert cursor.executed[0][0] == "INSERT INTO users (id) VALUES (?)"
    assert list(cursor.executed[0][1]) == [(1,), (2,)]
    assert result.rowcount_override == 2


def test_dispatch_execute_script() -> None:
    """dispatch_execute_script splits and executes multi-statement scripts."""
    cursor = FakeDb2Cursor()
    driver = Db2SyncDriver(FakeDb2Connection(lambda: cursor), statement_config=default_statement_config)
    statement = SQL(
        "CREATE TABLE t1 (id INT); CREATE TABLE t2 (id INT);", statement_config=default_statement_config, is_script=True
    )

    result = driver.dispatch_execute_script(cursor, statement)

    assert result.is_script_result is True
    assert len(cursor.executed) == 2
    assert cursor.executed[0][0] == "CREATE TABLE t1 (id INT)"
    assert cursor.executed[1][0] == "CREATE TABLE t2 (id INT)"


def test_dispatch_select_stream() -> None:
    """dispatch_select_stream provides a row stream iterating rows through fetchmany."""
    rows = [(1, "Ada"), (2, "Grace"), (3, "Margaret")]
    cursor = FakeDb2Cursor(rows=rows, description=[("id",), ("name",)])
    driver = Db2SyncDriver(FakeDb2Connection(lambda: cursor), statement_config=default_statement_config)
    statement = SQL("SELECT id, name FROM users", statement_config=default_statement_config)

    stream = driver.dispatch_select_stream(statement, chunk_size=2)
    assert stream is not None

    with stream:
        result_rows = list(stream)

    assert len(result_rows) == 3
    assert result_rows == [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}, {"id": 3, "name": "Margaret"}]


def test_exception_handler_maps_db2_error() -> None:
    """Db2SyncExceptionHandler translates Db2 driver errors into mapped SQLSpecError."""
    handler = Db2SyncExceptionHandler()
    fake_err = db2_error(-803, "23505", DUPLICATE_KEY_TEXT, cls=FakeDb2IntegrityError)

    with handler:
        raise fake_err

    assert isinstance(handler.pending_exception, UniqueViolationError)


def test_driver_execute_raises_mapped_exception() -> None:
    """driver.execute translates database errors into mapped SQLSpecError."""
    cursor = FakeDb2Cursor(error=db2_error(-803, "23505", DUPLICATE_KEY_TEXT, cls=FakeDb2IntegrityError))
    driver = Db2SyncDriver(FakeDb2Connection(lambda: cursor))

    with pytest.raises(UniqueViolationError):
        driver.execute("INSERT INTO users VALUES (1)")


def test_select_to_arrow_conversion() -> None:
    """select_to_arrow returns Arrow Table via in-memory conversion."""
    pytest.importorskip("pyarrow")
    rows = [(1, "Ada"), (2, "Grace")]
    cursor = FakeDb2Cursor(rows=rows, description=[("id",), ("name",)])
    driver = Db2SyncDriver(FakeDb2Connection(lambda: cursor), statement_config=default_statement_config)

    arrow_result = driver.select_to_arrow("SELECT id, name FROM users")

    table = arrow_result.get_data()
    assert table.num_rows == 2
    assert table.column_names == ["id", "name"]


def test_select_lowercases_implicit_uppercase_columns() -> None:
    """Names Db2 folded to uppercase surface as lowercase keys; quoted mixed case is kept."""
    cursor = FakeDb2Cursor(rows=[("S", 1, 2)], description=db2_description("schema_name", '"MixedCase"', "col_1"))
    driver = Db2SyncDriver(FakeDb2Connection(lambda: cursor))

    result = driver.execute('SELECT schema_name, "MixedCase", col_1 FROM t')

    assert result.column_names == ["schema_name", "MixedCase", "col_1"]
    assert result.get_data() == [{"schema_name": "S", "MixedCase": 1, "col_1": 2}]


def test_lowercase_column_names_can_be_disabled() -> None:
    """Disabling the feature keeps the names exactly as Db2 reports them."""
    cursor = FakeDb2Cursor(rows=[("S",)], description=db2_description("schema_name"))
    driver = Db2SyncDriver(FakeDb2Connection(lambda: cursor), driver_features={"enable_lowercase_column_names": False})

    assert driver.select("SELECT schema_name FROM t") == [{"SCHEMA_NAME": "S"}]


def test_select_stream_lowercases_implicit_uppercase_columns() -> None:
    """Streamed rows use the same lowercase keys as eager results."""
    cursor = FakeDb2Cursor(rows=[(1, "Ada"), (2, "Grace")], description=db2_description("id", "name"))
    driver = Db2SyncDriver(FakeDb2Connection(lambda: cursor))

    with driver.select_stream("SELECT id, name FROM users", chunk_size=1) as stream:
        rows = list(stream)

    assert rows == [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]


def test_repeated_select_keeps_lowercase_columns() -> None:
    """Re-executing a cached query returns the same lowercase keys as the first execution."""
    driver = Db2SyncDriver(
        FakeDb2Connection(lambda: FakeDb2Cursor(rows=[(1,)], description=db2_description("schema_name")))
    )

    first = driver.execute("SELECT schema_name FROM t WHERE id = ?", (1,))
    second = driver.execute("SELECT schema_name FROM t WHERE id = ?", (1,))

    assert first.get_data() == [{"schema_name": 1}]
    assert second.get_data() == [{"schema_name": 1}]
