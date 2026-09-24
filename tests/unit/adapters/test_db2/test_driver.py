"""Tests for the IBM Db2 sync and async drivers.

Every test runs in both driver modes through the ``db2_mode`` fixture.
"""

import pytest

from sqlspec.adapters.db2.core import default_statement_config
from sqlspec.adapters.db2.data_dictionary import Db2AsyncDataDictionary, Db2SyncDataDictionary
from sqlspec.core import SQL
from sqlspec.driver import AsyncRowStream, SyncRowStream
from sqlspec.exceptions import SQLParsingError, TransactionError, UniqueViolationError
from tests.unit.adapters.test_db2._fakes import (
    DriverMode,
    FakeDb2Connection,
    FakeDb2Cursor,
    FakeDb2IntegrityError,
    db2_description,
    db2_error,
)

pytestmark = pytest.mark.anyio

UNSAFE_SAVEPOINT_NAMES = ["1; DROP TABLE users", "sp-1", "sp 1", "", '"sp"']
DUPLICATE_KEY_TEXT = (
    "One or more values in the INSERT statement, UPDATE statement, or foreign key update caused by a DELETE "
    'statement are not valid because the primary key, unique constraint or unique index identified by "1" '
    'constrains table "DB2INST1.USERS" from having duplicate values for the index key.'
)


@pytest.mark.parametrize(
    ("rows", "expected"),
    [([(1, "Ada"), (2, "Grace")], [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]), ([], [])],
    ids=["rows", "empty"],
)
async def test_execute_maps_db2_row_formats(
    db2_mode: DriverMode, rows: list[tuple[int, str]], expected: list[dict[str, int | str]]
) -> None:
    """Driver maps ibm_db_dbi tuple rows to dictionaries keyed by column name."""
    cursor = FakeDb2Cursor(rows=rows, description=[("id",), ("name",)])
    driver = db2_mode.driver(FakeDb2Connection(lambda: cursor))

    result = await db2_mode.call(driver.execute, "SELECT id, name FROM users")

    assert result.get_data() == expected
    assert result.column_names == ["id", "name"]
    assert len(cursor.executed) == 1
    assert cursor.closed is True


@pytest.mark.parametrize("bad_name", UNSAFE_SAVEPOINT_NAMES)
async def test_db2_savepoint_overrides_reject_unsafe_names(db2_mode: DriverMode, bad_name: str) -> None:
    """Savepoint operations must reject unsafe identifiers."""
    driver = db2_mode.driver(FakeDb2Connection())

    with pytest.raises(TransactionError):
        await db2_mode.call(driver.create_savepoint, bad_name)
    with pytest.raises(TransactionError):
        await db2_mode.call(driver.release_savepoint, bad_name)
    with pytest.raises(TransactionError):
        await db2_mode.call(driver.rollback_to_savepoint, bad_name)


async def test_db2_savepoint_overrides_accept_valid_name(db2_mode: DriverMode) -> None:
    """Valid savepoint names format proper Db2 savepoint statements."""
    cursor = FakeDb2Cursor()
    driver = db2_mode.driver(FakeDb2Connection(lambda: cursor))

    await db2_mode.call(driver.create_savepoint, "sp1")
    await db2_mode.call(driver.release_savepoint, "sp1")
    await db2_mode.call(driver.rollback_to_savepoint, "sp1")

    assert [call[0] for call in cursor.executed] == [
        "SAVEPOINT sp1 ON ROLLBACK RETAIN CURSORS",
        "RELEASE SAVEPOINT sp1",
        "ROLLBACK TO SAVEPOINT sp1",
    ]


async def test_dispatch_execute_select_compiles_and_collects_rows(db2_mode: DriverMode) -> None:
    """SELECT statement execution compiles with qmark positional style and populates SQLResult."""
    cursor = FakeDb2Cursor(rows=[(1, "Ada")], description=[("id",), ("name",)])
    driver = db2_mode.driver(FakeDb2Connection(lambda: cursor), statement_config=default_statement_config)
    statement = SQL("SELECT id, name FROM users WHERE id = ?", 1, statement_config=default_statement_config)

    result = await db2_mode.call(driver.dispatch_execute, db2_mode.cursor(cursor), statement)

    assert cursor.executed == [("SELECT id, name FROM users WHERE id = ?", (1,))]
    assert result.selected_data == [(1, "Ada")]
    assert result.column_names == ["id", "name"]
    assert result.data_row_count == 1


async def test_dispatch_execute_dml_reports_rowcount(db2_mode: DriverMode) -> None:
    """DML execution reports the cursor's rowcount."""
    cursor = FakeDb2Cursor(rowcount=3)
    driver = db2_mode.driver(FakeDb2Connection(lambda: cursor))

    result = await db2_mode.call(driver.execute, "UPDATE users SET name = ? WHERE id > ?", ("x", 1))

    assert result.rows_affected == 3
    assert cursor.executed == [("UPDATE users SET name = ? WHERE id > ?", ("x", 1))]


async def test_dispatch_execute_many_uses_executemany_and_rowcount(db2_mode: DriverMode) -> None:
    """execute_many delegates batch parameter sequences to cursor.executemany."""
    cursor = FakeDb2Cursor(rowcount=2)
    driver = db2_mode.driver(FakeDb2Connection(lambda: cursor), statement_config=default_statement_config)
    statement = SQL(
        "INSERT INTO users (id) VALUES (?)", [(1,), (2,)], statement_config=default_statement_config, is_many=True
    )

    result = await db2_mode.call(driver.dispatch_execute_many, db2_mode.cursor(cursor), statement)

    assert len(cursor.executed) == 1
    assert cursor.executed[0][0] == "INSERT INTO users (id) VALUES (?)"
    assert list(cursor.executed[0][1]) == [(1,), (2,)]
    assert result.rowcount_override == 2


async def test_dispatch_execute_script(db2_mode: DriverMode) -> None:
    """dispatch_execute_script splits and executes multi-statement scripts."""
    cursor = FakeDb2Cursor()
    driver = db2_mode.driver(FakeDb2Connection(lambda: cursor), statement_config=default_statement_config)
    statement = SQL(
        "CREATE TABLE t1 (id INT); CREATE TABLE t2 (id INT);", statement_config=default_statement_config, is_script=True
    )

    result = await db2_mode.call(driver.dispatch_execute_script, db2_mode.cursor(cursor), statement)

    assert result.is_script_result is True
    assert [call[0] for call in cursor.executed] == ["CREATE TABLE t1 (id INT)", "CREATE TABLE t2 (id INT)"]


async def test_dispatch_select_stream(db2_mode: DriverMode) -> None:
    """dispatch_select_stream reads rows in fetchmany chunks and closes the cursor."""
    rows = [(1, "Ada"), (2, "Grace"), (3, "Margaret")]
    cursor = FakeDb2Cursor(rows=rows, description=[("id",), ("name",)])
    driver = db2_mode.driver(FakeDb2Connection(lambda: cursor), statement_config=default_statement_config)
    statement = SQL("SELECT id, name FROM users", statement_config=default_statement_config)

    stream = driver.dispatch_select_stream(statement, chunk_size=2)
    assert isinstance(stream, AsyncRowStream if db2_mode.is_async else SyncRowStream)

    result_rows = await db2_mode.collect(stream)

    assert result_rows == [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}, {"id": 3, "name": "Margaret"}]
    assert cursor.closed is True


async def test_dispatch_select_stream_skips_non_queries(db2_mode: DriverMode) -> None:
    """Statements that return no rows get no native stream."""
    driver = db2_mode.driver(FakeDb2Connection(), statement_config=default_statement_config)
    statement = SQL("DELETE FROM users", statement_config=default_statement_config)

    assert driver.dispatch_select_stream(statement, chunk_size=2) is None


async def test_select_stream_maps_errors_and_closes_cursor(db2_mode: DriverMode) -> None:
    """A failing streamed query raises the mapped error and closes its cursor."""
    cursor = FakeDb2Cursor(error=db2_error(-204, "42704", '"APP.MISSING" is an undefined name.'))
    driver = db2_mode.driver(FakeDb2Connection(lambda: cursor))

    with pytest.raises(SQLParsingError, match="undefined name"):
        await db2_mode.collect(driver.select_stream("SELECT id FROM missing", chunk_size=2))

    assert cursor.closed is True


async def test_exception_handler_maps_db2_error(db2_mode: DriverMode) -> None:
    """The exception handler translates Db2 driver errors into mapped SQLSpecError."""
    handler = db2_mode.exception_handler()
    fake_err = db2_error(-803, "23505", DUPLICATE_KEY_TEXT, cls=FakeDb2IntegrityError)

    async with db2_mode.enter(handler):
        raise fake_err

    assert isinstance(handler.pending_exception, UniqueViolationError)


async def test_driver_execute_raises_mapped_exception(db2_mode: DriverMode) -> None:
    """driver.execute translates database errors into mapped SQLSpecError."""
    cursor = FakeDb2Cursor(error=db2_error(-803, "23505", DUPLICATE_KEY_TEXT, cls=FakeDb2IntegrityError))
    driver = db2_mode.driver(FakeDb2Connection(lambda: cursor))

    with pytest.raises(UniqueViolationError):
        await db2_mode.call(driver.execute, "INSERT INTO users VALUES (1)")


async def test_select_lowercases_implicit_uppercase_columns(db2_mode: DriverMode) -> None:
    """Names Db2 folded to uppercase surface as lowercase keys; quoted mixed case is kept."""
    cursor = FakeDb2Cursor(rows=[("S", 1, 2)], description=db2_description("schema_name", '"MixedCase"', "col_1"))
    driver = db2_mode.driver(FakeDb2Connection(lambda: cursor))

    result = await db2_mode.call(driver.execute, 'SELECT schema_name, "MixedCase", col_1 FROM t')

    assert result.column_names == ["schema_name", "MixedCase", "col_1"]
    assert result.get_data() == [{"schema_name": "S", "MixedCase": 1, "col_1": 2}]


async def test_lowercase_column_names_can_be_disabled(db2_mode: DriverMode) -> None:
    """Disabling the feature keeps the names exactly as Db2 reports them."""
    cursor = FakeDb2Cursor(rows=[("S",)], description=db2_description("schema_name"))
    driver = db2_mode.driver(
        FakeDb2Connection(lambda: cursor), driver_features={"enable_lowercase_column_names": False}
    )

    assert await db2_mode.call(driver.select, "SELECT schema_name FROM t") == [{"SCHEMA_NAME": "S"}]


async def test_select_stream_lowercases_implicit_uppercase_columns(db2_mode: DriverMode) -> None:
    """Streamed rows use the same lowercase keys as eager results."""
    cursor = FakeDb2Cursor(rows=[(1, "Ada"), (2, "Grace")], description=db2_description("id", "name"))
    driver = db2_mode.driver(FakeDb2Connection(lambda: cursor))

    rows = await db2_mode.collect(driver.select_stream("SELECT id, name FROM users", chunk_size=1))

    assert rows == [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]


async def test_repeated_select_keeps_lowercase_columns(db2_mode: DriverMode) -> None:
    """Re-executing a cached query returns the same lowercase keys and rowcounts as the first execution."""
    driver = db2_mode.driver(
        FakeDb2Connection(lambda: FakeDb2Cursor(rows=[(1,)], description=db2_description("schema_name"), rowcount=4))
    )

    first = await db2_mode.call(driver.execute, "SELECT schema_name FROM t WHERE id = ?", (1,))
    second = await db2_mode.call(driver.execute, "SELECT schema_name FROM t WHERE id = ?", (1,))
    first_update = await db2_mode.call(driver.execute, "UPDATE t SET a = ? WHERE id = ?", (1, 2))
    second_update = await db2_mode.call(driver.execute, "UPDATE t SET a = ? WHERE id = ?", (1, 2))

    assert first.get_data() == [{"schema_name": 1}]
    assert second.get_data() == [{"schema_name": 1}]
    assert (first_update.rows_affected, second_update.rows_affected) == (4, 4)


async def test_driver_exposes_matching_data_dictionary(db2_mode: DriverMode) -> None:
    """Each driver exposes the data dictionary of its own mode, created once."""
    driver = db2_mode.driver(FakeDb2Connection())

    dictionary = driver.data_dictionary

    assert isinstance(dictionary, Db2AsyncDataDictionary if db2_mode.is_async else Db2SyncDataDictionary)
    assert driver.data_dictionary is dictionary
