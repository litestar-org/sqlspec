"""Unit tests for Db2 exception translation and core helpers."""

import pytest

from sqlspec.adapters.db2.core import (
    build_insert_statement,
    collect_rows,
    create_mapped_exception,
    extract_sqlstate,
    format_identifier,
    normalize_execute_many_parameters,
    normalize_execute_parameters,
    resolve_column_names,
    resolve_many_rowcount,
    resolve_rowcount,
)
from sqlspec.adapters.db2.driver import Db2SyncDriver
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    ForeignKeyViolationError,
    NotNullViolationError,
    PermissionDeniedError,
    QueryTimeoutError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)
from tests.unit.adapters.test_db2._fakes import (
    DiagnosticAttributeError,
    FakeDb2Connection,
    FakeDb2Cursor,
    db2_description,
    db2_error,
)


@pytest.mark.parametrize(
    ("sqlstate", "expected_type"),
    [
        ("23505", UniqueViolationError),
        ("23503", ForeignKeyViolationError),
        ("23502", NotNullViolationError),
        ("23513", CheckViolationError),
        ("40001", DeadlockError),
        ("42501", PermissionDeniedError),
        ("08001", DatabaseConnectionError),
        ("08003", DatabaseConnectionError),
        ("08004", DatabaseConnectionError),
        ("57014", QueryTimeoutError),
        ("22001", DataError),
        ("22003", DataError),
        ("22007", DataError),
        ("22012", DataError),
    ],
)
def test_sqlstate_attribute_mapping(sqlstate: str, expected_type: type[SQLSpecError]) -> None:
    """Verify SQLSTATE attribute on exception maps to appropriate SQLSpec error."""
    exc = DiagnosticAttributeError("Simulated error", sqlstate=sqlstate)
    mapped = create_mapped_exception(exc)
    assert isinstance(mapped, expected_type)
    assert sqlstate in str(mapped)


@pytest.mark.parametrize(
    ("sqlstate_str", "expected_type"),
    [
        ("[IBM][CLI Driver][DB2/LINUXX8664] SQLSTATE=23505: duplicate key", UniqueViolationError),
        ("CLI0125E Error occurred. SQLSTATE: 40001 Deadlock", DeadlockError),
        ("Failed to connect. state=08001", DatabaseConnectionError),
        ("Execution canceled SQLSTATE=57014", QueryTimeoutError),
    ],
)
def test_sqlstate_embedded_in_message(sqlstate_str: str, expected_type: type[SQLSpecError]) -> None:
    """Verify SQLSTATE embedded in error message is detected and mapped."""
    exc = Exception(sqlstate_str)
    mapped = create_mapped_exception(exc)
    assert isinstance(mapped, expected_type)


@pytest.mark.parametrize(
    ("sqlcode", "expected_type"),
    [
        ("SQL0803N", UniqueViolationError),
        ("SQL0530N", ForeignKeyViolationError),
        ("SQL0407N", NotNullViolationError),
        ("SQL0545N", CheckViolationError),
        ("SQL0911N", DeadlockError),
        ("SQL0913N", DeadlockError),
        ("SQL0551N", PermissionDeniedError),
        ("SQL0552N", PermissionDeniedError),
        ("SQL0900N", DatabaseConnectionError),
        ("SQL1042C", DatabaseConnectionError),
        ("SQL30081N", DatabaseConnectionError),
        ("SQL0104N", SQLParsingError),
        ("SQL0204N", SQLParsingError),
        ("-803", UniqueViolationError),
    ],
)
def test_sqlcode_attribute_mapping(sqlcode: str, expected_type: type[SQLSpecError]) -> None:
    """A SQLCODE carried in an error attribute maps without a SQLSTATE."""
    exc = DiagnosticAttributeError("Simulated failure", error_code=sqlcode)
    mapped = create_mapped_exception(exc)
    assert isinstance(mapped, expected_type)
    assert "Db2 SQLCODE" in str(mapped)


def test_undefined_object_is_not_a_connection_error() -> None:
    """An undefined table whose name contains "connection" maps to a parsing error."""
    error = db2_error(-204, "42704", '"DB2INST1.CONNECTION_LOG" is an undefined name.')

    mapped = create_mapped_exception(error)

    assert type(mapped) is SQLParsingError
    assert "undefined object" in str(mapped)


@pytest.mark.parametrize(
    ("sqlcode", "sqlstate", "reason", "expected_type"),
    [
        (-911, "40001", 2, DeadlockError),
        (-911, "40001", 68, QueryTimeoutError),
        (-913, "57033", 2, DeadlockError),
        (-913, "57033", 68, QueryTimeoutError),
    ],
)
def test_lock_timeout_and_deadlock_split_by_reason_code(
    sqlcode: int, sqlstate: str, reason: int, expected_type: type[SQLSpecError]
) -> None:
    """Reason code 68 is a lock timeout; other SQL0911N/SQL0913N reasons are deadlocks."""
    error = db2_error(sqlcode, sqlstate, "The current transaction has been rolled back.", reason=reason)

    mapped = create_mapped_exception(error)

    assert type(mapped) is expected_type
    assert f"SQLSTATE {sqlstate}" in str(mapped)


def test_sqlcode_suffixes_resolve() -> None:
    """SQLCODEs resolve whatever their message suffix and from integer attributes."""
    message_error = Exception("[IBM][CLI Driver] SQL1042C  An unexpected system error occurred. SQLSTATE=58004")
    attribute_error = DiagnosticAttributeError("Simulated failure", error_code=-1042)

    assert isinstance(create_mapped_exception(message_error), DatabaseConnectionError)
    assert isinstance(create_mapped_exception(attribute_error), DatabaseConnectionError)


def test_check_violation() -> None:
    """SQLSTATE 23513 maps to a check constraint violation."""
    error = db2_error(
        -545,
        "23513",
        'The requested operation is not allowed because a row does not satisfy the check constraint "CK_AGE".',
    )

    assert type(create_mapped_exception(error)) is CheckViolationError


def test_unmapped_error_keeps_message() -> None:
    """An error with no known SQLSTATE or SQLCODE becomes a plain ``SQLSpecError`` with the original text."""
    error = db2_error(-4302, "38000", "Java stored procedure or user-defined function abnormally terminated.")

    mapped = create_mapped_exception(error)

    assert type(mapped) is SQLSpecError
    assert "SQL4302N" in str(mapped)


def test_handler_ignores_non_vendor_exceptions() -> None:
    """Errors that do not come from the Db2 driver pass through untranslated."""
    driver = Db2SyncDriver(FakeDb2Connection())

    with pytest.raises(ValueError, match="not a driver error"):
        with driver.handle_database_exceptions():
            raise ValueError("not a driver error")


def test_handler_maps_vendor_exceptions() -> None:
    """Driver errors are replaced by their mapped SQLSpec exception."""
    driver = Db2SyncDriver(FakeDb2Connection())
    handler = driver.handle_database_exceptions()

    with handler:
        raise db2_error(-803, "23505", "One or more values in the INSERT statement are not valid.")

    assert type(handler.pending_exception) is UniqueViolationError


def test_extract_sqlstate_reads_mapped_exception() -> None:
    """The SQLSTATE can be recovered from a mapped exception's message."""
    mapped = create_mapped_exception(db2_error(-204, "42704", '"APP.ADK_SESSIONS" is an undefined name.'))

    assert extract_sqlstate(mapped) == "42704"
    assert extract_sqlstate(RuntimeError("no diagnostics")) is None


def test_format_identifier() -> None:
    """Verify identifier quoting for Db2."""
    assert format_identifier("tbl") == '"tbl"'
    assert format_identifier("schema.tbl") == '"schema"."tbl"'
    assert format_identifier('"already_quoted"') == '"already_quoted"'

    with pytest.raises(SQLSpecError, match="must not be empty"):
        format_identifier("")


def test_build_insert_statement() -> None:
    """Verify generation of parameterized insert statement."""
    stmt = build_insert_statement("users", ["id", "name", "email"])
    assert stmt == 'INSERT INTO "users" ("id", "name", "email") VALUES (?, ?, ?)'


def test_normalize_parameters() -> None:
    """Verify parameter normalization."""
    assert normalize_execute_parameters(None) is None
    assert normalize_execute_parameters([1, 2, 3]) == (1, 2, 3)
    assert normalize_execute_parameters((1, 2)) == (1, 2)

    assert normalize_execute_many_parameters(None) is None
    assert normalize_execute_many_parameters([[1, "a"], [2, "b"]]) == ((1, "a"), (2, "b"))


def test_resolve_column_names() -> None:
    """Verify column name resolution from cursor description."""
    description = [("ID", 1, 10), ("Name", 2, 50)]
    assert resolve_column_names(description, lowercase=True) == ["id", "Name"]
    assert resolve_column_names(description, lowercase=False) == ["ID", "Name"]
    assert resolve_column_names(None, lowercase=True) == []

    cache: dict[int, tuple[object, list[str]]] = {}
    names = resolve_column_names(description, cache, lowercase=True)
    assert names == ["id", "Name"]
    assert resolve_column_names(description, cache, lowercase=True) is names


def test_resolve_rowcount() -> None:
    """Verify rowcount resolution from cursor."""

    class MockCursor:
        rowcount = 42

    assert resolve_rowcount(MockCursor()) == 42
    assert resolve_many_rowcount(MockCursor(), [(1,), (2,)]) == 42

    class NoRowcountCursor:
        rowcount = -1

    assert resolve_many_rowcount(NoRowcountCursor(), [(1,), (2,)], fallback_count=2) == 2


def test_collect_rows_reads_description_from_cursor() -> None:
    """Rows fetched from a cursor use its description, lowercasing implicit-uppercase names."""
    cursor = FakeDb2Cursor(rows=[(1, "a")], description=db2_description("id", '"Label"'))

    rows, column_names, row_format = collect_rows(cursor, lowercase=True)

    assert rows == [(1, "a")]
    assert column_names == ["id", "Label"]
    assert row_format == "tuple"
