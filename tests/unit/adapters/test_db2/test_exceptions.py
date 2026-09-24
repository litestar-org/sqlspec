"""Unit tests for Db2 exception translation and core helpers."""

import pytest

from sqlspec.adapters.db2.core import (
    build_insert_statement,
    collect_rows,
    create_mapped_exception,
    format_identifier,
    normalize_execute_many_parameters,
    normalize_execute_parameters,
    resolve_column_names,
    resolve_many_rowcount,
    resolve_rowcount,
)
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    PermissionDeniedError,
    QueryTimeoutError,
    SQLSpecError,
    UniqueViolationError,
)
from tests.unit.adapters.test_db2._fakes import DiagnosticAttributeError, FakeDb2Cursor, db2_description


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
        ("SQL0551N", PermissionDeniedError),
        ("SQL0900N", DatabaseConnectionError),
        ("SQL1042C", DatabaseConnectionError),
        ("SQL30081N", DatabaseConnectionError),
    ],
)
def test_sqlcode_attribute_mapping(sqlcode: str, expected_type: type[SQLSpecError]) -> None:
    """Verify SQLCODE attribute or string token maps to appropriate error."""
    exc = DiagnosticAttributeError("Simulated failure", error_code=sqlcode)
    mapped = create_mapped_exception(exc)
    assert isinstance(mapped, expected_type)
    assert sqlcode in str(mapped)


def test_message_heuristic_fallbacks() -> None:
    """Verify message keywords map when neither SQLSTATE nor SQLCODE is present."""
    assert isinstance(create_mapped_exception(Exception("unique constraint failed")), UniqueViolationError)
    assert isinstance(create_mapped_exception(Exception("foreign key violated")), ForeignKeyViolationError)
    assert isinstance(create_mapped_exception(Exception("value cannot be null")), NotNullViolationError)
    assert isinstance(create_mapped_exception(Exception("deadlock detected on lock")), DeadlockError)
    assert isinstance(create_mapped_exception(Exception("connection refused by host")), DatabaseConnectionError)
    assert isinstance(create_mapped_exception(Exception("permission denied for user")), PermissionDeniedError)


def test_exception_class_hierarchy_fallbacks() -> None:
    """Verify fallback to standard DB-API error types."""

    class IntegrityErrorDb2(Exception):
        pass

    class OperationalErrorDb2(Exception):
        pass

    class DataErrorDb2(Exception):
        pass

    assert isinstance(create_mapped_exception(IntegrityErrorDb2("fail")), IntegrityError)
    assert isinstance(create_mapped_exception(OperationalErrorDb2("fail")), OperationalError)
    assert isinstance(create_mapped_exception(DataErrorDb2("fail")), DataError)
    assert isinstance(create_mapped_exception(Exception("unknown error")), SQLSpecError)


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
