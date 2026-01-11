"""Exception handling integration tests for sqlite adapter."""

import pytest

from sqlspec.adapters.sqlite import SqliteDriver
from sqlspec.exceptions import (
    CheckViolationError,
    ForeignKeyViolationError,
    NotNullViolationError,
    SQLParsingError,
    UniqueViolationError,
)

pytestmark = pytest.mark.xdist_group("sqlite")


@pytest.fixture
def sqlite_exception_session(sqlite_basic_session: SqliteDriver) -> SqliteDriver:
    """Reuse shared SQLite session for exception scenarios."""

    return sqlite_basic_session


def test_unique_violation(sqlite_exception_session: SqliteDriver) -> None:
    """Test unique constraint violation raises UniqueViolationError."""
    sqlite_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_unique_constraint;
        CREATE TABLE test_unique_constraint (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL
        );
    """)

    sqlite_exception_session.execute("INSERT INTO test_unique_constraint (email) VALUES (?)", ("test@example.com",))

    with pytest.raises(UniqueViolationError) as exc_info:
        sqlite_exception_session.execute("INSERT INTO test_unique_constraint (email) VALUES (?)", ("test@example.com",))

    assert "unique" in str(exc_info.value).lower() or "2067" in str(exc_info.value)

    sqlite_exception_session.execute("DROP TABLE test_unique_constraint")


def test_foreign_key_violation(sqlite_exception_session: SqliteDriver) -> None:
    """Test foreign key constraint violation raises ForeignKeyViolationError."""
    sqlite_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_fk_child;
        DROP TABLE IF EXISTS test_fk_parent;
        CREATE TABLE test_fk_parent (
            id INTEGER PRIMARY KEY,
            name TEXT
        );
        CREATE TABLE test_fk_child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER NOT NULL,
            FOREIGN KEY (parent_id) REFERENCES test_fk_parent(id)
        );
    """)

    with pytest.raises(ForeignKeyViolationError) as exc_info:
        sqlite_exception_session.execute("INSERT INTO test_fk_child (parent_id) VALUES (?)", (999,))

    assert "foreign key" in str(exc_info.value).lower() or "787" in str(exc_info.value)

    sqlite_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_fk_child;
        DROP TABLE IF EXISTS test_fk_parent;
    """)


def test_not_null_violation(sqlite_exception_session: SqliteDriver) -> None:
    """Test NOT NULL constraint violation raises NotNullViolationError."""
    sqlite_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_not_null;
        CREATE TABLE test_not_null (
            id INTEGER PRIMARY KEY,
            required_field TEXT NOT NULL
        );
    """)

    with pytest.raises(NotNullViolationError) as exc_info:
        sqlite_exception_session.execute("INSERT INTO test_not_null (id) VALUES (?)", (1,))

    assert "not null" in str(exc_info.value).lower() or "1811" in str(exc_info.value)

    sqlite_exception_session.execute("DROP TABLE test_not_null")


def test_check_violation(sqlite_exception_session: SqliteDriver) -> None:
    """Test CHECK constraint violation raises CheckViolationError."""
    sqlite_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_check_constraint;
        CREATE TABLE test_check_constraint (
            id INTEGER PRIMARY KEY,
            age INTEGER CHECK (age >= 18)
        );
    """)

    with pytest.raises(CheckViolationError) as exc_info:
        sqlite_exception_session.execute("INSERT INTO test_check_constraint (age) VALUES (?)", (15,))

    assert "check" in str(exc_info.value).lower() or "531" in str(exc_info.value)

    sqlite_exception_session.execute("DROP TABLE test_check_constraint")


def test_sql_parsing_error(sqlite_exception_session: SqliteDriver) -> None:
    """Test syntax error raises SQLParsingError."""
    with pytest.raises(SQLParsingError) as exc_info:
        sqlite_exception_session.execute("SELCT * FROM nonexistent_table")

    assert "syntax" in str(exc_info.value).lower()
