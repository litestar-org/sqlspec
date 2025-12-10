"""Exception handling integration tests for duckdb adapter."""

from collections.abc import Generator

import pytest

from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.exceptions import (
    CheckViolationError,
    ForeignKeyViolationError,
    NotFoundError,
    NotNullViolationError,
    SQLParsingError,
    UniqueViolationError,
)

pytestmark = pytest.mark.xdist_group("duckdb")


@pytest.fixture
def duckdb_exception_session() -> Generator[DuckDBDriver, None, None]:
    """Create a DuckDB session for exception testing."""
    config = DuckDBConfig(connection_config={"database": ":memory:"})

    try:
        with config.provide_session() as session:
            yield session
    finally:
        config.close_pool()


def test_unique_violation(duckdb_exception_session: DuckDBDriver) -> None:
    """Test unique constraint violation raises UniqueViolationError."""
    duckdb_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_unique_constraint;
        CREATE TABLE test_unique_constraint (
            id INTEGER PRIMARY KEY,
            email VARCHAR UNIQUE
        );
    """)

    duckdb_exception_session.execute("INSERT INTO test_unique_constraint VALUES (1, ?)", ("test@example.com",))

    with pytest.raises(UniqueViolationError) as exc_info:
        duckdb_exception_session.execute("INSERT INTO test_unique_constraint VALUES (2, ?)", ("test@example.com",))

    assert "unique" in str(exc_info.value).lower() or "duplicate" in str(exc_info.value).lower()

    duckdb_exception_session.execute("DROP TABLE test_unique_constraint")


def test_foreign_key_violation(duckdb_exception_session: DuckDBDriver) -> None:
    """Test foreign key constraint violation raises ForeignKeyViolationError."""
    duckdb_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_fk_child;
        DROP TABLE IF EXISTS test_fk_parent;
        CREATE TABLE test_fk_parent (
            id INTEGER PRIMARY KEY,
            name VARCHAR
        );
        CREATE TABLE test_fk_child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER NOT NULL,
            FOREIGN KEY (parent_id) REFERENCES test_fk_parent(id)
        );
    """)

    with pytest.raises(ForeignKeyViolationError) as exc_info:
        duckdb_exception_session.execute("INSERT INTO test_fk_child VALUES (1, ?)", (999,))

    assert "foreign key" in str(exc_info.value).lower()

    duckdb_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_fk_child;
        DROP TABLE IF EXISTS test_fk_parent;
    """)


def test_not_null_violation(duckdb_exception_session: DuckDBDriver) -> None:
    """Test NOT NULL constraint violation raises NotNullViolationError."""
    duckdb_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_not_null;
        CREATE TABLE test_not_null (
            id INTEGER PRIMARY KEY,
            required_field VARCHAR NOT NULL
        );
    """)

    with pytest.raises(NotNullViolationError) as exc_info:
        duckdb_exception_session.execute("INSERT INTO test_not_null (id, required_field) VALUES (?, ?)", (1, None))

    assert "not null" in str(exc_info.value).lower() or "null" in str(exc_info.value).lower()

    duckdb_exception_session.execute("DROP TABLE test_not_null")


def test_check_violation(duckdb_exception_session: DuckDBDriver) -> None:
    """Test CHECK constraint violation raises CheckViolationError."""
    duckdb_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_check_constraint;
        CREATE TABLE test_check_constraint (
            id INTEGER PRIMARY KEY,
            age INTEGER CHECK (age >= 18)
        );
    """)

    with pytest.raises(CheckViolationError) as exc_info:
        duckdb_exception_session.execute("INSERT INTO test_check_constraint VALUES (1, ?)", (15,))

    assert "check" in str(exc_info.value).lower()

    duckdb_exception_session.execute("DROP TABLE test_check_constraint")


def test_catalog_exception_not_found(duckdb_exception_session: DuckDBDriver) -> None:
    """Test table not found raises NotFoundError."""
    with pytest.raises(NotFoundError) as exc_info:
        duckdb_exception_session.execute("SELECT * FROM nonexistent_table_xyz")

    assert "catalog" in str(exc_info.value).lower() or "not found" in str(exc_info.value).lower()


def test_sql_parsing_error(duckdb_exception_session: DuckDBDriver) -> None:
    """Test syntax error raises SQLParsingError."""
    with pytest.raises(SQLParsingError) as exc_info:
        duckdb_exception_session.execute("SELCT * FROM test_table")

    assert "pars" in str(exc_info.value).lower() or "syntax" in str(exc_info.value).lower()
