"""Exception handling integration tests for adbc adapter."""

from collections.abc import Generator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.exceptions import (
    CheckViolationError,
    ForeignKeyViolationError,
    NotNullViolationError,
    SQLParsingError,
    UniqueViolationError,
)

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.fixture
def adbc_exception_session(postgres_service: "PostgresService") -> Generator[AdbcDriver, None, None]:
    """Create an ADBC session for exception testing with PostgreSQL backend."""
    config = AdbcConfig(
        connection_config={
            "uri": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )

    try:
        with config.provide_session() as session:
            yield session
    finally:
        config.close_pool()


def test_unique_violation(adbc_exception_session: AdbcDriver) -> None:
    """Test unique constraint violation raises UniqueViolationError."""
    adbc_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_unique_constraint;
        CREATE TABLE test_unique_constraint (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL
        );
    """)

    adbc_exception_session.execute("INSERT INTO test_unique_constraint (email) VALUES ($1)", ("test@example.com",))

    with pytest.raises(UniqueViolationError) as exc_info:
        adbc_exception_session.execute("INSERT INTO test_unique_constraint (email) VALUES ($1)", ("test@example.com",))

    assert "unique" in str(exc_info.value).lower() or "23505" in str(exc_info.value)


def test_foreign_key_violation(adbc_exception_session: AdbcDriver) -> None:
    """Test foreign key constraint violation raises ForeignKeyViolationError."""
    adbc_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_fk_child CASCADE;
        DROP TABLE IF EXISTS test_fk_parent CASCADE;
        CREATE TABLE test_fk_parent (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100)
        );
        CREATE TABLE test_fk_child (
            id SERIAL PRIMARY KEY,
            parent_id INTEGER NOT NULL,
            FOREIGN KEY (parent_id) REFERENCES test_fk_parent(id)
        );
    """)

    with pytest.raises(ForeignKeyViolationError) as exc_info:
        adbc_exception_session.execute("INSERT INTO test_fk_child (parent_id) VALUES ($1)", (999,))

    assert "foreign key" in str(exc_info.value).lower() or "23503" in str(exc_info.value)

    adbc_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_fk_child CASCADE;
        DROP TABLE IF EXISTS test_fk_parent CASCADE;
    """)


def test_not_null_violation(adbc_exception_session: AdbcDriver) -> None:
    """Test NOT NULL constraint violation raises NotNullViolationError."""
    adbc_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_not_null;
        CREATE TABLE test_not_null (
            id SERIAL PRIMARY KEY,
            required_field VARCHAR(100) NOT NULL
        );
    """)

    with pytest.raises(NotNullViolationError) as exc_info:
        adbc_exception_session.execute("INSERT INTO test_not_null (id) VALUES ($1)", (1,))

    assert "not null" in str(exc_info.value).lower() or "23502" in str(exc_info.value)


def test_check_violation(adbc_exception_session: AdbcDriver) -> None:
    """Test CHECK constraint violation raises CheckViolationError."""
    adbc_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_check_constraint;
        CREATE TABLE test_check_constraint (
            id SERIAL PRIMARY KEY,
            age INTEGER CHECK (age >= 18)
        );
    """)

    with pytest.raises(CheckViolationError) as exc_info:
        adbc_exception_session.execute("INSERT INTO test_check_constraint (age) VALUES ($1)", (15,))

    assert "check" in str(exc_info.value).lower() or "23514" in str(exc_info.value)


def test_sql_parsing_error(adbc_exception_session: AdbcDriver) -> None:
    """Test syntax error raises SQLParsingError."""
    with pytest.raises(SQLParsingError) as exc_info:
        adbc_exception_session.execute("SELCT * FROM nonexistent_table")

    assert "syntax" in str(exc_info.value).lower() or "pars" in str(exc_info.value).lower()
