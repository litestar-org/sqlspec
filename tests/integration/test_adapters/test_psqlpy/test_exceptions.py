"""Exception handling integration tests for psqlpy adapter."""

from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psqlpy import PsqlpyConfig, PsqlpyDriver
from sqlspec.exceptions import (
    CheckViolationError,
    ForeignKeyViolationError,
    NotNullViolationError,
    SQLParsingError,
    UniqueViolationError,
)

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.fixture
async def psqlpy_exception_session(postgres_service: PostgresService) -> AsyncGenerator[PsqlpyDriver, None]:
    """Create a psqlpy session for exception testing."""
    config = PsqlpyConfig(
        pool_config={
            "dsn": f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
            "max_db_pool_size": 5,
        }
    )

    try:
        async with config.provide_session() as session:
            yield session
    finally:
        await config.close_pool()


async def test_unique_violation(psqlpy_exception_session: PsqlpyDriver) -> None:
    """Test unique constraint violation raises UniqueViolationError."""
    await psqlpy_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_unique_constraint;
        CREATE TABLE test_unique_constraint (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL
        );
    """)

    await psqlpy_exception_session.execute(
        "INSERT INTO test_unique_constraint (email) VALUES ($1)", ("test@example.com",)
    )

    with pytest.raises(UniqueViolationError) as exc_info:
        await psqlpy_exception_session.execute(
            "INSERT INTO test_unique_constraint (email) VALUES ($1)", ("test@example.com",)
        )

    assert "unique" in str(exc_info.value).lower() or "23505" in str(exc_info.value)

    await psqlpy_exception_session.execute("DROP TABLE test_unique_constraint")


async def test_foreign_key_violation(psqlpy_exception_session: PsqlpyDriver) -> None:
    """Test foreign key constraint violation raises ForeignKeyViolationError."""
    await psqlpy_exception_session.execute_script("""
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
        await psqlpy_exception_session.execute("INSERT INTO test_fk_child (parent_id) VALUES ($1)", (999,))

    assert "foreign key" in str(exc_info.value).lower() or "23503" in str(exc_info.value)

    await psqlpy_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_fk_child CASCADE;
        DROP TABLE IF EXISTS test_fk_parent CASCADE;
    """)


async def test_not_null_violation(psqlpy_exception_session: PsqlpyDriver) -> None:
    """Test NOT NULL constraint violation raises NotNullViolationError."""
    await psqlpy_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_not_null;
        CREATE TABLE test_not_null (
            id SERIAL PRIMARY KEY,
            required_field VARCHAR(100) NOT NULL
        );
    """)

    with pytest.raises(NotNullViolationError) as exc_info:
        await psqlpy_exception_session.execute("INSERT INTO test_not_null (id) VALUES ($1)", (1,))

    assert ("not" in str(exc_info.value).lower() and "null" in str(exc_info.value).lower()) or "23502" in str(
        exc_info.value
    )

    await psqlpy_exception_session.execute("DROP TABLE test_not_null")


async def test_check_violation(psqlpy_exception_session: PsqlpyDriver) -> None:
    """Test CHECK constraint violation raises CheckViolationError."""
    await psqlpy_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_check_constraint;
        CREATE TABLE test_check_constraint (
            id SERIAL PRIMARY KEY,
            age INTEGER CHECK (age >= 18)
        );
    """)

    with pytest.raises(CheckViolationError) as exc_info:
        await psqlpy_exception_session.execute("INSERT INTO test_check_constraint (age) VALUES ($1)", (15,))

    assert "check" in str(exc_info.value).lower() or "23514" in str(exc_info.value)

    await psqlpy_exception_session.execute("DROP TABLE test_check_constraint")


async def test_sql_parsing_error(psqlpy_exception_session: PsqlpyDriver) -> None:
    """Test syntax error raises SQLParsingError."""
    with pytest.raises(SQLParsingError) as exc_info:
        await psqlpy_exception_session.execute("SELCT * FROM nonexistent_table")

    assert "syntax" in str(exc_info.value).lower() or "42601" in str(exc_info.value)
