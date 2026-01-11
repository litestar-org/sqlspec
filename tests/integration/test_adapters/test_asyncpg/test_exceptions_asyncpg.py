"""Exception handling integration tests for asyncpg adapter."""

from collections.abc import AsyncGenerator

import pytest

from sqlspec.adapters.asyncpg import AsyncpgDriver
from sqlspec.exceptions import (
    CheckViolationError,
    ForeignKeyViolationError,
    NotNullViolationError,
    SQLParsingError,
    UniqueViolationError,
)
from tests.conftest import requires_interpreted

pytestmark = [pytest.mark.xdist_group("postgres"), requires_interpreted]


@pytest.fixture
async def asyncpg_exception_session(asyncpg_async_driver: AsyncpgDriver) -> "AsyncGenerator[AsyncpgDriver, None]":
    """Create an asyncpg session for exception testing."""

    yield asyncpg_async_driver


async def test_unique_violation(asyncpg_exception_session: AsyncpgDriver) -> None:
    """Test unique constraint violation raises UniqueViolationError."""
    await asyncpg_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_unique_constraint;
        CREATE TABLE test_unique_constraint (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL
        );
    """)

    await asyncpg_exception_session.execute(
        "INSERT INTO test_unique_constraint (email) VALUES ($1)", ("test@example.com",)
    )

    with pytest.raises(UniqueViolationError) as exc_info:
        await asyncpg_exception_session.execute(
            "INSERT INTO test_unique_constraint (email) VALUES ($1)", ("test@example.com",)
        )

    assert "unique" in str(exc_info.value).lower() or "23505" in str(exc_info.value)

    await asyncpg_exception_session.execute("DROP TABLE test_unique_constraint")


async def test_foreign_key_violation(asyncpg_exception_session: AsyncpgDriver) -> None:
    """Test foreign key constraint violation raises ForeignKeyViolationError."""
    await asyncpg_exception_session.execute_script("""
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
        await asyncpg_exception_session.execute("INSERT INTO test_fk_child (parent_id) VALUES ($1)", (999,))

    assert "foreign key" in str(exc_info.value).lower() or "23503" in str(exc_info.value)

    await asyncpg_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_fk_child CASCADE;
        DROP TABLE IF EXISTS test_fk_parent CASCADE;
    """)


async def test_not_null_violation(asyncpg_exception_session: AsyncpgDriver) -> None:
    """Test NOT NULL constraint violation raises NotNullViolationError."""
    await asyncpg_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_not_null;
        CREATE TABLE test_not_null (
            id SERIAL PRIMARY KEY,
            required_field VARCHAR(100) NOT NULL
        );
    """)

    with pytest.raises(NotNullViolationError) as exc_info:
        await asyncpg_exception_session.execute("INSERT INTO test_not_null (id) VALUES ($1)", (1,))

    assert "not null" in str(exc_info.value).lower() or "23502" in str(exc_info.value)

    await asyncpg_exception_session.execute("DROP TABLE test_not_null")


async def test_check_violation(asyncpg_exception_session: AsyncpgDriver) -> None:
    """Test CHECK constraint violation raises CheckViolationError."""
    await asyncpg_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_check_constraint;
        CREATE TABLE test_check_constraint (
            id SERIAL PRIMARY KEY,
            age INTEGER CHECK (age >= 18)
        );
    """)

    with pytest.raises(CheckViolationError) as exc_info:
        await asyncpg_exception_session.execute("INSERT INTO test_check_constraint (age) VALUES ($1)", (15,))

    assert "check" in str(exc_info.value).lower() or "23514" in str(exc_info.value)

    await asyncpg_exception_session.execute("DROP TABLE test_check_constraint")


async def test_sql_parsing_error(asyncpg_exception_session: AsyncpgDriver) -> None:
    """Test syntax error raises SQLParsingError."""
    with pytest.raises(SQLParsingError) as exc_info:
        await asyncpg_exception_session.execute("SELCT * FROM nonexistent_table")

    assert "syntax" in str(exc_info.value).lower() or "42601" in str(exc_info.value)
