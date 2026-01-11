"""Exception handling integration tests for psycopg adapter."""

from collections.abc import AsyncGenerator, Generator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgAsyncDriver, PsycopgSyncConfig, PsycopgSyncDriver
from sqlspec.exceptions import (
    CheckViolationError,
    ForeignKeyViolationError,
    NotNullViolationError,
    SQLParsingError,
    UniqueViolationError,
)

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.fixture
def psycopg_sync_exception_session(postgres_service: "PostgresService") -> Generator[PsycopgSyncDriver, None, None]:
    """Create a psycopg sync session for exception testing."""
    config = PsycopgSyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
            "kwargs": {"autocommit": True},
        }
    )

    try:
        with config.provide_session() as session:
            yield session
    finally:
        if config.connection_instance:
            config.close_pool()


@pytest.fixture
async def psycopg_async_exception_session(
    postgres_service: "PostgresService",
) -> AsyncGenerator[PsycopgAsyncDriver, None]:
    """Create a psycopg async session for exception testing."""
    config = PsycopgAsyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
            "kwargs": {"autocommit": True},
        }
    )

    try:
        async with config.provide_session() as session:
            yield session
    finally:
        if config.connection_instance:
            await config.close_pool()


def test_sync_unique_violation(psycopg_sync_exception_session: PsycopgSyncDriver) -> None:
    """Test unique constraint violation raises UniqueViolationError (sync)."""
    psycopg_sync_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_unique_constraint;
        CREATE TABLE test_unique_constraint (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL
        );
    """)

    psycopg_sync_exception_session.execute(
        "INSERT INTO test_unique_constraint (email) VALUES (%s)", ("test@example.com",)
    )

    with pytest.raises(UniqueViolationError) as exc_info:
        psycopg_sync_exception_session.execute(
            "INSERT INTO test_unique_constraint (email) VALUES (%s)", ("test@example.com",)
        )

    assert "unique" in str(exc_info.value).lower() or "23505" in str(exc_info.value)


async def test_async_unique_violation(psycopg_async_exception_session: PsycopgAsyncDriver) -> None:
    """Test unique constraint violation raises UniqueViolationError (async)."""
    await psycopg_async_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_unique_constraint;
        CREATE TABLE test_unique_constraint (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL
        );
    """)

    await psycopg_async_exception_session.execute(
        "INSERT INTO test_unique_constraint (email) VALUES (%s)", ("test@example.com",)
    )

    with pytest.raises(UniqueViolationError) as exc_info:
        await psycopg_async_exception_session.execute(
            "INSERT INTO test_unique_constraint (email) VALUES (%s)", ("test@example.com",)
        )

    assert "unique" in str(exc_info.value).lower() or "23505" in str(exc_info.value)


def test_sync_foreign_key_violation(psycopg_sync_exception_session: PsycopgSyncDriver) -> None:
    """Test foreign key constraint violation raises ForeignKeyViolationError (sync)."""
    psycopg_sync_exception_session.execute_script("""
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
        psycopg_sync_exception_session.execute("INSERT INTO test_fk_child (parent_id) VALUES (%s)", (999,))

    assert "foreign key" in str(exc_info.value).lower() or "23503" in str(exc_info.value)


def test_sync_not_null_violation(psycopg_sync_exception_session: PsycopgSyncDriver) -> None:
    """Test NOT NULL constraint violation raises NotNullViolationError (sync)."""
    psycopg_sync_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_not_null;
        CREATE TABLE test_not_null (
            id SERIAL PRIMARY KEY,
            required_field VARCHAR(100) NOT NULL
        );
    """)

    with pytest.raises(NotNullViolationError) as exc_info:
        psycopg_sync_exception_session.execute("INSERT INTO test_not_null (id) VALUES (%s)", (1,))

    assert "not null" in str(exc_info.value).lower() or "23502" in str(exc_info.value)


def test_sync_check_violation(psycopg_sync_exception_session: PsycopgSyncDriver) -> None:
    """Test CHECK constraint violation raises CheckViolationError (sync)."""
    psycopg_sync_exception_session.execute_script("""
        DROP TABLE IF EXISTS test_check_constraint;
        CREATE TABLE test_check_constraint (
            id SERIAL PRIMARY KEY,
            age INTEGER CHECK (age >= 18)
        );
    """)

    with pytest.raises(CheckViolationError) as exc_info:
        psycopg_sync_exception_session.execute("INSERT INTO test_check_constraint (age) VALUES (%s)", (15,))

    assert "check" in str(exc_info.value).lower() or "23514" in str(exc_info.value)


def test_sync_sql_parsing_error(psycopg_sync_exception_session: PsycopgSyncDriver) -> None:
    """Test syntax error raises SQLParsingError (sync)."""
    with pytest.raises(SQLParsingError) as exc_info:
        psycopg_sync_exception_session.execute("SELCT * FROM nonexistent_table")

    assert "syntax" in str(exc_info.value).lower() or "42601" in str(exc_info.value)
