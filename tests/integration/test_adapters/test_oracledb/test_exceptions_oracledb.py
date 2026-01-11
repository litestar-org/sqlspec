"""Exception handling integration tests for oracledb adapter."""

from collections.abc import AsyncGenerator, Generator

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleAsyncDriver, OracleSyncConfig, OracleSyncDriver
from sqlspec.exceptions import NotNullViolationError, SQLParsingError, UniqueViolationError

pytestmark = pytest.mark.xdist_group("oracle")


@pytest.fixture
def oracle_sync_exception_session(oracle_service: OracleService) -> Generator[OracleSyncDriver, None, None]:
    """Create an Oracle sync session for exception testing."""
    config = OracleSyncConfig(
        connection_config={
            "user": oracle_service.user,
            "password": oracle_service.password,
            "dsn": f"{oracle_service.host}:{oracle_service.port}/{oracle_service.service_name}",
        }
    )

    try:
        with config.provide_session() as session:
            yield session
    finally:
        config.close_pool()


@pytest.fixture
async def oracle_async_exception_session(oracle_service: OracleService) -> AsyncGenerator[OracleAsyncDriver, None]:
    """Create an Oracle async session for exception testing."""
    config = OracleAsyncConfig(
        connection_config={
            "user": oracle_service.user,
            "password": oracle_service.password,
            "dsn": f"{oracle_service.host}:{oracle_service.port}/{oracle_service.service_name}",
        }
    )

    try:
        async with config.provide_session() as session:
            yield session
    finally:
        await config.close_pool()


def test_sync_unique_violation(oracle_sync_exception_session: OracleSyncDriver) -> None:
    """Test unique constraint violation raises UniqueViolationError (sync)."""
    oracle_sync_exception_session.execute_script("""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE test_unique_constraint';
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;
    """)

    oracle_sync_exception_session.execute("""
        CREATE TABLE test_unique_constraint (
            id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            email VARCHAR2(255) UNIQUE NOT NULL
        )
    """)

    oracle_sync_exception_session.execute(
        "INSERT INTO test_unique_constraint (email) VALUES (:1)", ("test@example.com",)
    )

    with pytest.raises(UniqueViolationError) as exc_info:
        oracle_sync_exception_session.execute(
            "INSERT INTO test_unique_constraint (email) VALUES (:1)", ("test@example.com",)
        )

    assert "unique" in str(exc_info.value).lower() or "00001" in str(exc_info.value)

    oracle_sync_exception_session.execute("DROP TABLE test_unique_constraint")


async def test_async_unique_violation(oracle_async_exception_session: OracleAsyncDriver) -> None:
    """Test unique constraint violation raises UniqueViolationError (async)."""
    await oracle_async_exception_session.execute_script("""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE test_unique_constraint';
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;
    """)

    await oracle_async_exception_session.execute("""
        CREATE TABLE test_unique_constraint (
            id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            email VARCHAR2(255) UNIQUE NOT NULL
        )
    """)

    await oracle_async_exception_session.execute(
        "INSERT INTO test_unique_constraint (email) VALUES (:1)", ("test@example.com",)
    )

    with pytest.raises(UniqueViolationError) as exc_info:
        await oracle_async_exception_session.execute(
            "INSERT INTO test_unique_constraint (email) VALUES (:1)", ("test@example.com",)
        )

    assert "unique" in str(exc_info.value).lower() or "00001" in str(exc_info.value)

    await oracle_async_exception_session.execute("DROP TABLE test_unique_constraint")


def test_sync_not_null_violation(oracle_sync_exception_session: OracleSyncDriver) -> None:
    """Test NOT NULL constraint violation raises NotNullViolationError (sync)."""
    oracle_sync_exception_session.execute_script("""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE test_not_null';
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;
    """)

    oracle_sync_exception_session.execute("""
        CREATE TABLE test_not_null (
            id NUMBER PRIMARY KEY,
            required_field VARCHAR2(100) NOT NULL
        )
    """)

    with pytest.raises(NotNullViolationError) as exc_info:
        oracle_sync_exception_session.execute("INSERT INTO test_not_null (id) VALUES (:1)", (1,))

    assert "not null" in str(exc_info.value).lower() or "01400" in str(exc_info.value)

    oracle_sync_exception_session.execute("DROP TABLE test_not_null")


def test_sync_sql_parsing_error(oracle_sync_exception_session: OracleSyncDriver) -> None:
    """Test syntax error raises SQLParsingError (sync)."""
    with pytest.raises(SQLParsingError) as exc_info:
        oracle_sync_exception_session.execute("SELCT * FROM nonexistent_table")

    assert "sql" in str(exc_info.value).lower()
