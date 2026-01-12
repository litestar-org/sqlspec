"""Test fixtures and configuration for ADBC integration tests."""

import functools
from collections.abc import Callable, Generator
from typing import Any, TypeVar, cast

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver

F = TypeVar("F", bound=Callable[..., Any])


def xfail_if_driver_missing(func: F) -> F:
    """Decorator to xfail a test if the ADBC driver shared object is missing."""

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if (
                "cannot open shared object file" in str(e)
                or "No module named" in str(e)
                or "Failed to import connect function" in str(e)
                or "Could not configure connection" in str(e)
            ):
                pytest.xfail(f"ADBC driver not available: {e}")
            raise e

    return cast("F", wrapper)


@pytest.fixture(scope="session")
def adbc_postgres_connection_config(postgres_service: "PostgresService") -> "dict[str, str]":
    """Shared PostgreSQL connection configuration for ADBC tests."""

    return {
        "uri": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
    }


@pytest.fixture(scope="session")
def adbc_postgres_config(adbc_postgres_connection_config: "dict[str, str]") -> "Generator[AdbcConfig, None, None]":
    """Provide an ADBC config targeting PostgreSQL."""

    config = AdbcConfig(connection_config=dict(adbc_postgres_connection_config))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_sync_driver(adbc_postgres_config: "AdbcConfig") -> "Generator[AdbcDriver, None, None]":
    """Create an ADBC driver for data dictionary testing."""

    with adbc_postgres_config.provide_session() as session:
        yield session


@pytest.fixture(scope="session")
def adbc_postgresql_config(adbc_postgres_connection_config: "dict[str, str]") -> "Generator[AdbcConfig, None, None]":
    """ADBC config using the PostgreSQL driver implementation."""

    connection_config = dict(adbc_postgres_connection_config)
    connection_config["driver_name"] = "adbc_driver_postgresql"
    config = AdbcConfig(connection_config=connection_config)
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_postgresql_session(adbc_postgresql_config: "AdbcConfig") -> "Generator[AdbcDriver, None, None]":
    """Create an ADBC PostgreSQL session with test table handling."""

    with adbc_postgresql_config.provide_session() as session:
        session.execute_script(
            """
                CREATE TABLE IF NOT EXISTS test_table (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    value INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
        )
        session.execute("TRUNCATE TABLE test_table")
        yield session
        try:
            session.execute_script("DROP TABLE IF EXISTS test_table")
        except Exception:  # pragma: no cover - defensive cleanup
            try:
                session.execute("ROLLBACK")
                session.execute_script("DROP TABLE IF EXISTS test_table")
            except Exception:
                pass


@pytest.fixture(scope="function")
def adbc_sqlite_config() -> "Generator[AdbcConfig, None, None]":
    """ADBC configuration for SQLite tests."""

    config = AdbcConfig(connection_config={"uri": ":memory:", "driver_name": "adbc_driver_sqlite"})
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_sqlite_session(adbc_sqlite_config: "AdbcConfig") -> "Generator[AdbcDriver, None, None]":
    """Yield a SQLite-backed ADBC session."""

    with adbc_sqlite_config.provide_session() as session:
        session.execute_script(
            """
                CREATE TABLE IF NOT EXISTS test_table (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    value INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """
        )
        session.execute("DELETE FROM test_table")
        yield session


@pytest.fixture(scope="function")
def adbc_duckdb_config() -> "Generator[AdbcConfig, None, None]":
    """ADBC configuration for DuckDB tests."""

    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_duckdb.dbapi.connect"})
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def adbc_duckdb_session(adbc_duckdb_config: "AdbcConfig") -> "Generator[AdbcDriver, None, None]":
    """Yield a DuckDB-backed ADBC session if the driver is available."""

    try:
        with adbc_duckdb_config.provide_session() as session:
            session.execute_script(
                """
                    CREATE TABLE IF NOT EXISTS test_table (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        value INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
            )
            session.execute("DELETE FROM test_table")
            yield session
    except Exception as exc:
        if (
            "cannot open shared object file" in str(exc)
            or "No module named" in str(exc)
            or "Failed to import connect function" in str(exc)
            or "Could not configure connection" in str(exc)
        ):
            pytest.skip("DuckDB ADBC driver unavailable")
        raise


@pytest.fixture(scope="function")
def adbc_duckdb_driver(adbc_duckdb_session: "AdbcDriver") -> "Generator[AdbcDriver, None, None]":
    """Alias fixture to emphasize driver usage in tests."""

    yield adbc_duckdb_session
