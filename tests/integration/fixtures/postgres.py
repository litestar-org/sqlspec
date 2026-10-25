"""Shared PostgreSQL-family integration fixtures."""

from typing import TYPE_CHECKING, Any

import pytest
from pytest_databases.docker.postgres import PostgresService, exec_psql

from sqlspec.utils.module_loader import import_optional_attr

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
    from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
    from sqlspec.adapters.psqlpy import PsqlpyConfig, PsqlpyDriver, PsqlpyPoolParams
    from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgPoolParams, PsycopgSyncConfig
else:
    AdbcConfig = import_optional_attr("sqlspec.adapters.adbc", "AdbcConfig")
    AdbcDriver = import_optional_attr("sqlspec.adapters.adbc", "AdbcDriver")
    AsyncpgConfig = import_optional_attr("sqlspec.adapters.asyncpg", "AsyncpgConfig")
    AsyncpgDriver = import_optional_attr("sqlspec.adapters.asyncpg", "AsyncpgDriver")
    PsqlpyConfig = import_optional_attr("sqlspec.adapters.psqlpy", "PsqlpyConfig")
    PsqlpyDriver = import_optional_attr("sqlspec.adapters.psqlpy", "PsqlpyDriver")
    PsqlpyPoolParams = import_optional_attr("sqlspec.adapters.psqlpy", "PsqlpyPoolParams")
    PsycopgAsyncConfig = import_optional_attr("sqlspec.adapters.psycopg", "PsycopgAsyncConfig")
    PsycopgPoolParams = import_optional_attr("sqlspec.adapters.psycopg", "PsycopgPoolParams")
    PsycopgSyncConfig = import_optional_attr("sqlspec.adapters.psycopg", "PsycopgSyncConfig")

__all__ = (
    "adbc_postgres_config",
    "adbc_postgres_connection_config",
    "adbc_sync_driver",
    "asyncpg_async_driver",
    "asyncpg_config",
    "asyncpg_connection_config",
    "paradedb_config_adbc",
    "paradedb_config_asyncpg",
    "paradedb_config_psqlpy",
    "paradedb_config_psycopg",
    "pgvector_config_adbc",
    "pgvector_config_asyncpg",
    "pgvector_config_psqlpy",
    "pgvector_config_psycopg",
    "psqlpy_config",
    "psqlpy_driver",
    "psqlpy_session",
    "psycopg_async_config",
    "psycopg_sync_config",
)


@pytest.fixture(scope="session")
def asyncpg_connection_config(postgres_service: PostgresService) -> dict[str, Any]:
    """Base pool configuration for AsyncPG tests."""
    return _postgres_connection_config(postgres_service)


@pytest.fixture(scope="session")
async def asyncpg_config(asyncpg_connection_config: dict[str, Any]) -> "AsyncGenerator[AsyncpgConfig, None]":
    """Provide a session-scoped AsyncpgConfig with a shared pool."""
    pytest.importorskip("asyncpg")
    config = AsyncpgConfig(connection_config=dict(asyncpg_connection_config))
    try:
        yield config
    finally:
        if config.connection_instance is not None:
            await config.close_pool()
            config.connection_instance = None


@pytest.fixture
async def asyncpg_async_driver(asyncpg_config: "AsyncpgConfig") -> "AsyncGenerator[AsyncpgDriver, None]":
    """Create an AsyncPG driver for integration tests."""
    pytest.importorskip("asyncpg")
    async with asyncpg_config.provide_session() as session:
        yield session


@pytest.fixture(scope="session")
def psycopg_sync_config(postgres_service: PostgresService) -> "Generator[PsycopgSyncConfig, None, None]":
    """Create a psycopg sync configuration."""
    pytest.importorskip("psycopg")
    pytest.importorskip("psycopg_pool")
    config = PsycopgSyncConfig(connection_config={"conninfo": _postgres_conninfo(postgres_service)})
    try:
        yield config
    finally:
        if config.connection_instance:
            config.close_pool()


@pytest.fixture(scope="session")
async def psycopg_async_config(postgres_service: PostgresService) -> "AsyncGenerator[PsycopgAsyncConfig, None]":
    """Create a psycopg async configuration."""
    pytest.importorskip("psycopg")
    pytest.importorskip("psycopg_pool")
    config = PsycopgAsyncConfig(
        connection_config={"conninfo": _postgres_conninfo(postgres_service), "min_size": 1, "max_size": 4}
    )
    try:
        yield config
    finally:
        try:
            if config.connection_instance:
                await config.close_pool()
            config.connection_instance = None
        except RuntimeError:
            pass


@pytest.fixture(scope="session")
async def psqlpy_config(postgres_service: PostgresService) -> "AsyncGenerator[PsqlpyConfig, None]":
    """Provide a session-scoped psqlpy configuration."""
    pytest.importorskip("psqlpy")
    config = PsqlpyConfig(connection_config={"dsn": _psqlpy_dsn(postgres_service), "max_db_pool_size": 5})
    try:
        yield config
    finally:
        if config.connection_instance is not None:
            config.connection_instance.close()
            config.connection_instance = None


@pytest.fixture
async def psqlpy_driver(psqlpy_config: "PsqlpyConfig") -> "AsyncGenerator[PsqlpyDriver, None]":
    """Yield a raw psqlpy driver session."""
    pytest.importorskip("psqlpy")
    async with psqlpy_config.provide_session() as session:
        yield session


@pytest.fixture
async def psqlpy_session(psqlpy_config: "PsqlpyConfig") -> "AsyncGenerator[PsqlpyDriver, None]":
    """Create a psqlpy session with test table setup and cleanup."""
    pytest.importorskip("psqlpy")
    async with psqlpy_config.provide_session() as session:
        await session.execute_script(
            """
                CREATE TABLE IF NOT EXISTS test_table_psqlpy (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50)
                );
            """
        )
        try:
            yield session
        finally:
            try:
                await session.execute_script("DROP TABLE IF EXISTS test_table_psqlpy;")
            except Exception:
                pass


@pytest.fixture(scope="session")
def adbc_postgres_connection_config(postgres_service: PostgresService) -> dict[str, str]:
    """Shared PostgreSQL connection configuration for ADBC tests."""
    return {"uri": _adbc_postgres_uri(postgres_service)}


@pytest.fixture(scope="session")
def adbc_postgres_config(adbc_postgres_connection_config: dict[str, str]) -> "Generator[AdbcConfig, None, None]":
    """Provide an ADBC config targeting PostgreSQL."""
    pytest.importorskip("adbc_driver_manager")
    pytest.importorskip("pyarrow")
    config = AdbcConfig(connection_config=dict(adbc_postgres_connection_config))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def adbc_sync_driver(adbc_postgres_config: "AdbcConfig") -> "Generator[AdbcDriver, None, None]":
    """Create an ADBC driver for data dictionary testing."""
    pytest.importorskip("adbc_driver_manager")
    pytest.importorskip("pyarrow")
    with adbc_postgres_config.provide_session() as session:
        yield session


@pytest.fixture
def pgvector_config_adbc(pgvector_service: PostgresService) -> "Generator[AdbcConfig, None, None]":
    """Provide an ADBC config connected to a pgvector-enabled PostgreSQL service."""
    pytest.importorskip("adbc_driver_manager")
    pytest.importorskip("pyarrow")
    _ensure_postgres_extension(pgvector_service, "vector")
    config = AdbcConfig(connection_config={"uri": _adbc_postgres_uri(pgvector_service)})
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def pgvector_config_asyncpg(pgvector_service: PostgresService) -> "AsyncpgConfig":
    """Provide an asyncpg config connected to a pgvector-enabled PostgreSQL service."""
    pytest.importorskip("asyncpg")
    _ensure_postgres_extension(pgvector_service, "vector")
    return AsyncpgConfig(connection_config=_asyncpg_pool_config(pgvector_service))


@pytest.fixture
def pgvector_config_psqlpy(pgvector_service: PostgresService) -> "PsqlpyConfig":
    """Provide a psqlpy config connected to a pgvector-enabled PostgreSQL service."""
    pytest.importorskip("psqlpy")
    _ensure_postgres_extension(pgvector_service, "vector")
    return PsqlpyConfig(connection_config=PsqlpyPoolParams(dsn=_psqlpy_dsn(pgvector_service)))


@pytest.fixture
def pgvector_config_psycopg(pgvector_service: PostgresService) -> "Generator[PsycopgSyncConfig, None, None]":
    """Provide a psycopg config connected to a pgvector-enabled PostgreSQL service."""
    pytest.importorskip("psycopg")
    pytest.importorskip("psycopg_pool")
    _ensure_postgres_extension(pgvector_service, "vector")
    config = PsycopgSyncConfig(
        connection_config=PsycopgPoolParams(conninfo=_postgres_conninfo(pgvector_service), min_size=1, max_size=4)
    )
    try:
        yield config
    finally:
        if config.connection_instance is not None:
            config.close_pool()
            config.connection_instance = None


@pytest.fixture
def paradedb_config_adbc(paradedb_service: PostgresService) -> "Generator[AdbcConfig, None, None]":
    """Provide an ADBC config connected to a ParadeDB service."""
    pytest.importorskip("adbc_driver_manager")
    pytest.importorskip("pyarrow")
    config = AdbcConfig(connection_config={"uri": _adbc_postgres_uri(paradedb_service)})
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def paradedb_config_asyncpg(paradedb_service: PostgresService) -> "AsyncpgConfig":
    """Provide an asyncpg config connected to a ParadeDB service."""
    pytest.importorskip("asyncpg")
    return AsyncpgConfig(connection_config=_asyncpg_pool_config(paradedb_service))


@pytest.fixture
def paradedb_config_psqlpy(paradedb_service: PostgresService) -> "PsqlpyConfig":
    """Provide a psqlpy config connected to a ParadeDB service."""
    pytest.importorskip("psqlpy")
    return PsqlpyConfig(connection_config=PsqlpyPoolParams(dsn=_psqlpy_dsn(paradedb_service)))


@pytest.fixture
def paradedb_config_psycopg(paradedb_service: PostgresService) -> "Generator[PsycopgSyncConfig, None, None]":
    """Provide a psycopg config connected to a ParadeDB service."""
    pytest.importorskip("psycopg")
    pytest.importorskip("psycopg_pool")
    config = PsycopgSyncConfig(
        connection_config=PsycopgPoolParams(conninfo=_postgres_conninfo(paradedb_service), min_size=1, max_size=4)
    )
    try:
        yield config
    finally:
        if config.connection_instance is not None:
            config.close_pool()
            config.connection_instance = None


def _postgres_connection_config(postgres_service: PostgresService) -> dict[str, Any]:
    return {
        "host": postgres_service.host,
        "port": postgres_service.port,
        "user": postgres_service.user,
        "password": postgres_service.password,
        "database": postgres_service.database,
    }


def _postgres_conninfo(postgres_service: PostgresService) -> str:
    return (
        f"postgresql://{postgres_service.user}:{postgres_service.password}"
        f"@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
    )


def _psqlpy_dsn(postgres_service: PostgresService) -> str:
    return (
        f"postgres://{postgres_service.user}:{postgres_service.password}"
        f"@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
    )


def _adbc_postgres_uri(postgres_service: PostgresService) -> str:
    return _postgres_conninfo(postgres_service)


def _asyncpg_pool_config(postgres_service: PostgresService) -> dict[str, Any]:
    return _postgres_connection_config(postgres_service)


def _ensure_postgres_extension(service: PostgresService, extension: str) -> None:
    """Ensure a PostgreSQL extension is enabled inside the service database."""
    exit_code, output = exec_psql(
        service.container,
        user=service.user,
        password=service.password,
        database=service.database,
        sql=f'CREATE EXTENSION IF NOT EXISTS "{extension}"',
    )
    if exit_code != 0:
        decoded_output = output.decode("utf-8", errors="replace")
        msg = f"Failed to enable PostgreSQL extension {extension!r}: {decoded_output}"
        raise RuntimeError(msg)
