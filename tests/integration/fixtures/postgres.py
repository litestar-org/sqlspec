"""Shared PostgreSQL-family integration fixtures."""

from collections.abc import AsyncGenerator, Generator
from typing import Any, cast

import pytest
from pytest_databases.docker.cockroachdb import CockroachDBService
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.adapters.psqlpy import PsqlpyConfig, PsqlpyDriver, PsqlpyPoolParams
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgPoolParams, PsycopgSyncConfig

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


def _postgres_connection_config(postgres_service: "PostgresService") -> "dict[str, Any]":
    return {
        "host": postgres_service.host,
        "port": postgres_service.port,
        "user": postgres_service.user,
        "password": postgres_service.password,
        "database": postgres_service.database,
    }


def _postgres_conninfo(postgres_service: "PostgresService") -> str:
    return (
        f"postgresql://{postgres_service.user}:{postgres_service.password}"
        f"@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
    )


def _psqlpy_dsn(postgres_service: "PostgresService") -> str:
    return (
        f"postgres://{postgres_service.user}:{postgres_service.password}"
        f"@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
    )


def _adbc_postgres_uri(postgres_service: "PostgresService") -> str:
    return _postgres_conninfo(postgres_service)


def _asyncpg_pool_config(postgres_service: "PostgresService") -> "dict[str, Any]":
    return _postgres_connection_config(postgres_service)


def _cockroach_conninfo(cockroachdb_service: "CockroachDBService") -> str:
    return (
        f"host={cockroachdb_service.host} port={cockroachdb_service.port} "
        f"user=root dbname={cockroachdb_service.database} sslmode=disable"
    )


def _cockroach_asyncpg_connection_config(cockroachdb_service: "CockroachDBService") -> "dict[str, Any]":
    return {
        "host": cockroachdb_service.host,
        "port": cockroachdb_service.port,
        "user": "root",
        "password": "",
        "database": cockroachdb_service.database,
        "ssl": None,
    }


def _ensure_postgres_extension(postgres_service: "PostgresService", extension: str) -> None:
    import psycopg

    with psycopg.connect(_postgres_conninfo(postgres_service)) as conn:
        cast("Any", conn).execute(f"CREATE EXTENSION IF NOT EXISTS {extension}")
        conn.commit()


@pytest.fixture(scope="session")
def asyncpg_connection_config(postgres_service: "PostgresService") -> "dict[str, Any]":
    """Base pool configuration for AsyncPG tests."""
    return _postgres_connection_config(postgres_service)


@pytest.fixture(scope="session")
async def asyncpg_config(asyncpg_connection_config: "dict[str, Any]") -> "AsyncGenerator[AsyncpgConfig, None]":
    """Provide a session-scoped AsyncpgConfig with a shared pool."""
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
    async with asyncpg_config.provide_session() as session:
        yield session


@pytest.fixture(scope="session")
def psycopg_sync_config(postgres_service: "PostgresService") -> "Generator[PsycopgSyncConfig, None, None]":
    """Create a psycopg sync configuration."""
    config = PsycopgSyncConfig(connection_config={"conninfo": _postgres_conninfo(postgres_service)})
    try:
        yield config
    finally:
        if config.connection_instance:
            config.close_pool()


@pytest.fixture(scope="session")
async def psycopg_async_config(postgres_service: "PostgresService") -> "AsyncGenerator[PsycopgAsyncConfig, None]":
    """Create a psycopg async configuration."""
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
async def psqlpy_config(postgres_service: "PostgresService") -> "AsyncGenerator[PsqlpyConfig, None]":
    """Provide a session-scoped psqlpy configuration."""
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
    async with psqlpy_config.provide_session() as session:
        yield session


@pytest.fixture
async def psqlpy_session(psqlpy_config: "PsqlpyConfig") -> "AsyncGenerator[PsqlpyDriver, None]":
    """Create a psqlpy session with test table setup and cleanup."""
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
def adbc_postgres_connection_config(postgres_service: "PostgresService") -> "dict[str, str]":
    """Shared PostgreSQL connection configuration for ADBC tests."""
    return {"uri": _adbc_postgres_uri(postgres_service)}


@pytest.fixture(scope="session")
def adbc_postgres_config(adbc_postgres_connection_config: "dict[str, str]") -> "Generator[AdbcConfig, None, None]":
    """Provide an ADBC config targeting PostgreSQL."""
    config = AdbcConfig(connection_config=dict(adbc_postgres_connection_config))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def adbc_sync_driver(adbc_postgres_config: "AdbcConfig") -> "Generator[AdbcDriver, None, None]":
    """Create an ADBC driver for data dictionary testing."""
    with adbc_postgres_config.provide_session() as session:
        yield session


@pytest.fixture
def pgvector_config_adbc(pgvector_service: "PostgresService") -> "Generator[AdbcConfig, None, None]":
    """Provide an ADBC config connected to a pgvector-enabled PostgreSQL service."""
    from sqlspec.adapters.adbc.core import build_connection_config, resolve_driver_connect_func

    connection_config = {"uri": _adbc_postgres_uri(pgvector_service)}
    conn = resolve_driver_connect_func(None, connection_config["uri"])(**build_connection_config(connection_config))
    try:
        cursor = conn.cursor()
        try:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS vector")
        finally:
            cursor.close()
        conn.commit()
    finally:
        conn.close()

    config = AdbcConfig(connection_config=connection_config)
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def pgvector_config_asyncpg(pgvector_service: "PostgresService") -> "AsyncpgConfig":
    """Provide an asyncpg config connected to a pgvector-enabled PostgreSQL service."""
    _ensure_postgres_extension(pgvector_service, "vector")
    return AsyncpgConfig(connection_config=_asyncpg_pool_config(pgvector_service))


@pytest.fixture
def pgvector_config_psqlpy(pgvector_service: "PostgresService") -> "PsqlpyConfig":
    """Provide a psqlpy config connected to a pgvector-enabled PostgreSQL service."""
    _ensure_postgres_extension(pgvector_service, "vector")
    return PsqlpyConfig(connection_config=PsqlpyPoolParams(dsn=_psqlpy_dsn(pgvector_service)))


@pytest.fixture
def pgvector_config_psycopg(pgvector_service: "PostgresService") -> "Generator[PsycopgSyncConfig, None, None]":
    """Provide a psycopg config connected to a pgvector-enabled PostgreSQL service."""
    import psycopg

    connection_config = PsycopgPoolParams(conninfo=_postgres_conninfo(pgvector_service), min_size=1, max_size=4)
    with psycopg.connect(connection_config["conninfo"]) as conn:
        conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
        conn.commit()

    config = PsycopgSyncConfig(connection_config=connection_config)
    try:
        yield config
    finally:
        if config.connection_instance is not None:
            config.close_pool()
            config.connection_instance = None


@pytest.fixture
def paradedb_config_adbc(paradedb_service: "PostgresService") -> "Generator[AdbcConfig, None, None]":
    """Provide an ADBC config connected to a ParadeDB service."""
    config = AdbcConfig(connection_config={"uri": _adbc_postgres_uri(paradedb_service)})
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def paradedb_config_asyncpg(paradedb_service: "PostgresService") -> "AsyncpgConfig":
    """Provide an asyncpg config connected to a ParadeDB service."""
    return AsyncpgConfig(connection_config=_asyncpg_pool_config(paradedb_service))


@pytest.fixture
def paradedb_config_psqlpy(paradedb_service: "PostgresService") -> "PsqlpyConfig":
    """Provide a psqlpy config connected to a ParadeDB service."""
    return PsqlpyConfig(connection_config=PsqlpyPoolParams(dsn=_psqlpy_dsn(paradedb_service)))


@pytest.fixture
def paradedb_config_psycopg(paradedb_service: "PostgresService") -> "Generator[PsycopgSyncConfig, None, None]":
    """Provide a psycopg config connected to a ParadeDB service."""
    config = PsycopgSyncConfig(
        connection_config=PsycopgPoolParams(conninfo=_postgres_conninfo(paradedb_service), min_size=1, max_size=4)
    )
    try:
        yield config
    finally:
        if config.connection_instance is not None:
            config.close_pool()
            config.connection_instance = None
