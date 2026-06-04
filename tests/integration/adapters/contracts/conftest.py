"""Fixtures for shared adapter contract tests."""

import contextlib
from collections.abc import AsyncGenerator, Callable, Generator
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from pytest_databases.docker.cockroachdb import CockroachDBService
from pytest_databases.docker.mysql import MySQLService
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.aiomysql import AiomysqlConfig, AiomysqlDriver
from sqlspec.adapters.aiomysql.adk import AiomysqlADKStore
from sqlspec.adapters.aiomysql.litestar.store import AiomysqlStore
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.aiosqlite.adk import AiosqliteADKStore
from sqlspec.adapters.aiosqlite.litestar.store import AiosqliteStore
from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver
from sqlspec.adapters.asyncmy.adk import AsyncmyADKStore
from sqlspec.adapters.asyncmy.litestar.store import AsyncmyStore
from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore
from sqlspec.adapters.asyncpg.config import AsyncpgPoolConfig
from sqlspec.adapters.asyncpg.litestar.store import AsyncpgStore
from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig, CockroachAsyncpgDriver
from sqlspec.adapters.cockroach_psycopg import (
    CockroachPsycopgAsyncConfig,
    CockroachPsycopgAsyncDriver,
    CockroachPsycopgSyncConfig,
    CockroachPsycopgSyncDriver,
)
from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.adapters.duckdb.adk import DuckdbADKStore
from sqlspec.adapters.duckdb.litestar.store import DuckdbStore
from sqlspec.adapters.mysqlconnector import (
    MysqlConnectorAsyncConfig,
    MysqlConnectorAsyncDriver,
    MysqlConnectorSyncConfig,
    MysqlConnectorSyncDriver,
)
from sqlspec.adapters.mysqlconnector.adk import MysqlConnectorAsyncADKStore, MysqlConnectorSyncADKStore
from sqlspec.adapters.mysqlconnector.litestar.store import MysqlConnectorAsyncStore, MysqlConnectorSyncStore
from sqlspec.adapters.psqlpy import PsqlpyConfig, PsqlpyDriver
from sqlspec.adapters.psqlpy.adk import PsqlpyADKStore
from sqlspec.adapters.psqlpy.litestar.store import PsqlpyStore
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgAsyncDriver, PsycopgSyncConfig, PsycopgSyncDriver
from sqlspec.adapters.psycopg.litestar.store import PsycopgAsyncStore, PsycopgSyncStore
from sqlspec.adapters.pymysql import PyMysqlConfig, PyMysqlDriver
from sqlspec.adapters.pymysql.litestar.store import PyMysqlStore
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.adapters.sqlite.adk import SqliteADKStore
from sqlspec.adapters.sqlite.litestar.store import SQLiteStore
from tests.integration.adapters.contracts._adk_cases import ADK_STORE_PARAMS, AdkStoreCase, AdkStoreCaseContext
from tests.integration.adapters.contracts._cases import (
    ASYNC_DRIVER_PARAMS,
    DRIVER_PARAMS,
    SYNC_DRIVER_PARAMS,
    DriverCase,
    DriverCaseContext,
)
from tests.integration.adapters.contracts._events_cases import (
    ASYNC_EVENTS_PARAMS,
    SYNC_EVENTS_PARAMS,
    EventsCase,
    EventsCaseContext,
)
from tests.integration.adapters.contracts._migration_cases import (
    ASYNC_MIGRATION_PARAMS,
    SYNC_MIGRATION_PARAMS,
    MigrationCase,
    MigrationCaseContext,
)
from tests.integration.adapters.contracts._schema import (
    DEFAULT_CONTRACT_TABLE,
    DUCKDB_CONTRACT_TABLE,
    MYSQL_CONTRACT_TABLE,
    POSTGRES_CONTRACT_TABLE,
)
from tests.integration.adapters.contracts._store_cases import STORE_PARAMS, StoreCase, StoreCaseContext


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


def _cockroach_conninfo(cockroachdb_service: CockroachDBService) -> str:
    return (
        f"host={cockroachdb_service.host} port={cockroachdb_service.port} "
        f"user=root dbname={cockroachdb_service.database} sslmode=disable"
    )


@pytest.fixture
def contract_sqlite_driver() -> Generator[SqliteDriver, None, None]:
    """Provide a fresh SQLite driver for contract tests."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as driver:
            driver.execute("PRAGMA foreign_keys = ON")
            driver.execute_script(DEFAULT_CONTRACT_TABLE.create_sql)
            driver.commit()
            yield driver
    finally:
        config.close_pool()


@pytest.fixture
def contract_duckdb_driver() -> Generator[DuckDBDriver, None, None]:
    """Provide a fresh DuckDB driver for contract tests."""
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as driver:
            driver.execute_script("DROP TABLE IF EXISTS contract_items")
            driver.execute_script(DUCKDB_CONTRACT_TABLE.create_sql)
            driver.commit()
            yield driver
    finally:
        config.close_pool()


@pytest.fixture
def contract_mysqlconnector_sync_driver(mysql_service: MySQLService) -> Generator[MysqlConnectorSyncDriver, None, None]:
    """Provide a fresh mysql-connector sync driver for contract tests."""
    config = MysqlConnectorSyncConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
            "use_pure": True,
            "pool_size": 5,
        }
    )
    try:
        with config.provide_session() as driver:
            driver.execute("SET sql_notes = 0")
            driver.execute_script("DROP TABLE IF EXISTS contract_items")
            driver.execute_script(MYSQL_CONTRACT_TABLE.create_sql)
            driver.execute("SET sql_notes = 1")
            driver.commit()
            yield driver
            driver.execute("SET sql_notes = 0")
            driver.execute_script("DROP TABLE IF EXISTS contract_items")
            driver.execute("SET sql_notes = 1")
            driver.commit()
    finally:
        config.close_pool()


@pytest.fixture
def contract_pymysql_driver(mysql_service: MySQLService) -> Generator[PyMysqlDriver, None, None]:
    """Provide a fresh PyMySQL driver for contract tests."""
    config = PyMysqlConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
        }
    )
    try:
        with config.provide_session() as driver:
            driver.execute("SET sql_notes = 0")
            driver.execute_script("DROP TABLE IF EXISTS contract_items")
            driver.execute_script(MYSQL_CONTRACT_TABLE.create_sql)
            driver.execute("SET sql_notes = 1")
            driver.commit()
            yield driver
            driver.execute("SET sql_notes = 0")
            driver.execute_script("DROP TABLE IF EXISTS contract_items")
            driver.execute("SET sql_notes = 1")
            driver.commit()
    finally:
        config.close_pool()


@pytest.fixture
def contract_psycopg_sync_driver(postgres_service: PostgresService) -> Generator[PsycopgSyncDriver, None, None]:
    """Provide a fresh psycopg sync driver for contract tests."""
    config = PsycopgSyncConfig(
        connection_config={
            "conninfo": _postgres_conninfo(postgres_service),
            "autocommit": True,
            "min_size": 1,
            "max_size": 5,
        }
    )
    try:
        with config.provide_session() as driver:
            driver.execute_script("DROP TABLE IF EXISTS contract_items")
            driver.execute_script(POSTGRES_CONTRACT_TABLE.create_sql)
            driver.commit()
            yield driver
            with contextlib.suppress(Exception):
                driver.rollback()
            driver.execute_script("DROP TABLE IF EXISTS contract_items")
            driver.commit()
    finally:
        config.close_pool()


@pytest.fixture
def contract_cockroach_psycopg_sync_driver(
    cockroachdb_service: CockroachDBService,
) -> Generator[CockroachPsycopgSyncDriver, None, None]:
    """Provide a fresh CockroachDB psycopg sync driver for contract tests."""
    config = CockroachPsycopgSyncConfig(
        connection_config={"conninfo": _cockroach_conninfo(cockroachdb_service), "min_size": 1, "max_size": 5}
    )
    try:
        with config.provide_session() as driver:
            driver.execute_script("DROP TABLE IF EXISTS contract_items")
            driver.execute_script(POSTGRES_CONTRACT_TABLE.create_sql)
            driver.commit()
            yield driver
            with contextlib.suppress(Exception):
                driver.rollback()
            driver.execute_script("DROP TABLE IF EXISTS contract_items")
            driver.commit()
    finally:
        config.close_pool()


@pytest.fixture
async def contract_aiosqlite_driver() -> AsyncGenerator[AiosqliteDriver, None]:
    """Provide a fresh aiosqlite driver for contract tests."""
    config = AiosqliteConfig()
    try:
        async with config.provide_session() as driver:
            await driver.execute("PRAGMA foreign_keys = ON")
            await driver.execute_script(DEFAULT_CONTRACT_TABLE.create_sql)
            await driver.commit()
            yield driver
    finally:
        if config.connection_instance:
            await config.close_pool()
        config.connection_instance = None


@pytest.fixture
async def contract_aiomysql_driver(mysql_service: MySQLService) -> AsyncGenerator[AiomysqlDriver, None]:
    """Provide a fresh aiomysql driver for contract tests."""
    config = AiomysqlConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "db": mysql_service.db,
            "autocommit": True,
            "minsize": 1,
            "maxsize": 5,
        }
    )
    try:
        async with config.provide_session() as driver:
            await driver.execute("SET sql_notes = 0")
            await driver.execute_script("DROP TABLE IF EXISTS contract_items")
            await driver.execute_script(MYSQL_CONTRACT_TABLE.create_sql)
            await driver.execute("SET sql_notes = 1")
            await driver.commit()
            yield driver
            await driver.execute("SET sql_notes = 0")
            await driver.execute_script("DROP TABLE IF EXISTS contract_items")
            await driver.execute("SET sql_notes = 1")
            await driver.commit()
    finally:
        await config.close_pool()


@pytest.fixture
async def contract_asyncmy_driver(mysql_service: MySQLService) -> AsyncGenerator[AsyncmyDriver, None]:
    """Provide a fresh AsyncMy driver for contract tests."""
    config = AsyncmyConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
            "minsize": 1,
            "maxsize": 5,
        }
    )
    try:
        async with config.provide_session() as driver:
            await driver.execute("SET sql_notes = 0")
            await driver.execute_script("DROP TABLE IF EXISTS contract_items")
            await driver.execute_script(MYSQL_CONTRACT_TABLE.create_sql)
            await driver.execute("SET sql_notes = 1")
            await driver.commit()
            yield driver
            await driver.execute("SET sql_notes = 0")
            await driver.execute_script("DROP TABLE IF EXISTS contract_items")
            await driver.execute("SET sql_notes = 1")
            await driver.commit()
    finally:
        await config.close_pool()


@pytest.fixture
async def contract_mysqlconnector_async_driver(
    mysql_service: MySQLService,
) -> AsyncGenerator[MysqlConnectorAsyncDriver, None]:
    """Provide a fresh mysql-connector async driver for contract tests."""
    config = MysqlConnectorAsyncConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
            "use_pure": True,
        }
    )
    async with config.provide_session() as driver:
        await driver.execute("SET sql_notes = 0")
        await driver.execute_script("DROP TABLE IF EXISTS contract_items")
        await driver.execute_script(MYSQL_CONTRACT_TABLE.create_sql)
        await driver.execute("SET sql_notes = 1")
        await driver.commit()
        yield driver
        await driver.execute("SET sql_notes = 0")
        await driver.execute_script("DROP TABLE IF EXISTS contract_items")
        await driver.execute("SET sql_notes = 1")
        await driver.commit()


@pytest.fixture
async def contract_asyncpg_driver(postgres_service: PostgresService) -> AsyncGenerator[AsyncpgDriver, None]:
    """Provide a fresh asyncpg driver for contract tests."""
    connection_config: AsyncpgPoolConfig = {
        "host": postgres_service.host,
        "port": postgres_service.port,
        "user": postgres_service.user,
        "password": postgres_service.password,
        "database": postgres_service.database,
        "min_size": 1,
        "max_size": 5,
    }
    config = AsyncpgConfig(connection_config=connection_config)
    try:
        async with config.provide_session() as driver:
            await driver.execute_script("DROP TABLE IF EXISTS contract_items")
            await driver.execute_script(POSTGRES_CONTRACT_TABLE.create_sql)
            await driver.commit()
            yield driver
            with contextlib.suppress(Exception):
                await driver.rollback()
            await driver.execute_script("DROP TABLE IF EXISTS contract_items")
            await driver.commit()
    finally:
        await config.close_pool()


@pytest.fixture
async def contract_psqlpy_driver(postgres_service: PostgresService) -> AsyncGenerator[PsqlpyDriver, None]:
    """Provide a fresh psqlpy driver for contract tests."""
    config = PsqlpyConfig(connection_config={"dsn": _psqlpy_dsn(postgres_service), "max_db_pool_size": 5})
    try:
        async with config.provide_session() as driver:
            await driver.execute_script("DROP TABLE IF EXISTS contract_items")
            await driver.execute_script(POSTGRES_CONTRACT_TABLE.create_sql)
            await driver.commit()
            yield driver
            with contextlib.suppress(Exception):
                await driver.rollback()
            await driver.execute_script("DROP TABLE IF EXISTS contract_items")
            await driver.commit()
    finally:
        await config.close_pool()


@pytest.fixture
async def contract_psycopg_async_driver(postgres_service: PostgresService) -> AsyncGenerator[PsycopgAsyncDriver, None]:
    """Provide a fresh psycopg async driver for contract tests."""
    config = PsycopgAsyncConfig(
        connection_config={
            "conninfo": _postgres_conninfo(postgres_service),
            "autocommit": True,
            "min_size": 1,
            "max_size": 5,
        }
    )
    try:
        async with config.provide_session() as driver:
            await driver.execute_script("DROP TABLE IF EXISTS contract_items")
            await driver.execute_script(POSTGRES_CONTRACT_TABLE.create_sql)
            await driver.commit()
            yield driver
            await driver.execute_script("DROP TABLE IF EXISTS contract_items")
            await driver.commit()
    finally:
        await config.close_pool()


@pytest.fixture
async def contract_cockroach_asyncpg_driver(
    cockroachdb_service: CockroachDBService,
) -> AsyncGenerator[CockroachAsyncpgDriver, None]:
    """Provide a fresh CockroachDB asyncpg driver for contract tests."""
    config = CockroachAsyncpgConfig(
        connection_config={
            "host": cockroachdb_service.host,
            "port": cockroachdb_service.port,
            "user": "root",
            "password": "",
            "database": cockroachdb_service.database,
            "ssl": None,
            "min_size": 1,
            "max_size": 5,
        }
    )
    try:
        async with config.provide_session() as driver:
            await driver.execute_script("DROP TABLE IF EXISTS contract_items")
            await driver.execute_script(POSTGRES_CONTRACT_TABLE.create_sql)
            await driver.commit()
            yield driver
            await driver.execute_script("DROP TABLE IF EXISTS contract_items")
            await driver.commit()
    finally:
        await config.close_pool()


@pytest.fixture
async def contract_cockroach_psycopg_async_driver(
    cockroachdb_service: CockroachDBService,
) -> AsyncGenerator[CockroachPsycopgAsyncDriver, None]:
    """Provide a fresh CockroachDB psycopg async driver for contract tests."""
    config = CockroachPsycopgAsyncConfig(
        connection_config={"conninfo": _cockroach_conninfo(cockroachdb_service), "min_size": 1, "max_size": 5}
    )
    try:
        async with config.provide_session() as driver:
            await driver.execute_script("DROP TABLE IF EXISTS contract_items")
            await driver.execute_script(POSTGRES_CONTRACT_TABLE.create_sql)
            await driver.commit()
            yield driver
            with contextlib.suppress(Exception):
                await driver.rollback()
            await driver.execute_script("DROP TABLE IF EXISTS contract_items")
            await driver.commit()
    finally:
        await config.close_pool()


def _events_migration_config(tmp_path: Path, suffix: str) -> dict[str, Any]:
    migrations = tmp_path / f"migrations_{suffix}"
    migrations.mkdir()
    return {
        "script_location": str(migrations),
        "include_extensions": ["events"],
        "version_table_name": f"ddl_migrations_{suffix}",
    }


@pytest.fixture
def events_config_sqlite(tmp_path: Path) -> Callable[..., Any]:
    """Build SQLite event-channel configs for contract tests."""

    def make(*, extension_config: dict[str, Any], suffix: str) -> SqliteConfig:
        return SqliteConfig(
            connection_config={"database": str(tmp_path / f"events_{suffix}.db")},
            migration_config=_events_migration_config(tmp_path, suffix),
            extension_config=extension_config,
        )

    return make


@pytest.fixture
def events_config_duckdb(tmp_path: Path) -> Callable[..., Any]:
    """Build DuckDB event-channel configs for contract tests."""

    def make(*, extension_config: dict[str, Any], suffix: str) -> DuckDBConfig:
        return DuckDBConfig(
            connection_config={"database": str(tmp_path / f"events_{suffix}.duckdb")},
            migration_config=_events_migration_config(tmp_path, suffix),
            extension_config=extension_config,
        )

    return make


@pytest.fixture
def events_config_aiosqlite(tmp_path: Path) -> Callable[..., Any]:
    """Build aiosqlite event-channel configs for contract tests."""

    def make(*, extension_config: dict[str, Any], suffix: str) -> AiosqliteConfig:
        return AiosqliteConfig(
            connection_config={"database": str(tmp_path / f"events_{suffix}.db")},
            migration_config=_events_migration_config(tmp_path, suffix),
            extension_config=extension_config,
        )

    return make


def _mysql_connection_config(mysql_service: MySQLService, *, database_key: str = "database") -> dict[str, Any]:
    return {
        "host": mysql_service.host,
        "port": mysql_service.port,
        "user": mysql_service.user,
        "password": mysql_service.password,
        database_key: mysql_service.db,
        "autocommit": True,
    }


@pytest.fixture
def events_config_pymysql(mysql_service: MySQLService, tmp_path: Path) -> Callable[..., Any]:
    """Build PyMySQL event-channel configs for contract tests."""

    def make(*, extension_config: dict[str, Any], suffix: str) -> PyMysqlConfig:
        return PyMysqlConfig(
            connection_config=_mysql_connection_config(mysql_service),
            migration_config=_events_migration_config(tmp_path, suffix),
            extension_config=extension_config,
        )

    return make


@pytest.fixture
def events_config_asyncmy(mysql_service: MySQLService, tmp_path: Path) -> Callable[..., Any]:
    """Build AsyncMy event-channel configs for contract tests."""

    def make(*, extension_config: dict[str, Any], suffix: str) -> AsyncmyConfig:
        return AsyncmyConfig(
            connection_config=_mysql_connection_config(mysql_service),
            migration_config=_events_migration_config(tmp_path, suffix),
            extension_config=extension_config,
        )

    return make


@pytest.fixture
def events_config_aiomysql(mysql_service: MySQLService, tmp_path: Path) -> Callable[..., Any]:
    """Build aiomysql event-channel configs for contract tests."""

    def make(*, extension_config: dict[str, Any], suffix: str) -> AiomysqlConfig:
        return AiomysqlConfig(
            connection_config=_mysql_connection_config(mysql_service, database_key="db"),
            migration_config=_events_migration_config(tmp_path, suffix),
            extension_config=extension_config,
        )

    return make


@pytest.fixture
def events_config_mysqlconnector_async(mysql_service: MySQLService, tmp_path: Path) -> Callable[..., Any]:
    """Build mysql-connector async event-channel configs for contract tests."""

    def make(*, extension_config: dict[str, Any], suffix: str) -> MysqlConnectorAsyncConfig:
        connection_config = _mysql_connection_config(mysql_service)
        connection_config["use_pure"] = True
        return MysqlConnectorAsyncConfig(
            connection_config=connection_config,
            migration_config=_events_migration_config(tmp_path, suffix),
            extension_config=extension_config,
        )

    return make


@pytest.fixture
def events_config_psycopg_sync(postgres_service: PostgresService, tmp_path: Path) -> Callable[..., Any]:
    """Build psycopg sync event-channel configs for contract tests."""

    def make(*, extension_config: dict[str, Any], suffix: str) -> PsycopgSyncConfig:
        return PsycopgSyncConfig(
            connection_config={"conninfo": _postgres_conninfo(postgres_service)},
            migration_config=_events_migration_config(tmp_path, suffix),
            extension_config=extension_config,
        )

    return make


@pytest.fixture
def events_config_psycopg_async(postgres_service: PostgresService, tmp_path: Path) -> Callable[..., Any]:
    """Build psycopg async event-channel configs for contract tests."""

    def make(*, extension_config: dict[str, Any], suffix: str) -> PsycopgAsyncConfig:
        return PsycopgAsyncConfig(
            connection_config={"conninfo": _postgres_conninfo(postgres_service)},
            migration_config=_events_migration_config(tmp_path, suffix),
            extension_config=extension_config,
        )

    return make


@pytest.fixture
def events_config_psqlpy(postgres_service: PostgresService, tmp_path: Path) -> Callable[..., Any]:
    """Build psqlpy event-channel configs for contract tests."""

    def make(*, extension_config: dict[str, Any], suffix: str) -> PsqlpyConfig:
        return PsqlpyConfig(
            connection_config={"dsn": _psqlpy_dsn(postgres_service)},
            migration_config=_events_migration_config(tmp_path, suffix),
            extension_config=extension_config,
        )

    return make


def _resolve_events_case(request: pytest.FixtureRequest, case: EventsCase) -> EventsCaseContext:
    return EventsCaseContext(case=case, make_config=request.getfixturevalue(case.factory_fixture))


@pytest.fixture(params=SYNC_EVENTS_PARAMS)
def sync_events_case(request: pytest.FixtureRequest) -> EventsCaseContext:
    """Resolve a sync event-channel contract case by factory fixture name."""
    return _resolve_events_case(request, request.param)


@pytest.fixture(params=ASYNC_EVENTS_PARAMS)
def async_events_case(request: pytest.FixtureRequest) -> EventsCaseContext:
    """Resolve an async event-channel contract case by factory fixture name."""
    return _resolve_events_case(request, request.param)


@pytest.fixture
def migration_config_sqlite(tmp_path: Path) -> Callable[..., Any]:
    """Build SQLite configs for migration contract tests."""

    def make(*, script_location: str, version_table_name: str, suffix: str) -> SqliteConfig:
        return SqliteConfig(
            connection_config={"database": str(tmp_path / f"mig_{suffix}.db")},
            migration_config={"script_location": script_location, "version_table_name": version_table_name},
        )

    return make


@pytest.fixture
def migration_config_duckdb(tmp_path: Path) -> Callable[..., Any]:
    """Build DuckDB configs for migration contract tests."""

    def make(*, script_location: str, version_table_name: str, suffix: str) -> DuckDBConfig:
        return DuckDBConfig(
            connection_config={"database": str(tmp_path / f"mig_{suffix}.duckdb")},
            migration_config={"script_location": script_location, "version_table_name": version_table_name},
        )

    return make


@pytest.fixture
def migration_config_aiosqlite(tmp_path: Path) -> Callable[..., Any]:
    """Build aiosqlite configs for migration contract tests."""

    def make(*, script_location: str, version_table_name: str, suffix: str) -> AiosqliteConfig:
        return AiosqliteConfig(
            connection_config={"database": str(tmp_path / f"mig_{suffix}.db")},
            migration_config={"script_location": script_location, "version_table_name": version_table_name},
        )

    return make


@pytest.fixture
def migration_config_pymysql(mysql_service: MySQLService) -> Callable[..., Any]:
    """Build PyMySQL configs for migration contract tests."""

    def make(*, script_location: str, version_table_name: str, suffix: str) -> PyMysqlConfig:
        return PyMysqlConfig(
            connection_config=_mysql_connection_config(mysql_service),
            migration_config={"script_location": script_location, "version_table_name": version_table_name},
        )

    return make


@pytest.fixture
def migration_config_asyncmy(mysql_service: MySQLService) -> Callable[..., Any]:
    """Build AsyncMy configs for migration contract tests."""

    def make(*, script_location: str, version_table_name: str, suffix: str) -> AsyncmyConfig:
        return AsyncmyConfig(
            connection_config=_mysql_connection_config(mysql_service),
            migration_config={"script_location": script_location, "version_table_name": version_table_name},
        )

    return make


@pytest.fixture
def migration_config_aiomysql(mysql_service: MySQLService) -> Callable[..., Any]:
    """Build aiomysql configs for migration contract tests."""

    def make(*, script_location: str, version_table_name: str, suffix: str) -> AiomysqlConfig:
        return AiomysqlConfig(
            connection_config=_mysql_connection_config(mysql_service, database_key="db"),
            migration_config={"script_location": script_location, "version_table_name": version_table_name},
        )

    return make


@pytest.fixture
def migration_config_mysqlconnector_async(mysql_service: MySQLService) -> Callable[..., Any]:
    """Build mysql-connector async configs for migration contract tests."""

    def make(*, script_location: str, version_table_name: str, suffix: str) -> MysqlConnectorAsyncConfig:
        connection_config = _mysql_connection_config(mysql_service)
        connection_config["use_pure"] = True
        return MysqlConnectorAsyncConfig(
            connection_config=connection_config,
            migration_config={"script_location": script_location, "version_table_name": version_table_name},
        )

    return make


@pytest.fixture
def migration_config_psycopg_sync(postgres_service: PostgresService) -> Callable[..., Any]:
    """Build psycopg sync configs for migration contract tests."""

    def make(*, script_location: str, version_table_name: str, suffix: str) -> PsycopgSyncConfig:
        return PsycopgSyncConfig(
            connection_config={"conninfo": _postgres_conninfo(postgres_service)},
            migration_config={"script_location": script_location, "version_table_name": version_table_name},
        )

    return make


@pytest.fixture
def migration_config_psycopg_async(postgres_service: PostgresService) -> Callable[..., Any]:
    """Build psycopg async configs for migration contract tests."""

    def make(*, script_location: str, version_table_name: str, suffix: str) -> PsycopgAsyncConfig:
        return PsycopgAsyncConfig(
            connection_config={"conninfo": _postgres_conninfo(postgres_service)},
            migration_config={"script_location": script_location, "version_table_name": version_table_name},
        )

    return make


@pytest.fixture
def migration_config_psqlpy(postgres_service: PostgresService) -> Callable[..., Any]:
    """Build psqlpy configs for migration contract tests."""

    def make(*, script_location: str, version_table_name: str, suffix: str) -> PsqlpyConfig:
        return PsqlpyConfig(
            connection_config={"dsn": _psqlpy_dsn(postgres_service)},
            migration_config={"script_location": script_location, "version_table_name": version_table_name},
        )

    return make


def _resolve_migration_case(request: pytest.FixtureRequest, case: MigrationCase) -> MigrationCaseContext:
    return MigrationCaseContext(case=case, make_config=request.getfixturevalue(case.factory_fixture))


@pytest.fixture(params=SYNC_MIGRATION_PARAMS)
def sync_migration_case(request: pytest.FixtureRequest) -> MigrationCaseContext:
    """Resolve a sync migration contract case by factory fixture name."""
    return _resolve_migration_case(request, request.param)


@pytest.fixture(params=ASYNC_MIGRATION_PARAMS)
def async_migration_case(request: pytest.FixtureRequest) -> MigrationCaseContext:
    """Resolve an async migration contract case by factory fixture name."""
    return _resolve_migration_case(request, request.param)


_STORE_TABLE = "litestar_contract_sessions"
_STORE_EXTENSION_CONFIG: dict[str, Any] = {"litestar": {"session_table": _STORE_TABLE}}


@pytest.fixture
async def contract_sqlite_store() -> "AsyncGenerator[SQLiteStore, None]":
    """Provide a ready SQLite Litestar store for contract tests."""
    config = SqliteConfig(
        connection_config={"database": "file:contract_store_sqlite?mode=memory&cache=shared", "uri": True},
        extension_config=_STORE_EXTENSION_CONFIG,
    )
    store = SQLiteStore(config)
    await store.create_table()
    yield store
    with contextlib.suppress(Exception):
        await store.delete_all()
    config.close_pool()


@pytest.fixture
async def contract_aiosqlite_store() -> "AsyncGenerator[AiosqliteStore, None]":
    """Provide a ready aiosqlite Litestar store for contract tests."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"}, extension_config=_STORE_EXTENSION_CONFIG)
    store = AiosqliteStore(config)
    await store.create_table()
    yield store
    with contextlib.suppress(Exception):
        await store.delete_all()
    await config.close_pool()


@pytest.fixture
async def contract_duckdb_store(tmp_path: Path) -> "AsyncGenerator[DuckdbStore, None]":
    """Provide a ready DuckDB Litestar store for contract tests."""
    config = DuckDBConfig(
        connection_config={"database": str(tmp_path / f"store_{uuid4().hex}.duckdb")},
        extension_config=_STORE_EXTENSION_CONFIG,
    )
    store = DuckdbStore(config)
    await store.create_table()
    yield store
    with contextlib.suppress(Exception):
        await store.delete_all()
    config.close_pool()


@pytest.fixture
async def contract_asyncpg_store(postgres_service: PostgresService) -> "AsyncGenerator[AsyncpgStore, None]":
    """Provide a ready asyncpg Litestar store for contract tests."""
    config = AsyncpgConfig(
        connection_config=_postgres_connection_config(postgres_service), extension_config=_STORE_EXTENSION_CONFIG
    )
    store = AsyncpgStore(config)
    await store.create_table()
    yield store
    with contextlib.suppress(Exception):
        await store.delete_all()
    await config.close_pool()


@pytest.fixture
async def contract_psqlpy_store(postgres_service: PostgresService) -> "AsyncGenerator[PsqlpyStore, None]":
    """Provide a ready psqlpy Litestar store for contract tests."""
    config = PsqlpyConfig(
        connection_config={"dsn": _psqlpy_dsn(postgres_service)}, extension_config=_STORE_EXTENSION_CONFIG
    )
    store = PsqlpyStore(config)
    await store.create_table()
    yield store
    with contextlib.suppress(Exception):
        await store.delete_all()
    await config.close_pool()


@pytest.fixture
async def contract_psycopg_async_store(postgres_service: PostgresService) -> "AsyncGenerator[PsycopgAsyncStore, None]":
    """Provide a ready psycopg async Litestar store for contract tests."""
    config = PsycopgAsyncConfig(
        connection_config={"conninfo": _postgres_conninfo(postgres_service)}, extension_config=_STORE_EXTENSION_CONFIG
    )
    store = PsycopgAsyncStore(config)
    await store.create_table()
    yield store
    with contextlib.suppress(Exception):
        await store.delete_all()
    await config.close_pool()


@pytest.fixture
async def contract_psycopg_sync_store(postgres_service: PostgresService) -> "AsyncGenerator[PsycopgSyncStore, None]":
    """Provide a ready psycopg sync Litestar store for contract tests."""
    config = PsycopgSyncConfig(
        connection_config={"conninfo": _postgres_conninfo(postgres_service)}, extension_config=_STORE_EXTENSION_CONFIG
    )
    store = PsycopgSyncStore(config)
    await store.create_table()
    yield store
    with contextlib.suppress(Exception):
        await store.delete_all()
    config.close_pool()


@pytest.fixture
async def contract_aiomysql_store(mysql_service: MySQLService) -> "AsyncGenerator[AiomysqlStore, None]":
    """Provide a ready aiomysql Litestar store for contract tests."""
    config = AiomysqlConfig(
        connection_config=_mysql_connection_config(mysql_service, database_key="db"),
        extension_config=_STORE_EXTENSION_CONFIG,
    )
    store = AiomysqlStore(config)
    await store.create_table()
    yield store
    with contextlib.suppress(Exception):
        await store.delete_all()
    await config.close_pool()


@pytest.fixture
async def contract_asyncmy_store(mysql_service: MySQLService) -> "AsyncGenerator[AsyncmyStore, None]":
    """Provide a ready asyncmy Litestar store for contract tests."""
    config = AsyncmyConfig(
        connection_config=_mysql_connection_config(mysql_service), extension_config=_STORE_EXTENSION_CONFIG
    )
    store = AsyncmyStore(config)
    await store.create_table()
    yield store
    with contextlib.suppress(Exception):
        await store.delete_all()
    await config.close_pool()


@pytest.fixture
async def contract_mysqlconnector_async_store(
    mysql_service: MySQLService,
) -> "AsyncGenerator[MysqlConnectorAsyncStore, None]":
    """Provide a ready mysql-connector async Litestar store for contract tests."""
    connection_config = _mysql_connection_config(mysql_service)
    connection_config["use_pure"] = True
    config = MysqlConnectorAsyncConfig(connection_config=connection_config, extension_config=_STORE_EXTENSION_CONFIG)
    store = MysqlConnectorAsyncStore(config)
    await store.create_table()
    yield store
    with contextlib.suppress(Exception):
        await store.delete_all()
    await config.close_pool()


@pytest.fixture
async def contract_mysqlconnector_sync_store(
    mysql_service: MySQLService,
) -> "AsyncGenerator[MysqlConnectorSyncStore, None]":
    """Provide a ready mysql-connector sync Litestar store for contract tests."""
    connection_config = _mysql_connection_config(mysql_service)
    connection_config["use_pure"] = True
    config = MysqlConnectorSyncConfig(connection_config=connection_config, extension_config=_STORE_EXTENSION_CONFIG)
    store = MysqlConnectorSyncStore(config)
    await store.create_table()
    yield store
    with contextlib.suppress(Exception):
        await store.delete_all()
    config.close_pool()


@pytest.fixture
async def contract_pymysql_store(mysql_service: MySQLService) -> "AsyncGenerator[PyMysqlStore, None]":
    """Provide a ready PyMySQL Litestar store for contract tests."""
    config = PyMysqlConfig(
        connection_config=_mysql_connection_config(mysql_service), extension_config=_STORE_EXTENSION_CONFIG
    )
    store = PyMysqlStore(config)
    await store.create_table()
    yield store
    with contextlib.suppress(Exception):
        await store.delete_all()
    config.close_pool()


def _adk_extension_config(suffix: str) -> dict[str, Any]:
    return {"adk": {"session_table": f"adk_s_{suffix}", "events_table": f"adk_e_{suffix}"}}


@pytest.fixture
def adk_store_sqlite(tmp_path: Path) -> Callable[..., Any]:
    """Build a fresh SQLite ADK store with isolated tables per call."""

    def make() -> "tuple[Any, Any]":
        suffix = uuid4().hex[:8]
        config = SqliteConfig(
            connection_config={"database": str(tmp_path / f"adk_{suffix}.db")},
            extension_config=_adk_extension_config(suffix),
        )
        return config, SqliteADKStore(config)

    return make


@pytest.fixture
def adk_store_aiosqlite(tmp_path: Path) -> Callable[..., Any]:
    """Build a fresh aiosqlite ADK store with isolated tables per call."""

    def make() -> "tuple[Any, Any]":
        suffix = uuid4().hex[:8]
        config = AiosqliteConfig(
            connection_config={"database": str(tmp_path / f"adk_{suffix}.db")},
            extension_config=_adk_extension_config(suffix),
        )
        return config, AiosqliteADKStore(config)

    return make


@pytest.fixture
def adk_store_duckdb(tmp_path: Path) -> Callable[..., Any]:
    """Build a fresh DuckDB ADK store with isolated tables per call."""

    def make() -> "tuple[Any, Any]":
        suffix = uuid4().hex[:8]
        config = DuckDBConfig(
            connection_config={"database": str(tmp_path / f"adk_{suffix}.duckdb")},
            extension_config=_adk_extension_config(suffix),
        )
        return config, DuckdbADKStore(config)

    return make


@pytest.fixture
def adk_store_aiomysql(mysql_service: MySQLService) -> Callable[..., Any]:
    """Build a fresh aiomysql ADK store with isolated tables per call."""

    def make() -> "tuple[Any, Any]":
        suffix = uuid4().hex[:8]
        config = AiomysqlConfig(
            connection_config=_mysql_connection_config(mysql_service, database_key="db"),
            extension_config=_adk_extension_config(suffix),
        )
        return config, AiomysqlADKStore(config)

    return make


@pytest.fixture
def adk_store_asyncmy(mysql_service: MySQLService) -> Callable[..., Any]:
    """Build a fresh asyncmy ADK store with isolated tables per call."""

    def make() -> "tuple[Any, Any]":
        suffix = uuid4().hex[:8]
        config = AsyncmyConfig(
            connection_config=_mysql_connection_config(mysql_service), extension_config=_adk_extension_config(suffix)
        )
        return config, AsyncmyADKStore(config)

    return make


@pytest.fixture
def adk_store_mysqlconnector_async(mysql_service: MySQLService) -> Callable[..., Any]:
    """Build a fresh mysql-connector async ADK store with isolated tables per call."""

    def make() -> "tuple[Any, Any]":
        suffix = uuid4().hex[:8]
        connection_config = _mysql_connection_config(mysql_service)
        connection_config["use_pure"] = True
        config = MysqlConnectorAsyncConfig(
            connection_config=connection_config, extension_config=_adk_extension_config(suffix)
        )
        return config, MysqlConnectorAsyncADKStore(config)

    return make


@pytest.fixture
def adk_store_mysqlconnector_sync(mysql_service: MySQLService) -> Callable[..., Any]:
    """Build a fresh mysql-connector sync ADK store with isolated tables per call."""

    def make() -> "tuple[Any, Any]":
        suffix = uuid4().hex[:8]
        connection_config = _mysql_connection_config(mysql_service)
        connection_config["use_pure"] = True
        config = MysqlConnectorSyncConfig(
            connection_config=connection_config, extension_config=_adk_extension_config(suffix)
        )
        return config, MysqlConnectorSyncADKStore(config)

    return make


@pytest.fixture
def adk_store_asyncpg(postgres_service: PostgresService) -> Callable[..., Any]:
    """Build a fresh asyncpg ADK store with isolated tables per call."""

    def make() -> "tuple[Any, Any]":
        suffix = uuid4().hex[:8]
        config = AsyncpgConfig(
            connection_config=_postgres_connection_config(postgres_service),
            extension_config=_adk_extension_config(suffix),
        )
        return config, AsyncpgADKStore(config)

    return make


@pytest.fixture
def adk_store_psqlpy(postgres_service: PostgresService) -> Callable[..., Any]:
    """Build a fresh psqlpy ADK store with isolated tables per call."""

    def make() -> "tuple[Any, Any]":
        suffix = uuid4().hex[:8]
        config = PsqlpyConfig(
            connection_config={"dsn": _psqlpy_dsn(postgres_service)}, extension_config=_adk_extension_config(suffix)
        )
        return config, PsqlpyADKStore(config)

    return make


def _resolve_adk_store_case(request: pytest.FixtureRequest, case: AdkStoreCase) -> AdkStoreCaseContext:
    return AdkStoreCaseContext(case=case, make_store=request.getfixturevalue(case.factory_fixture))


@pytest.fixture(params=ADK_STORE_PARAMS)
def adk_store_case(request: pytest.FixtureRequest) -> AdkStoreCaseContext:
    """Resolve an ADK store contract case by factory fixture name."""
    return _resolve_adk_store_case(request, request.param)


def _resolve_store_case(request: pytest.FixtureRequest, case: StoreCase) -> StoreCaseContext:
    return StoreCaseContext(case=case, store=request.getfixturevalue(case.fixture_name))


@pytest.fixture(params=STORE_PARAMS)
def store_case(request: pytest.FixtureRequest) -> StoreCaseContext:
    """Resolve a Litestar store contract case by fixture name."""
    return _resolve_store_case(request, request.param)


def _resolve_driver_case(request: pytest.FixtureRequest, case: DriverCase) -> DriverCaseContext:
    return DriverCaseContext(case=case, driver=request.getfixturevalue(case.fixture_name))


@pytest.fixture(params=SYNC_DRIVER_PARAMS)
def sync_driver_case(request: pytest.FixtureRequest) -> DriverCaseContext:
    """Resolve a sync driver contract case by fixture name."""
    case = request.param
    return _resolve_driver_case(request, case)


@pytest.fixture(params=ASYNC_DRIVER_PARAMS)
def async_driver_case(request: pytest.FixtureRequest) -> DriverCaseContext:
    """Resolve an async driver contract case by fixture name."""
    case = request.param
    return _resolve_driver_case(request, case)


@pytest.fixture(params=DRIVER_PARAMS)
def driver_case(request: pytest.FixtureRequest) -> DriverCaseContext:
    """Resolve any driver contract case by fixture name for metadata-only contracts."""
    case = request.param
    return _resolve_driver_case(request, case)
