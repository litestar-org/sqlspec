"""Fixtures for shared adapter contract tests."""

import contextlib
from collections.abc import AsyncGenerator, Generator
from typing import Any

import pytest
from pytest_databases.docker.cockroachdb import CockroachDBService
from pytest_databases.docker.mysql import MySQLService
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.aiomysql import AiomysqlConfig, AiomysqlDriver
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver
from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig, CockroachAsyncpgDriver
from sqlspec.adapters.cockroach_psycopg import (
    CockroachPsycopgAsyncConfig,
    CockroachPsycopgAsyncDriver,
    CockroachPsycopgSyncConfig,
    CockroachPsycopgSyncDriver,
)
from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.adapters.mysqlconnector import (
    MysqlConnectorAsyncConfig,
    MysqlConnectorAsyncDriver,
    MysqlConnectorSyncConfig,
    MysqlConnectorSyncDriver,
)
from sqlspec.adapters.psqlpy import PsqlpyConfig, PsqlpyDriver
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgAsyncDriver, PsycopgSyncConfig, PsycopgSyncDriver
from sqlspec.adapters.pymysql import PyMysqlConfig, PyMysqlDriver
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from tests.integration.adapters.contracts._cases import (
    ASYNC_DRIVER_PARAMS,
    DRIVER_PARAMS,
    SYNC_DRIVER_PARAMS,
    DriverCase,
    DriverCaseContext,
)
from tests.integration.adapters.contracts._schema import (
    DEFAULT_CONTRACT_TABLE,
    DUCKDB_CONTRACT_TABLE,
    MYSQL_CONTRACT_TABLE,
    POSTGRES_CONTRACT_TABLE,
)


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
    config = AsyncpgConfig(
        connection_config={**_postgres_connection_config(postgres_service), "min_size": 1, "max_size": 5}
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
