"""Fixtures for shared adapter contract tests."""

from collections.abc import AsyncGenerator, Generator

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.aiomysql import AiomysqlConfig, AiomysqlDriver
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver
from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.adapters.mysqlconnector import (
    MysqlConnectorAsyncConfig,
    MysqlConnectorAsyncDriver,
    MysqlConnectorSyncConfig,
    MysqlConnectorSyncDriver,
)
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
