"""Fixtures for shared adapter contract tests."""

import contextlib
from collections.abc import AsyncGenerator, Callable, Generator
from dataclasses import replace
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from pytest_databases.docker.bigquery import BigQueryService
from pytest_databases.docker.cockroachdb import CockroachDBService
from pytest_databases.docker.mssql import MSSQLService
from pytest_databases.docker.mysql import MySQLService
from pytest_databases.docker.oracle import OracleService
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.adapters.adbc.adk import AdbcADKStore
from sqlspec.adapters.adbc.litestar import ADBCStore
from sqlspec.adapters.aiomysql import AiomysqlConfig, AiomysqlDriver, AiomysqlDriverFeatures
from sqlspec.adapters.aiomysql.adk import AiomysqlADKStore
from sqlspec.adapters.aiomysql.litestar import AiomysqlStore
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver, AiosqliteDriverFeatures
from sqlspec.adapters.aiosqlite.adk import AiosqliteADKStore
from sqlspec.adapters.aiosqlite.litestar import AiosqliteStore
from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig, ArrowOdbcDriver
from sqlspec.adapters.arrow_odbc.adk import ArrowOdbcADKStore
from sqlspec.adapters.arrow_odbc.litestar import ArrowOdbcStore
from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver, AsyncmyDriverFeatures
from sqlspec.adapters.asyncmy.adk import AsyncmyADKStore
from sqlspec.adapters.asyncmy.litestar import AsyncmyStore
from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver, AsyncpgDriverFeatures
from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore
from sqlspec.adapters.asyncpg.litestar import AsyncpgStore
from sqlspec.adapters.bigquery import BigQueryConfig, BigQueryDriver, BigQueryDriverFeatures
from sqlspec.adapters.cockroach_asyncpg import (
    CockroachAsyncpgConfig,
    CockroachAsyncpgDriver,
    CockroachAsyncpgDriverFeatures,
)
from sqlspec.adapters.cockroach_asyncpg.adk import CockroachAsyncpgADKStore
from sqlspec.adapters.cockroach_psycopg import (
    CockroachPsycopgAsyncConfig,
    CockroachPsycopgAsyncDriver,
    CockroachPsycopgDriverFeatures,
    CockroachPsycopgSyncConfig,
    CockroachPsycopgSyncDriver,
)
from sqlspec.adapters.cockroach_psycopg.adk import CockroachPsycopgAsyncADKStore, CockroachPsycopgSyncADKStore
from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver, DuckDBDriverFeatures
from sqlspec.adapters.duckdb.adk import DuckdbADKStore
from sqlspec.adapters.duckdb.litestar import DuckdbStore
from sqlspec.adapters.mysqlconnector import (
    MysqlConnectorAsyncConfig,
    MysqlConnectorAsyncDriver,
    MysqlConnectorDriverFeatures,
    MysqlConnectorSyncConfig,
    MysqlConnectorSyncDriver,
)
from sqlspec.adapters.mysqlconnector.adk import MysqlConnectorAsyncADKStore, MysqlConnectorSyncADKStore
from sqlspec.adapters.mysqlconnector.litestar import MysqlConnectorAsyncStore, MysqlConnectorSyncStore
from sqlspec.adapters.oracledb import (
    OracleAsyncConfig,
    OracleAsyncDriver,
    OracleDriverFeatures,
    OraclePoolParams,
    OracleSyncConfig,
    OracleSyncDriver,
)
from sqlspec.adapters.oracledb.adk import OracleAsyncADKStore, OracleSyncADKStore
from sqlspec.adapters.oracledb.litestar import OracleAsyncStore, OracleSyncStore
from sqlspec.adapters.psqlpy import PsqlpyConfig, PsqlpyDriver, PsqlpyDriverFeatures, PsqlpyPoolParams
from sqlspec.adapters.psqlpy.adk import PsqlpyADKStore
from sqlspec.adapters.psqlpy.litestar import PsqlpyStore
from sqlspec.adapters.psycopg import (
    PsycopgAsyncConfig,
    PsycopgAsyncDriver,
    PsycopgDriverFeatures,
    PsycopgSyncConfig,
    PsycopgSyncDriver,
)
from sqlspec.adapters.psycopg.adk import PsycopgAsyncADKStore, PsycopgSyncADKStore
from sqlspec.adapters.psycopg.litestar import PsycopgAsyncStore, PsycopgSyncStore
from sqlspec.adapters.pymysql import PyMysqlConfig, PyMysqlDriver, PyMysqlDriverFeatures
from sqlspec.adapters.pymysql.adk import PyMysqlADKStore
from sqlspec.adapters.pymysql.litestar import PyMysqlStore
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver, SqliteDriverFeatures
from sqlspec.adapters.sqlite.adk import SqliteADKStore
from sqlspec.adapters.sqlite.litestar import SQLiteStore
from tests.integration.adapters.bigquery._wedge import describe_wedge, is_emulator_wedge
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
    ASYNC_LISTEN_NOTIFY_PARAMS,
    SYNC_EVENTS_PARAMS,
    SYNC_LISTEN_NOTIFY_PARAMS,
    EventsCase,
    EventsCaseContext,
    ListenNotifyCase,
    ListenNotifyCaseContext,
)
from tests.integration.adapters.contracts._migration_cases import (
    ASYNC_MIGRATION_PARAMS,
    SYNC_MIGRATION_PARAMS,
    MigrationCase,
    MigrationCaseContext,
)
from tests.integration.adapters.contracts._postgres_extension_cases import (
    PostgresExtensionCase,
    PostgresExtensionCaseContext,
)
from tests.integration.adapters.contracts._schema import (
    DEFAULT_CONTRACT_TABLE,
    DUCKDB_CONTRACT_TABLE,
    MSSQL_CONTRACT_TABLE,
    MYSQL_CONTRACT_TABLE,
    ORACLE_CONTRACT_TABLE,
    POSTGRES_CONTRACT_TABLE,
    ContractTable,
    build_bigquery_contract_table,
)
from tests.integration.adapters.contracts._store_cases import STORE_PARAMS, StoreCase, StoreCaseContext
from tests.integration.fixtures.mysql import _mysql_connection_config
from tests.integration.fixtures.postgres import (
    _adbc_postgres_uri,
    _asyncpg_pool_config,
    _cockroach_asyncpg_connection_config,
    _cockroach_conninfo,
    _postgres_connection_config,
    _postgres_conninfo,
    _psqlpy_dsn,
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


_ADBC_DRIVER_MISSING_MARKERS = (
    "cannot open shared object file",
    "No module named",
    "Failed to import connect function",
    "Could not configure connection",
)


@contextlib.contextmanager
def _provide_adbc_contract_driver(config: AdbcConfig, table: ContractTable) -> Generator[AdbcDriver, None, None]:
    try:
        with config.provide_session() as driver:
            driver.execute_script("DROP TABLE IF EXISTS contract_items")
            driver.execute_script(table.create_sql)
            driver.commit()
            yield driver
    except Exception as exc:
        if any(marker in str(exc) for marker in _ADBC_DRIVER_MISSING_MARKERS):
            pytest.skip(f"ADBC driver not available: {exc}")
        raise


@pytest.fixture
def contract_adbc_sqlite_driver() -> Generator[AdbcDriver, None, None]:
    """Provide a fresh ADBC SQLite driver for contract tests."""
    config = AdbcConfig(connection_config={"uri": ":memory:", "driver_name": "adbc_driver_sqlite"})
    try:
        with _provide_adbc_contract_driver(config, DEFAULT_CONTRACT_TABLE) as driver:
            yield driver
    finally:
        config.close_pool()


@pytest.fixture
def contract_adbc_duckdb_driver() -> Generator[AdbcDriver, None, None]:
    """Provide a fresh ADBC DuckDB driver for contract tests."""
    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_duckdb.dbapi.connect"})
    try:
        with _provide_adbc_contract_driver(config, DUCKDB_CONTRACT_TABLE) as driver:
            yield driver
    finally:
        config.close_pool()


@pytest.fixture
def contract_adbc_postgres_driver(postgres_service: PostgresService) -> Generator[AdbcDriver, None, None]:
    """Provide a fresh ADBC PostgreSQL driver for contract tests."""
    config = AdbcConfig(
        connection_config={"uri": _adbc_postgres_uri(postgres_service), "driver_name": "adbc_driver_postgresql"}
    )
    try:
        with _provide_adbc_contract_driver(config, POSTGRES_CONTRACT_TABLE) as driver:
            yield driver
    finally:
        config.close_pool()


@pytest.fixture
def bigquery_contract_table(bigquery_service: BigQueryService) -> ContractTable:
    """Resolve a unique fully-qualified BigQuery ContractTable for one contract test."""
    table_name = f"contract_items_{uuid4().hex[:8]}"
    return build_bigquery_contract_table(f"`{bigquery_service.project}.{bigquery_service.dataset}.{table_name}`")


@pytest.fixture(scope="session")
def _bigquery_contract_session(bigquery_service: BigQueryService) -> "Generator[BigQueryDriver, None, None]":
    """Session-scoped BigQuery driver for contract tests.

    The emulator is unreliable under repeated DDL and hangs on default-dataset job
    config, so the client is reused across the xdist group, a dotted dataset_id keeps
    the default dataset unset, and tables are referenced fully-qualified.
    """
    config = BigQueryConfig(
        connection_config={
            "project": bigquery_service.project,
            "dataset_id": f"`{bigquery_service.project}`.`{bigquery_service.dataset}`",
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
            "credentials": AnonymousCredentials(),  # type: ignore[no-untyped-call]
        },
        driver_features={"job_result_timeout": 30.0, "job_retry_deadline": 0.0, "request_timeout": 15.0},
    )
    try:
        with config.provide_session() as driver:
            yield driver
    finally:
        config.close_pool()


_bigquery_wedge_reason: "str | None" = None


@pytest.fixture
def contract_bigquery_driver(
    _bigquery_contract_session: BigQueryDriver, bigquery_contract_table: ContractTable
) -> BigQueryDriver:
    """Provide the session BigQuery driver with an isolated contract table per test.

    Once the emulator wedges (accepts requests but never responds), every call
    fails after the request timeout; skip the remaining BigQuery contract tests
    instead of paying that timeout per test.
    """
    global _bigquery_wedge_reason
    if _bigquery_wedge_reason is not None:
        pytest.skip(f"BigQuery emulator wedged earlier in this session ({_bigquery_wedge_reason})")
    try:
        _bigquery_contract_session.execute_script(bigquery_contract_table.create_sql)
    except Exception as error:
        if is_emulator_wedge(error):
            _bigquery_wedge_reason = describe_wedge(error)
            pytest.skip(f"BigQuery emulator wedged earlier in this session ({_bigquery_wedge_reason})")
        raise
    return _bigquery_contract_session


def _oracle_pool_params(oracle_service: OracleService) -> OraclePoolParams:
    return OraclePoolParams(
        host=oracle_service.host,
        port=oracle_service.port,
        service_name=oracle_service.service_name,
        user=oracle_service.user,
        password=oracle_service.password,
        min=1,
        max=5,
    )


@pytest.fixture
def contract_oracle_sync_driver(oracle_23ai_service: OracleService) -> Generator[OracleSyncDriver, None, None]:
    """Provide a fresh Oracle sync driver for contract tests."""
    config = OracleSyncConfig(connection_config=_oracle_pool_params(oracle_23ai_service))
    try:
        with config.provide_session() as driver:
            driver.execute_script("DROP TABLE IF EXISTS contract_items")
            driver.execute_script(ORACLE_CONTRACT_TABLE.create_sql)
            driver.commit()
            yield driver
    finally:
        config.close_pool()


@pytest.fixture
def contract_arrow_odbc_mssql_driver(mssql_service: MSSQLService) -> Generator[ArrowOdbcDriver, None, None]:
    """Provide a fresh arrow-odbc driver backed by SQL Server."""
    config = ArrowOdbcConfig(
        connection_config={"connection_string": mssql_service.connection_string},
        driver_features={"dbms_name": "Microsoft SQL Server"},
    )
    try:
        with config.provide_session() as driver:
            driver.execute_script("DROP TABLE IF EXISTS contract_items")
            driver.execute_script(MSSQL_CONTRACT_TABLE.create_sql)
            driver.commit()
            yield driver
            with contextlib.suppress(Exception):
                driver.rollback()
            driver.execute_script("DROP TABLE IF EXISTS contract_items")
            driver.commit()
    finally:
        config.close_pool()


@pytest.fixture
async def contract_oracle_async_driver(oracle_23ai_service: OracleService) -> "AsyncGenerator[OracleAsyncDriver, None]":
    """Provide a fresh Oracle async driver for contract tests."""
    config = OracleAsyncConfig(connection_config=_oracle_pool_params(oracle_23ai_service))
    try:
        async with config.provide_session() as driver:
            await driver.execute_script("DROP TABLE IF EXISTS contract_items")
            await driver.execute_script(ORACLE_CONTRACT_TABLE.create_sql)
            await driver.commit()
            yield driver
    finally:
        await config.close_pool()


@pytest.fixture
def contract_mysqlconnector_sync_driver(mysql_service: MySQLService) -> Generator[MysqlConnectorSyncDriver, None, None]:
    """Provide a fresh mysql-connector sync driver for contract tests."""
    connection_config = _mysql_connection_config(mysql_service)
    connection_config.update({"use_pure": True, "pool_size": 5})
    config = MysqlConnectorSyncConfig(connection_config=connection_config)
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
    config = PyMysqlConfig(connection_config=_mysql_connection_config(mysql_service))
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
    connection_config = _mysql_connection_config(mysql_service, database_key="db")
    connection_config.update({"minsize": 1, "maxsize": 5})
    config = AiomysqlConfig(connection_config=connection_config)
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
    connection_config = _mysql_connection_config(mysql_service)
    connection_config.update({"minsize": 1, "maxsize": 5})
    config = AsyncmyConfig(connection_config=connection_config)
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
    connection_config = _mysql_connection_config(mysql_service)
    connection_config["use_pure"] = True
    config = MysqlConnectorAsyncConfig(connection_config=connection_config)
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
    connection_config = _asyncpg_pool_config(postgres_service)
    connection_config.update({"min_size": 1, "max_size": 5})
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
    connection_config = _cockroach_asyncpg_connection_config(cockroachdb_service)
    connection_config.update({"min_size": 1, "max_size": 5})
    config = CockroachAsyncpgConfig(connection_config=connection_config)
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


@pytest.fixture
def events_config_arrow_odbc_mssql(mssql_service: MSSQLService, tmp_path: Path) -> Callable[..., Any]:
    """Build arrow-odbc SQL Server event-channel configs for contract tests."""

    def make(*, extension_config: dict[str, Any], suffix: str) -> ArrowOdbcConfig:
        return ArrowOdbcConfig(
            connection_config={"connection_string": mssql_service.connection_string},
            migration_config=_events_migration_config(tmp_path, suffix),
            extension_config=extension_config,
            driver_features={"dbms_name": "Microsoft SQL Server"},
        )

    return make


@pytest.fixture
def events_config_oracle_sync(oracle_23ai_service: OracleService, tmp_path: Path) -> Callable[..., Any]:
    """Build Oracle sync event-channel configs for contract tests."""

    def make(*, extension_config: dict[str, Any], suffix: str) -> OracleSyncConfig:
        return OracleSyncConfig(
            connection_config=_oracle_pool_params(oracle_23ai_service),
            migration_config=_events_migration_config(tmp_path, suffix),
            extension_config=extension_config,
        )

    return make


@pytest.fixture
def events_config_oracle_async(oracle_23ai_service: OracleService, tmp_path: Path) -> Callable[..., Any]:
    """Build Oracle async event-channel configs for contract tests."""

    def make(*, extension_config: dict[str, Any], suffix: str) -> OracleAsyncConfig:
        return OracleAsyncConfig(
            connection_config=_oracle_pool_params(oracle_23ai_service),
            migration_config=_events_migration_config(tmp_path, suffix),
            extension_config=extension_config,
        )

    return make


@pytest.fixture
def listen_notify_config_asyncpg(postgres_service: PostgresService) -> Callable[..., Any]:
    """Build asyncpg native LISTEN/NOTIFY configs for contract tests."""

    def make(*, suffix: str) -> AsyncpgConfig:
        return AsyncpgConfig(
            connection_config={"dsn": _postgres_conninfo(postgres_service)},
            extension_config={"events": {"backend": "notify"}},
        )

    return make


@pytest.fixture
def listen_notify_config_psqlpy(postgres_service: PostgresService) -> Callable[..., Any]:
    """Build psqlpy native LISTEN/NOTIFY configs for contract tests."""

    def make(*, suffix: str) -> PsqlpyConfig:
        return PsqlpyConfig(
            connection_config=PsqlpyPoolParams(dsn=_psqlpy_dsn(postgres_service)),
            extension_config={"events": {"backend": "notify"}},
        )

    return make


@pytest.fixture
def listen_notify_config_psycopg_sync(postgres_service: PostgresService) -> Callable[..., Any]:
    """Build psycopg sync native LISTEN/NOTIFY configs for contract tests."""

    def make(*, suffix: str) -> PsycopgSyncConfig:
        return PsycopgSyncConfig(
            connection_config={"conninfo": _postgres_conninfo(postgres_service)},
            extension_config={"events": {"backend": "notify"}},
        )

    return make


@pytest.fixture
def listen_notify_config_psycopg_async(postgres_service: PostgresService) -> Callable[..., Any]:
    """Build psycopg async native LISTEN/NOTIFY configs for contract tests."""

    def make(*, suffix: str) -> PsycopgAsyncConfig:
        return PsycopgAsyncConfig(
            connection_config={"conninfo": _postgres_conninfo(postgres_service)},
            extension_config={"events": {"backend": "notify"}},
        )

    return make


def _resolve_events_case(request: pytest.FixtureRequest, case: EventsCase) -> EventsCaseContext:
    return EventsCaseContext(case=case, make_config=request.getfixturevalue(case.factory_fixture))


def _resolve_listen_notify_case(request: pytest.FixtureRequest, case: ListenNotifyCase) -> ListenNotifyCaseContext:
    return ListenNotifyCaseContext(case=case, make_config=request.getfixturevalue(case.factory_fixture))


@pytest.fixture(params=SYNC_EVENTS_PARAMS)
def sync_events_case(request: pytest.FixtureRequest) -> EventsCaseContext:
    """Resolve a sync event-channel contract case by factory fixture name."""
    return _resolve_events_case(request, request.param)


@pytest.fixture(params=ASYNC_EVENTS_PARAMS)
def async_events_case(request: pytest.FixtureRequest) -> EventsCaseContext:
    """Resolve an async event-channel contract case by factory fixture name."""
    return _resolve_events_case(request, request.param)


@pytest.fixture(params=SYNC_LISTEN_NOTIFY_PARAMS)
def sync_listen_notify_case(request: pytest.FixtureRequest) -> ListenNotifyCaseContext:
    """Resolve a sync native LISTEN/NOTIFY contract case by factory fixture name."""
    return _resolve_listen_notify_case(request, request.param)


@pytest.fixture(params=ASYNC_LISTEN_NOTIFY_PARAMS)
def async_listen_notify_case(request: pytest.FixtureRequest) -> ListenNotifyCaseContext:
    """Resolve an async native LISTEN/NOTIFY contract case by factory fixture name."""
    return _resolve_listen_notify_case(request, request.param)


def _lifecycle_connection_config(
    database: str, *, pooled: bool, connection_overrides: "dict[str, Any] | None" = None
) -> "dict[str, Any]":
    """Build a connection_config dict, adding pool sizing keys (a superset of the typed params)."""
    connection_config: dict[str, Any] = {"database": database}
    if pooled:
        connection_config.update({"pool_min_size": 2, "pool_max_size": 5})
    if connection_overrides:
        connection_config.update(connection_overrides)
    return connection_config


@pytest.fixture
def lifecycle_config_sqlite(tmp_path: Path) -> "Callable[..., SqliteConfig]":
    """Build fresh SQLite configs for the pooling/connection-hook and driver-feature contracts."""

    def make(
        *,
        pooled: bool = False,
        driver_features: "SqliteDriverFeatures | None" = None,
        connection_overrides: "dict[str, Any] | None" = None,
        connection_instance: object | None = None,
    ) -> SqliteConfig:
        connection_config = _lifecycle_connection_config(
            str(tmp_path / "lifecycle.db"), pooled=pooled, connection_overrides=connection_overrides
        )
        extra: dict[str, Any] = {}
        if driver_features is not None:
            extra["driver_features"] = driver_features
        if connection_instance is not None:
            extra["connection_instance"] = connection_instance
        return SqliteConfig(connection_config=connection_config, **extra)

    return make


@pytest.fixture
def lifecycle_config_aiosqlite(tmp_path: Path) -> "Callable[..., AiosqliteConfig]":
    """Build fresh aiosqlite configs for the pooling/connection-hook lifecycle contracts."""

    def make(
        *,
        pooled: bool = False,
        driver_features: "AiosqliteDriverFeatures | None" = None,
        connection_overrides: "dict[str, Any] | None" = None,
        connection_instance: object | None = None,
    ) -> AiosqliteConfig:
        connection_config = _lifecycle_connection_config(
            str(tmp_path / "lifecycle_aiosqlite.db"), pooled=pooled, connection_overrides=connection_overrides
        )
        extra: dict[str, Any] = {}
        if driver_features is not None:
            extra["driver_features"] = driver_features
        if connection_instance is not None:
            extra["connection_instance"] = connection_instance
        return AiosqliteConfig(connection_config=connection_config, **extra)

    return make


@pytest.fixture
def lifecycle_config_duckdb(tmp_path: Path) -> "Callable[..., DuckDBConfig]":
    """Build fresh DuckDB configs for the pooling/connection-hook lifecycle contracts."""

    def make(
        *,
        pooled: bool = False,
        driver_features: "DuckDBDriverFeatures | None" = None,
        connection_instance: object | None = None,
    ) -> DuckDBConfig:
        connection_config = _lifecycle_connection_config(str(tmp_path / "lifecycle.duckdb"), pooled=pooled)
        extra: dict[str, Any] = {}
        if driver_features is not None:
            extra["driver_features"] = driver_features
        if connection_instance is not None:
            extra["connection_instance"] = connection_instance
        return DuckDBConfig(connection_config=connection_config, **extra)

    return make


@pytest.fixture
def lifecycle_config_asyncpg(postgres_service: PostgresService) -> "Callable[..., AsyncpgConfig]":
    """Build fresh asyncpg configs for the pooling/connection-hook lifecycle contracts."""

    def make(
        *,
        pooled: bool = False,
        driver_features: "AsyncpgDriverFeatures | None" = None,
        connection_instance: object | None = None,
    ) -> AsyncpgConfig:
        connection_config: dict[str, Any] = _postgres_connection_config(postgres_service)
        if pooled:
            connection_config.update({"min_size": 2, "max_size": 5})
        extra: dict[str, Any] = {}
        if driver_features is not None:
            extra["driver_features"] = driver_features
        if connection_instance is not None:
            extra["connection_instance"] = connection_instance
        return AsyncpgConfig(connection_config=connection_config, **extra)

    return make


@pytest.fixture
def lifecycle_config_psqlpy(postgres_service: PostgresService) -> "Callable[..., PsqlpyConfig]":
    """Build fresh psqlpy configs for the pooling/connection-hook lifecycle contracts."""

    def make(
        *,
        pooled: bool = False,
        driver_features: "PsqlpyDriverFeatures | None" = None,
        connection_instance: object | None = None,
    ) -> PsqlpyConfig:
        connection_config: dict[str, Any] = {"dsn": _psqlpy_dsn(postgres_service)}
        if pooled:
            connection_config["max_db_pool_size"] = 5
        extra: dict[str, Any] = {}
        if driver_features is not None:
            extra["driver_features"] = driver_features
        if connection_instance is not None:
            extra["connection_instance"] = connection_instance
        return PsqlpyConfig(connection_config=connection_config, **extra)

    return make


@pytest.fixture
def lifecycle_config_psycopg_sync(postgres_service: PostgresService) -> "Callable[..., PsycopgSyncConfig]":
    """Build fresh psycopg sync configs for the pooling/connection-hook lifecycle contracts."""

    def make(
        *,
        pooled: bool = False,
        driver_features: "PsycopgDriverFeatures | None" = None,
        connection_overrides: "dict[str, Any] | None" = None,
        connection_instance: object | None = None,
    ) -> PsycopgSyncConfig:
        connection_config: dict[str, Any] = {"conninfo": _postgres_conninfo(postgres_service), "autocommit": True}
        if connection_overrides:
            connection_config.update(connection_overrides)
        if pooled:
            connection_config.update({"min_size": 2, "max_size": 5})
        extra: dict[str, Any] = {}
        if driver_features is not None:
            extra["driver_features"] = driver_features
        if connection_instance is not None:
            extra["connection_instance"] = connection_instance
        return PsycopgSyncConfig(connection_config=connection_config, **extra)

    return make


@pytest.fixture
def lifecycle_config_psycopg_async(postgres_service: PostgresService) -> "Callable[..., PsycopgAsyncConfig]":
    """Build fresh psycopg async configs for the pooling/connection-hook lifecycle contracts."""

    def make(
        *,
        pooled: bool = False,
        driver_features: "PsycopgDriverFeatures | None" = None,
        connection_overrides: "dict[str, Any] | None" = None,
        connection_instance: object | None = None,
    ) -> PsycopgAsyncConfig:
        connection_config: dict[str, Any] = {"conninfo": _postgres_conninfo(postgres_service), "autocommit": True}
        if connection_overrides:
            connection_config.update(connection_overrides)
        if pooled:
            connection_config.update({"min_size": 2, "max_size": 5})
        extra: dict[str, Any] = {}
        if driver_features is not None:
            extra["driver_features"] = driver_features
        if connection_instance is not None:
            extra["connection_instance"] = connection_instance
        return PsycopgAsyncConfig(connection_config=connection_config, **extra)

    return make


@pytest.fixture
def lifecycle_config_cockroach_asyncpg(
    cockroachdb_service: CockroachDBService,
) -> "Callable[..., CockroachAsyncpgConfig]":
    """Build fresh CockroachDB asyncpg configs for the pooling/connection-hook lifecycle contracts."""

    def make(
        *,
        pooled: bool = False,
        driver_features: "CockroachAsyncpgDriverFeatures | None" = None,
        connection_instance: object | None = None,
    ) -> CockroachAsyncpgConfig:
        connection_config = _cockroach_asyncpg_connection_config(cockroachdb_service)
        if pooled:
            connection_config.update({"min_size": 2, "max_size": 5})
        extra: dict[str, Any] = {}
        if driver_features is not None:
            extra["driver_features"] = driver_features
        if connection_instance is not None:
            extra["connection_instance"] = connection_instance
        return CockroachAsyncpgConfig(connection_config=connection_config, **extra)

    return make


@pytest.fixture
def lifecycle_config_cockroach_psycopg_sync(
    cockroachdb_service: CockroachDBService,
) -> "Callable[..., CockroachPsycopgSyncConfig]":
    """Build fresh CockroachDB psycopg sync configs for the pooling/connection-hook lifecycle contracts."""

    def make(
        *,
        pooled: bool = False,
        driver_features: "CockroachPsycopgDriverFeatures | None" = None,
        connection_instance: object | None = None,
    ) -> CockroachPsycopgSyncConfig:
        connection_config: dict[str, Any] = {"conninfo": _cockroach_conninfo(cockroachdb_service)}
        if pooled:
            connection_config.update({"min_size": 2, "max_size": 5})
        extra: dict[str, Any] = {}
        if driver_features is not None:
            extra["driver_features"] = driver_features
        if connection_instance is not None:
            extra["connection_instance"] = connection_instance
        return CockroachPsycopgSyncConfig(connection_config=connection_config, **extra)

    return make


@pytest.fixture
def lifecycle_config_cockroach_psycopg_async(
    cockroachdb_service: CockroachDBService,
) -> "Callable[..., CockroachPsycopgAsyncConfig]":
    """Build fresh CockroachDB psycopg async configs for the pooling/connection-hook lifecycle contracts."""

    def make(
        *,
        pooled: bool = False,
        driver_features: "CockroachPsycopgDriverFeatures | None" = None,
        connection_instance: object | None = None,
    ) -> CockroachPsycopgAsyncConfig:
        connection_config: dict[str, Any] = {"conninfo": _cockroach_conninfo(cockroachdb_service)}
        if pooled:
            connection_config.update({"min_size": 2, "max_size": 5})
        extra: dict[str, Any] = {}
        if driver_features is not None:
            extra["driver_features"] = driver_features
        if connection_instance is not None:
            extra["connection_instance"] = connection_instance
        return CockroachPsycopgAsyncConfig(connection_config=connection_config, **extra)

    return make


@pytest.fixture
def lifecycle_config_aiomysql(mysql_service: MySQLService) -> "Callable[..., AiomysqlConfig]":
    """Build fresh aiomysql configs for the pooling/connection-hook lifecycle contracts."""

    def make(
        *,
        pooled: bool = False,
        driver_features: "AiomysqlDriverFeatures | None" = None,
        connection_instance: object | None = None,
    ) -> AiomysqlConfig:
        connection_config = _mysql_connection_config(mysql_service, database_key="db")
        if pooled:
            connection_config.update({"minsize": 2, "maxsize": 5})
        extra: dict[str, Any] = {}
        if driver_features is not None:
            extra["driver_features"] = driver_features
        if connection_instance is not None:
            extra["connection_instance"] = connection_instance
        return AiomysqlConfig(connection_config=connection_config, **extra)

    return make


@pytest.fixture
def lifecycle_config_asyncmy(mysql_service: MySQLService) -> "Callable[..., AsyncmyConfig]":
    """Build fresh asyncmy configs for the pooling/connection-hook lifecycle contracts."""

    def make(
        *,
        pooled: bool = False,
        driver_features: "AsyncmyDriverFeatures | None" = None,
        connection_instance: object | None = None,
    ) -> AsyncmyConfig:
        connection_config = _mysql_connection_config(mysql_service)
        if pooled:
            connection_config.update({"minsize": 2, "maxsize": 5})
        extra: dict[str, Any] = {}
        if driver_features is not None:
            extra["driver_features"] = driver_features
        if connection_instance is not None:
            extra["connection_instance"] = connection_instance
        return AsyncmyConfig(connection_config=connection_config, **extra)

    return make


@pytest.fixture
def lifecycle_config_mysqlconnector_sync(mysql_service: MySQLService) -> "Callable[..., MysqlConnectorSyncConfig]":
    """Build fresh mysql-connector sync configs for the pooling/connection-hook lifecycle contracts."""

    def make(
        *,
        pooled: bool = False,
        driver_features: "MysqlConnectorDriverFeatures | None" = None,
        connection_instance: object | None = None,
    ) -> MysqlConnectorSyncConfig:
        connection_config = _mysql_connection_config(mysql_service)
        connection_config["use_pure"] = True
        if pooled:
            connection_config["pool_size"] = 5
        extra: dict[str, Any] = {}
        if driver_features is not None:
            extra["driver_features"] = driver_features
        if connection_instance is not None:
            extra["connection_instance"] = connection_instance
        return MysqlConnectorSyncConfig(connection_config=connection_config, **extra)

    return make


@pytest.fixture
def lifecycle_config_mysqlconnector_async(mysql_service: MySQLService) -> "Callable[..., MysqlConnectorAsyncConfig]":
    """Build fresh mysql-connector async configs for the connection-hook lifecycle contract (no pooling)."""

    def make(
        *, pooled: bool = False, driver_features: "MysqlConnectorDriverFeatures | None" = None
    ) -> MysqlConnectorAsyncConfig:
        connection_config = _mysql_connection_config(mysql_service)
        connection_config["use_pure"] = True
        if driver_features is None:
            return MysqlConnectorAsyncConfig(connection_config=connection_config)
        return MysqlConnectorAsyncConfig(connection_config=connection_config, driver_features=driver_features)

    return make


@pytest.fixture
def lifecycle_config_pymysql(mysql_service: MySQLService) -> "Callable[..., PyMysqlConfig]":
    """Build fresh PyMySQL configs for the pooling/connection-hook lifecycle contracts (thread-local pool)."""

    def make(
        *,
        pooled: bool = False,
        driver_features: "PyMysqlDriverFeatures | None" = None,
        connection_instance: object | None = None,
    ) -> PyMysqlConfig:
        connection_config = _mysql_connection_config(mysql_service)
        extra: dict[str, Any] = {}
        if driver_features is not None:
            extra["driver_features"] = driver_features
        if connection_instance is not None:
            extra["connection_instance"] = connection_instance
        return PyMysqlConfig(connection_config=connection_config, **extra)

    return make


@pytest.fixture
def lifecycle_config_bigquery(bigquery_service: BigQueryService) -> "Callable[..., BigQueryConfig]":
    """Build fresh BigQuery configs for the connection-hook lifecycle contract (no pooling)."""

    def make(*, pooled: bool = False, driver_features: "BigQueryDriverFeatures | None" = None) -> BigQueryConfig:
        connection_config: dict[str, Any] = {
            "project": bigquery_service.project,
            "dataset_id": f"`{bigquery_service.project}`.`{bigquery_service.dataset}`",
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
            "credentials": AnonymousCredentials(),  # type: ignore[no-untyped-call]
        }
        if driver_features is None:
            return BigQueryConfig(connection_config=connection_config)
        return BigQueryConfig(connection_config=connection_config, driver_features=driver_features)

    return make


@pytest.fixture
def lifecycle_config_oracle_sync(oracle_23ai_service: OracleService) -> "Callable[..., OracleSyncConfig]":
    """Build fresh Oracle sync configs for the pooling/connection-hook lifecycle contracts."""

    def make(
        *,
        pooled: bool = False,
        driver_features: "OracleDriverFeatures | None" = None,
        connection_instance: object | None = None,
    ) -> OracleSyncConfig:
        connection_config = _oracle_pool_params(oracle_23ai_service)
        extra: dict[str, Any] = {}
        if driver_features is not None:
            extra["driver_features"] = driver_features
        if connection_instance is not None:
            extra["connection_instance"] = connection_instance
        return OracleSyncConfig(connection_config=connection_config, **extra)

    return make


@pytest.fixture
def lifecycle_config_oracle_async(oracle_23ai_service: OracleService) -> "Callable[..., OracleAsyncConfig]":
    """Build fresh Oracle async configs for the pooling/connection-hook lifecycle contracts."""

    def make(
        *,
        pooled: bool = False,
        driver_features: "OracleDriverFeatures | None" = None,
        connection_instance: object | None = None,
    ) -> OracleAsyncConfig:
        connection_config = _oracle_pool_params(oracle_23ai_service)
        extra: dict[str, Any] = {}
        if driver_features is not None:
            extra["driver_features"] = driver_features
        if connection_instance is not None:
            extra["connection_instance"] = connection_instance
        return OracleAsyncConfig(connection_config=connection_config, **extra)

    return make


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

    def make(
        *,
        script_location: str,
        version_table_name: str,
        suffix: str,
        default_schema: str | None = None,
        version_table_schema: str | None = None,
    ) -> DuckDBConfig:
        migration_config: dict[str, Any] = {
            "script_location": script_location,
            "version_table_name": version_table_name,
        }
        if default_schema is not None:
            migration_config["default_schema"] = default_schema
        if version_table_schema is not None:
            migration_config["version_table_schema"] = version_table_schema
        return DuckDBConfig(
            connection_config={"database": str(tmp_path / f"mig_{suffix}.duckdb")}, migration_config=migration_config
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
def migration_config_asyncpg(postgres_service: PostgresService) -> Callable[..., Any]:
    """Build asyncpg configs for migration contract tests."""

    def make(
        *,
        script_location: str,
        version_table_name: str,
        suffix: str,
        default_schema: str | None = None,
        version_table_schema: str | None = None,
    ) -> AsyncpgConfig:
        migration_config: dict[str, Any] = {
            "script_location": script_location,
            "version_table_name": version_table_name,
        }
        if default_schema is not None:
            migration_config["default_schema"] = default_schema
        if version_table_schema is not None:
            migration_config["version_table_schema"] = version_table_schema
        return AsyncpgConfig(
            connection_config=_postgres_connection_config(postgres_service), migration_config=migration_config
        )

    return make


@pytest.fixture
def migration_config_psycopg_sync(postgres_service: PostgresService) -> Callable[..., Any]:
    """Build psycopg sync configs for migration contract tests."""

    def make(
        *,
        script_location: str,
        version_table_name: str,
        suffix: str,
        default_schema: str | None = None,
        version_table_schema: str | None = None,
    ) -> PsycopgSyncConfig:
        migration_config: dict[str, Any] = {
            "script_location": script_location,
            "version_table_name": version_table_name,
        }
        if default_schema is not None:
            migration_config["default_schema"] = default_schema
        if version_table_schema is not None:
            migration_config["version_table_schema"] = version_table_schema
        return PsycopgSyncConfig(
            connection_config={"conninfo": _postgres_conninfo(postgres_service)}, migration_config=migration_config
        )

    return make


@pytest.fixture
def migration_config_psycopg_async(postgres_service: PostgresService) -> Callable[..., Any]:
    """Build psycopg async configs for migration contract tests."""

    def make(
        *,
        script_location: str,
        version_table_name: str,
        suffix: str,
        default_schema: str | None = None,
        version_table_schema: str | None = None,
    ) -> PsycopgAsyncConfig:
        migration_config: dict[str, Any] = {
            "script_location": script_location,
            "version_table_name": version_table_name,
        }
        if default_schema is not None:
            migration_config["default_schema"] = default_schema
        if version_table_schema is not None:
            migration_config["version_table_schema"] = version_table_schema
        return PsycopgAsyncConfig(
            connection_config={"conninfo": _postgres_conninfo(postgres_service)}, migration_config=migration_config
        )

    return make


@pytest.fixture
def migration_config_psqlpy(postgres_service: PostgresService) -> Callable[..., Any]:
    """Build psqlpy configs for migration contract tests."""

    def make(
        *,
        script_location: str,
        version_table_name: str,
        suffix: str,
        default_schema: str | None = None,
        version_table_schema: str | None = None,
    ) -> PsqlpyConfig:
        migration_config: dict[str, Any] = {
            "script_location": script_location,
            "version_table_name": version_table_name,
        }
        if default_schema is not None:
            migration_config["default_schema"] = default_schema
        if version_table_schema is not None:
            migration_config["version_table_schema"] = version_table_schema
        return PsqlpyConfig(connection_config={"dsn": _psqlpy_dsn(postgres_service)}, migration_config=migration_config)

    return make


@pytest.fixture
def migration_config_adbc_postgres(postgres_service: PostgresService) -> Callable[..., Any]:
    """Build ADBC PostgreSQL configs for migration contract tests."""

    def make(
        *,
        script_location: str,
        version_table_name: str,
        suffix: str,
        default_schema: str | None = None,
        version_table_schema: str | None = None,
    ) -> AdbcConfig:
        migration_config: dict[str, Any] = {
            "script_location": script_location,
            "version_table_name": version_table_name,
        }
        if default_schema is not None:
            migration_config["default_schema"] = default_schema
        if version_table_schema is not None:
            migration_config["version_table_schema"] = version_table_schema
        return AdbcConfig(
            connection_config={"uri": _postgres_conninfo(postgres_service), "driver_name": "adbc_driver_postgresql"},
            migration_config=migration_config,
        )

    return make


@pytest.fixture
def migration_config_adbc_sqlite(tmp_path: Path) -> Callable[..., Any]:
    """Build ADBC SQLite configs for migration contract tests."""

    def make(*, script_location: str, version_table_name: str, suffix: str) -> AdbcConfig:
        return AdbcConfig(
            connection_config={
                "uri": f"file:{tmp_path / f'mig_{suffix}.db'}",
                "driver_name": "adbc_driver_sqlite",
                "autocommit": True,
            },
            migration_config={"script_location": script_location, "version_table_name": version_table_name},
        )

    return make


@pytest.fixture
def migration_config_oracle_sync(oracle_23ai_service: OracleService) -> Callable[..., Any]:
    """Build Oracle sync configs for migration contract tests."""

    def make(*, script_location: str, version_table_name: str, suffix: str) -> OracleSyncConfig:
        return OracleSyncConfig(
            connection_config=_oracle_pool_params(oracle_23ai_service),
            migration_config={"script_location": script_location, "version_table_name": version_table_name},
        )

    return make


@pytest.fixture
def migration_config_oracle_async(oracle_23ai_service: OracleService) -> Callable[..., Any]:
    """Build Oracle async configs for migration contract tests."""

    def make(*, script_location: str, version_table_name: str, suffix: str) -> OracleAsyncConfig:
        return OracleAsyncConfig(
            connection_config=_oracle_pool_params(oracle_23ai_service),
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


@pytest.fixture
async def contract_arrow_odbc_store(mssql_service: MSSQLService) -> "AsyncGenerator[ArrowOdbcStore, None]":
    """Provide a ready arrow-odbc SQL Server Litestar store for contract tests."""
    config = ArrowOdbcConfig(
        connection_config={"connection_string": mssql_service.connection_string},
        extension_config=_STORE_EXTENSION_CONFIG,
        driver_features={"dbms_name": "Microsoft SQL Server"},
    )
    store = ArrowOdbcStore(config)
    await store.create_table()
    yield store
    with contextlib.suppress(Exception):
        await store.delete_all()
    config.close_pool()


@pytest.fixture
async def contract_adbc_store(postgres_service: PostgresService) -> "AsyncGenerator[ADBCStore, None]":
    """Provide a ready PostgreSQL-backed ADBC Litestar store for contract tests."""
    config = AdbcConfig(
        connection_config={"uri": _adbc_postgres_uri(postgres_service), "driver_name": "adbc_driver_postgresql"},
        extension_config=_STORE_EXTENSION_CONFIG,
    )
    store = ADBCStore(config)
    await store.create_table()
    yield store
    with contextlib.suppress(Exception):
        await store.delete_all()
    config.close_pool()


@pytest.fixture
async def contract_oracle_async_store(oracle_23ai_service: OracleService) -> "AsyncGenerator[OracleAsyncStore, None]":
    """Provide a ready Oracle async Litestar store for contract tests."""
    config = OracleAsyncConfig(
        connection_config=_oracle_pool_params(oracle_23ai_service), extension_config=_STORE_EXTENSION_CONFIG
    )
    store = OracleAsyncStore(config)
    await store.create_table()
    yield store
    with contextlib.suppress(Exception):
        await store.delete_all()
    if config.connection_instance:
        await config.close_pool()


@pytest.fixture
async def contract_oracle_sync_store(oracle_23ai_service: OracleService) -> "AsyncGenerator[OracleSyncStore, None]":
    """Provide a ready Oracle sync Litestar store for contract tests."""
    config = OracleSyncConfig(
        connection_config=_oracle_pool_params(oracle_23ai_service), extension_config=_STORE_EXTENSION_CONFIG
    )
    store = OracleSyncStore(config)
    await store.create_table()
    yield store
    with contextlib.suppress(Exception):
        await store.delete_all()
    if config.connection_instance:
        config.close_pool()


def _adk_extension_config(suffix: str) -> dict[str, Any]:
    return {
        "adk": {
            "session_table": f"adk_s_{suffix}",
            "events_table": f"adk_e_{suffix}",
            "app_state_table": f"adk_app_{suffix}",
            "user_state_table": f"adk_user_{suffix}",
            "metadata_table": f"adk_meta_{suffix}",
        }
    }


def _ensure_adbc_store_driver_available(config: AdbcConfig) -> None:
    try:
        with config.provide_session():
            pass
    except Exception as error:
        if any(marker in str(error) for marker in _ADBC_DRIVER_MISSING_MARKERS):
            pytest.skip(f"ADBC driver not available: {error}")
        raise


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


@pytest.fixture
def adk_store_psycopg_async(postgres_service: PostgresService) -> Callable[..., Any]:
    """Build a fresh psycopg async ADK store with isolated tables per call."""

    def make() -> "tuple[Any, Any]":
        suffix = uuid4().hex[:8]
        config = PsycopgAsyncConfig(
            connection_config={"conninfo": _postgres_conninfo(postgres_service), "autocommit": True},
            extension_config=_adk_extension_config(suffix),
        )
        return config, PsycopgAsyncADKStore(config)

    return make


@pytest.fixture
def adk_store_psycopg_sync(postgres_service: PostgresService) -> Callable[..., Any]:
    """Build a fresh psycopg sync ADK store with isolated tables per call."""

    def make() -> "tuple[Any, Any]":
        suffix = uuid4().hex[:8]
        config = PsycopgSyncConfig(
            connection_config={"conninfo": _postgres_conninfo(postgres_service), "autocommit": True},
            extension_config=_adk_extension_config(suffix),
        )
        return config, PsycopgSyncADKStore(config)

    return make


@pytest.fixture
def adk_store_oracle_async(oracle_23ai_service: OracleService) -> Callable[..., Any]:
    """Build a fresh Oracle async ADK store with isolated tables per call."""

    def make() -> "tuple[Any, Any]":
        suffix = uuid4().hex[:8]
        config = OracleAsyncConfig(
            connection_config=_oracle_pool_params(oracle_23ai_service), extension_config=_adk_extension_config(suffix)
        )
        return config, OracleAsyncADKStore(config)

    return make


@pytest.fixture
def adk_store_oracle_sync(oracle_23ai_service: OracleService) -> Callable[..., Any]:
    """Build a fresh Oracle sync ADK store with isolated tables per call."""

    def make() -> "tuple[Any, Any]":
        suffix = uuid4().hex[:8]
        config = OracleSyncConfig(
            connection_config=_oracle_pool_params(oracle_23ai_service), extension_config=_adk_extension_config(suffix)
        )
        return config, OracleSyncADKStore(config)

    return make


@pytest.fixture
def adk_store_pymysql(mysql_service: MySQLService) -> Callable[..., Any]:
    """Build a fresh PyMySQL ADK store with isolated tables per call."""

    def make() -> "tuple[Any, Any]":
        suffix = uuid4().hex[:8]
        config = PyMysqlConfig(
            connection_config=_mysql_connection_config(mysql_service), extension_config=_adk_extension_config(suffix)
        )
        return config, PyMysqlADKStore(config)

    return make


@pytest.fixture
def adk_store_cockroach_asyncpg(cockroachdb_service: CockroachDBService) -> Callable[..., Any]:
    """Build a fresh CockroachDB asyncpg ADK store with isolated tables per call."""

    def make() -> "tuple[Any, Any]":
        suffix = uuid4().hex[:8]
        connection_config = _cockroach_asyncpg_connection_config(cockroachdb_service)
        connection_config.update({"min_size": 1, "max_size": 5})
        config = CockroachAsyncpgConfig(
            connection_config=connection_config, extension_config=_adk_extension_config(suffix)
        )
        return config, CockroachAsyncpgADKStore(config)

    return make


@pytest.fixture
def adk_store_cockroach_psycopg_async(cockroachdb_service: CockroachDBService) -> Callable[..., Any]:
    """Build a fresh CockroachDB psycopg async ADK store with isolated tables per call."""

    def make() -> "tuple[Any, Any]":
        suffix = uuid4().hex[:8]
        config = CockroachPsycopgAsyncConfig(
            connection_config={"conninfo": _cockroach_conninfo(cockroachdb_service)},
            extension_config=_adk_extension_config(suffix),
        )
        return config, CockroachPsycopgAsyncADKStore(config)

    return make


@pytest.fixture
def adk_store_cockroach_psycopg_sync(cockroachdb_service: CockroachDBService) -> Callable[..., Any]:
    """Build a fresh CockroachDB psycopg sync ADK store with isolated tables per call."""

    def make() -> "tuple[Any, Any]":
        suffix = uuid4().hex[:8]
        config = CockroachPsycopgSyncConfig(
            connection_config={"conninfo": _cockroach_conninfo(cockroachdb_service)},
            extension_config=_adk_extension_config(suffix),
        )
        return config, CockroachPsycopgSyncADKStore(config)

    return make


@pytest.fixture
def adk_store_adbc_sqlite(tmp_path: Path) -> Callable[..., Any]:
    """Build a fresh ADBC SQLite ADK store with isolated tables per call."""

    def make() -> "tuple[Any, Any]":
        suffix = uuid4().hex[:8]
        config = AdbcConfig(
            connection_config={"driver_name": "sqlite", "uri": f"file:{tmp_path / f'adk_{suffix}.db'}"},
            extension_config=_adk_extension_config(suffix),
        )
        _ensure_adbc_store_driver_available(config)
        return config, AdbcADKStore(config)

    return make


@pytest.fixture
def adk_store_adbc_duckdb(tmp_path: Path) -> Callable[..., Any]:
    """Build a fresh ADBC DuckDB ADK store with isolated tables per call."""

    def make() -> "tuple[Any, Any]":
        suffix = uuid4().hex[:8]
        config = AdbcConfig(
            connection_config={"driver_name": "duckdb", "path": str(tmp_path / f"adk_{suffix}.duckdb")},
            extension_config=_adk_extension_config(suffix),
        )
        _ensure_adbc_store_driver_available(config)
        return config, AdbcADKStore(config)

    return make


@pytest.fixture
def adk_store_adbc_postgres(postgres_service: PostgresService) -> Callable[..., Any]:
    """Build a fresh ADBC PostgreSQL ADK store with isolated tables per call."""

    def make() -> "tuple[Any, Any]":
        suffix = uuid4().hex[:8]
        config = AdbcConfig(
            connection_config={"driver_name": "postgresql", "uri": _adbc_postgres_uri(postgres_service)},
            extension_config=_adk_extension_config(suffix),
        )
        _ensure_adbc_store_driver_available(config)
        return config, AdbcADKStore(config)

    return make


@pytest.fixture
def adk_store_arrow_odbc_mssql(mssql_service: MSSQLService) -> Callable[..., Any]:
    """Build a fresh arrow-odbc SQL Server ADK store with isolated tables per call."""

    def make() -> "tuple[Any, Any]":
        suffix = uuid4().hex[:8]
        config = ArrowOdbcConfig(
            connection_config={"connection_string": mssql_service.connection_string},
            extension_config=_adk_extension_config(suffix),
            driver_features={"dbms_name": "Microsoft SQL Server"},
        )
        return config, ArrowOdbcADKStore(config)

    return make


def _resolve_adk_store_case(request: pytest.FixtureRequest, case: AdkStoreCase) -> AdkStoreCaseContext:
    return AdkStoreCaseContext(case=case, make_store=request.getfixturevalue(case.factory_fixture))


@pytest.fixture(params=ADK_STORE_PARAMS)
def adk_store_case(request: pytest.FixtureRequest) -> AdkStoreCaseContext:
    """Resolve an ADK store contract case by factory fixture name."""
    return _resolve_adk_store_case(request, request.param)


@pytest.fixture
def adk_capability_store_case(request: pytest.FixtureRequest) -> AdkStoreCaseContext:
    """Resolve an ADK store case selected by a capability-filtered param list."""
    return _resolve_adk_store_case(request, request.param)


def _resolve_store_case(request: pytest.FixtureRequest, case: StoreCase) -> StoreCaseContext:
    return StoreCaseContext(case=case, store=request.getfixturevalue(case.fixture_name))


@pytest.fixture(params=STORE_PARAMS)
def store_case(request: pytest.FixtureRequest) -> StoreCaseContext:
    """Resolve a Litestar store contract case by fixture name."""
    return _resolve_store_case(request, request.param)


def _resolve_driver_case(request: pytest.FixtureRequest, case: DriverCase) -> DriverCaseContext:
    driver = request.getfixturevalue(case.fixture_name)
    if case.table_fixture is not None:
        case = replace(case, table=request.getfixturevalue(case.table_fixture))
    make_config = request.getfixturevalue(case.config_factory_fixture) if case.config_factory_fixture else None
    return DriverCaseContext(case=case, driver=driver, make_config=make_config)


@pytest.fixture
def sync_postgres_extension_case(request: pytest.FixtureRequest) -> Generator[PostgresExtensionCaseContext, None, None]:
    """Resolve a sync PostgreSQL extension contract case."""
    case = request.param
    assert isinstance(case, PostgresExtensionCase)
    config = request.getfixturevalue(case.config_fixture_name)
    with config.provide_session() as driver:
        yield PostgresExtensionCaseContext(case=case, config=config, driver=driver)


@pytest.fixture
async def async_postgres_extension_case(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[PostgresExtensionCaseContext, None]:
    """Resolve an async PostgreSQL extension contract case."""
    case = request.param
    assert isinstance(case, PostgresExtensionCase)
    config = request.getfixturevalue(case.config_fixture_name)
    try:
        async with config.provide_session() as driver:
            yield PostgresExtensionCaseContext(case=case, config=config, driver=driver)
    finally:
        await config.close_pool()
        config.connection_instance = None


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


@pytest.fixture
def sync_capability_driver_case(request: pytest.FixtureRequest) -> DriverCaseContext:
    """Resolve a sync driver case supplied by a capability-filtered parametrization."""
    case = request.param
    return _resolve_driver_case(request, case)


@pytest.fixture
def async_capability_driver_case(request: pytest.FixtureRequest) -> DriverCaseContext:
    """Resolve an async driver case supplied by a capability-filtered parametrization."""
    case = request.param
    return _resolve_driver_case(request, case)


@pytest.fixture
def sync_lifecycle_driver_case(request: pytest.FixtureRequest) -> DriverCaseContext:
    """Resolve a sync driver case that supports at least one lifecycle contract."""
    case = request.param
    return _resolve_driver_case(request, case)


@pytest.fixture
def async_lifecycle_driver_case(request: pytest.FixtureRequest) -> DriverCaseContext:
    """Resolve an async driver case that supports at least one lifecycle contract."""
    case = request.param
    return _resolve_driver_case(request, case)


@pytest.fixture(params=DRIVER_PARAMS)
def driver_case(request: pytest.FixtureRequest) -> DriverCaseContext:
    """Resolve any driver contract case by fixture name for metadata-only contracts."""
    case = request.param
    return _resolve_driver_case(request, case)
