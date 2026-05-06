# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
"""Cross-adapter exception mapping contract tests."""

import contextlib
import inspect
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import pytest
from google.api_core import exceptions as api_exceptions
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.aiomysql import AiomysqlConfig
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.asyncmy import AsyncmyConfig
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.mysqlconnector import MysqlConnectorAsyncConfig
from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.psqlpy import PsqlpyConfig
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.pymysql import PyMysqlConfig
from sqlspec.adapters.spanner import SpannerSyncConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import (
    CheckViolationError,
    ForeignKeyViolationError,
    NotFoundError,
    NotNullViolationError,
    SQLConversionError,
    SQLParsingError,
    UniqueViolationError,
)
from sqlspec.typing import PYARROW_INSTALLED
from tests.integration.adapters.adbc.conftest import xfail_if_driver_missing

SQLITE_ADAPTERS = [
    pytest.param("sqlite", marks=pytest.mark.xdist_group("sqlite"), id="sqlite"),
    pytest.param("aiosqlite", marks=pytest.mark.xdist_group("sqlite"), id="aiosqlite"),
]

MYSQL_ADAPTERS = [
    pytest.param("aiomysql", marks=[pytest.mark.xdist_group("mysql"), pytest.mark.aiomysql], id="aiomysql"),
    pytest.param("asyncmy", marks=[pytest.mark.xdist_group("mysql"), pytest.mark.asyncmy], id="asyncmy"),
    pytest.param(
        "mysqlconnector", marks=[pytest.mark.xdist_group("mysql"), pytest.mark.mysql_connector], id="mysqlconnector"
    ),
    pytest.param("pymysql", marks=[pytest.mark.xdist_group("mysql"), pytest.mark.pymysql], id="pymysql"),
]

POSTGRES_ADAPTERS = [
    pytest.param("adbc-postgres", marks=[pytest.mark.xdist_group("postgres"), pytest.mark.postgres], id="adbc"),
    pytest.param("asyncpg", marks=[pytest.mark.xdist_group("postgres"), pytest.mark.asyncpg], id="asyncpg"),
    pytest.param("psqlpy", marks=[pytest.mark.xdist_group("postgres"), pytest.mark.postgres], id="psqlpy"),
    pytest.param("psycopg-sync", marks=[pytest.mark.xdist_group("postgres"), pytest.mark.postgres], id="psycopg-sync"),
    pytest.param(
        "psycopg-async", marks=[pytest.mark.xdist_group("postgres"), pytest.mark.postgres], id="psycopg-async"
    ),
]

DUCKDB_ADAPTERS = [pytest.param("duckdb", marks=pytest.mark.xdist_group("duckdb"), id="duckdb")]

ORACLE_ADAPTERS = [
    pytest.param("oracle-sync", marks=[pytest.mark.xdist_group("oracle"), pytest.mark.oracle], id="oracle-sync"),
    pytest.param("oracle-async", marks=[pytest.mark.xdist_group("oracle"), pytest.mark.oracle], id="oracle-async"),
]

CONSTRAINT_ADAPTERS = [*SQLITE_ADAPTERS, *MYSQL_ADAPTERS, *POSTGRES_ADAPTERS, *DUCKDB_ADAPTERS]
UNIQUE_ADAPTERS = [*CONSTRAINT_ADAPTERS, *ORACLE_ADAPTERS]
NOT_NULL_ADAPTERS = [*CONSTRAINT_ADAPTERS, pytest.param("oracle-sync", marks=pytest.mark.oracle, id="oracle-sync")]
SQL_PARSING_ADAPTERS = [*UNIQUE_ADAPTERS]


@pytest.fixture(scope="session")
def spanner_contract_database(spanner_service: Any, spanner_connection: Any) -> Any:
    """Ensure the Spanner emulator instance and database exist for contract tests."""
    instance = spanner_connection.instance(spanner_service.instance_name)
    if not instance.exists():
        config_name = f"{spanner_connection.project_name}/instanceConfigs/emulator-config"
        instance = spanner_connection.instance(spanner_service.instance_name, configuration_name=config_name)
        instance.create().result(300)

    database = instance.database(spanner_service.database_name)
    if not database.exists():
        database.create().result(300)

    return database


@pytest.fixture(scope="session")
def spanner_contract_config(spanner_service: Any, spanner_contract_database: Any) -> SpannerSyncConfig:
    """Create a Spanner config for exception contract tests."""
    return SpannerSyncConfig(
        connection_config={
            "project": spanner_service.project,
            "instance_id": spanner_service.instance_name,
            "database_id": spanner_service.database_name,
            "credentials": spanner_service.credentials,
            "client_options": {"api_endpoint": f"{spanner_service.host}:{spanner_service.port}"},
            "min_sessions": 1,
            "max_sessions": 5,
        }
    )


def _spanner_run_ddl(database: Any, statements: list[str]) -> None:
    operation = database.update_ddl(statements)
    operation.result(300)


def _spanner_drop_table(database: Any, table_name: str) -> None:
    try:
        _spanner_run_ddl(database, [f"DROP TABLE {table_name}"])
    except api_exceptions.GoogleAPICallError:
        pass


@pytest.fixture
def spanner_contract_users_table(spanner_contract_database: Any) -> Generator[str, None, None]:
    """Create a Spanner users table for exception contract tests."""
    table_name = f"test_users_{uuid4().hex[:8]}"
    _spanner_drop_table(spanner_contract_database, table_name)
    _spanner_run_ddl(
        spanner_contract_database,
        [
            f"""
            CREATE TABLE {table_name} (
                id STRING(36) NOT NULL,
                name STRING(100),
                email STRING(255),
                age INT64
            ) PRIMARY KEY (id)
            """
        ],
    )

    try:
        yield table_name
    finally:
        _spanner_drop_table(spanner_contract_database, table_name)


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _close_config(config: Any) -> None:
    close_pool = getattr(config, "close_pool", None)
    if close_pool is not None:
        await _maybe_await(close_pool())
    elif (connection := getattr(config, "connection_instance", None)) is not None:
        close = getattr(connection, "close", None)
        if close is not None:
            await _maybe_await(close())

    if hasattr(config, "connection_instance"):
        config.connection_instance = None


def _postgres_url(request: pytest.FixtureRequest, *, scheme: str = "postgresql") -> str:
    service = request.getfixturevalue("postgres_service")
    return f"{scheme}://{service.user}:{service.password}@{service.host}:{service.port}/{service.database}"


def _mysql_config(adapter: str, mysql_service: MySQLService) -> Any:
    connection_config: dict[str, Any] = {
        "host": mysql_service.host,
        "port": mysql_service.port,
        "user": mysql_service.user,
        "password": mysql_service.password,
        "autocommit": True,
    }
    if adapter == "aiomysql":
        connection_config["db"] = mysql_service.db
        return AiomysqlConfig(connection_config=connection_config)
    connection_config["database"] = mysql_service.db
    if adapter == "asyncmy":
        return AsyncmyConfig(connection_config=connection_config)
    if adapter == "mysqlconnector":
        connection_config["use_pure"] = True
        return MysqlConnectorAsyncConfig(connection_config=connection_config)
    return PyMysqlConfig(connection_config=connection_config)


def _oracle_config(adapter: str, request: pytest.FixtureRequest) -> Any:
    service = request.getfixturevalue("oracle_service")
    connection_config = {
        "user": service.user,
        "password": service.password,
        "dsn": f"{service.host}:{service.port}/{service.service_name}",
    }
    return (
        OracleAsyncConfig(connection_config=connection_config)
        if adapter == "oracle-async"
        else OracleSyncConfig(connection_config=connection_config)
    )


def _make_config(adapter: str, request: pytest.FixtureRequest) -> Any:
    if adapter == "sqlite":
        return SqliteConfig(connection_config={"database": ":memory:"})
    if adapter == "aiosqlite":
        return AiosqliteConfig()
    if adapter in {"aiomysql", "asyncmy", "mysqlconnector", "pymysql"}:
        return _mysql_config(adapter, request.getfixturevalue("mysql_service"))
    if adapter == "adbc-postgres":
        return AdbcConfig(connection_config={"uri": _postgres_url(request), "driver_name": "adbc_driver_postgresql"})
    if adapter == "asyncpg":
        service = request.getfixturevalue("postgres_service")
        return AsyncpgConfig(
            connection_config={
                "host": service.host,
                "port": service.port,
                "user": service.user,
                "password": service.password,
                "database": service.database,
            }
        )
    if adapter == "psqlpy":
        return PsqlpyConfig(connection_config={"dsn": _postgres_url(request, scheme="postgres"), "max_db_pool_size": 5})
    if adapter == "psycopg-sync":
        return PsycopgSyncConfig(connection_config={"conninfo": _postgres_url(request)})
    if adapter == "psycopg-async":
        return PsycopgAsyncConfig(connection_config={"conninfo": _postgres_url(request)}, pool_config={"min_size": 1})
    if adapter == "duckdb":
        return DuckDBConfig(connection_config={"database": ":memory:"})
    return _oracle_config(adapter, request)


def _is_async_adapter(adapter: str) -> bool:
    return adapter in {
        "aiosqlite",
        "aiomysql",
        "asyncmy",
        "mysqlconnector",
        "asyncpg",
        "psqlpy",
        "psycopg-async",
        "oracle-async",
    }


@asynccontextmanager
async def _driver(adapter: str, request: pytest.FixtureRequest) -> AsyncGenerator[Any, None]:
    config = _make_config(adapter, request)
    try:
        if _is_async_adapter(adapter):
            async with config.provide_session() as driver:
                if adapter in {"sqlite", "aiosqlite"}:
                    await _execute(driver, "PRAGMA foreign_keys = ON")
                yield driver
        else:
            with config.provide_session() as driver:
                if adapter in {"sqlite", "aiosqlite"}:
                    await _execute(driver, "PRAGMA foreign_keys = ON")
                yield driver
    finally:
        await _close_config(config)


async def _execute(driver: Any, statement: str, parameters: Any | None = None) -> Any:
    result = driver.execute(statement) if parameters is None else driver.execute(statement, parameters)
    return await _maybe_await(result)


async def _execute_script(driver: Any, script: str) -> Any:
    result = driver.execute_script(script)
    return await _maybe_await(result)


async def _rollback(driver: Any) -> None:
    rollback = getattr(driver, "rollback", None)
    if rollback is not None:
        with contextlib.suppress(Exception):
            await _maybe_await(rollback())


async def _drop(driver: Any, table_name: str, *, oracle: bool = False, cascade: bool = False) -> None:
    if oracle:
        with contextlib.suppress(Exception):
            await _execute(driver, f"DROP TABLE {table_name} PURGE")
        return
    suffix = " CASCADE" if cascade else ""
    await _execute(driver, f"DROP TABLE IF EXISTS {table_name}{suffix}")


def _name(adapter: str, suffix: str) -> str:
    return f"ex_{suffix}_{adapter.replace('-', '_')}"


def _id_column(adapter: str) -> str:
    if adapter.startswith("oracle"):
        return "id NUMBER PRIMARY KEY"
    return "id INT PRIMARY KEY"


def _text_type(adapter: str) -> str:
    if adapter.startswith("oracle"):
        return "VARCHAR2(255)"
    if adapter in {"duckdb", "pymysql", "aiomysql", "asyncmy", "mysqlconnector"}:
        return "VARCHAR(255)"
    return "TEXT"


async def _create_unique_table(driver: Any, adapter: str, table_name: str) -> None:
    await _drop(driver, table_name, oracle=adapter.startswith("oracle"))
    await _execute(
        driver, f"CREATE TABLE {table_name} ({_id_column(adapter)}, email {_text_type(adapter)} UNIQUE NOT NULL)"
    )


async def _create_not_null_table(driver: Any, adapter: str, table_name: str) -> None:
    await _drop(driver, table_name, oracle=adapter.startswith("oracle"))
    await _execute(
        driver, f"CREATE TABLE {table_name} ({_id_column(adapter)}, required_field {_text_type(adapter)} NOT NULL)"
    )


async def _create_check_table(driver: Any, adapter: str, table_name: str) -> None:
    await _drop(driver, table_name)
    await _execute(driver, f"CREATE TABLE {table_name} ({_id_column(adapter)}, age INT CHECK (age >= 18))")


async def _create_fk_tables(driver: Any, adapter: str, parent: str, child: str) -> None:
    cascade = adapter in {"adbc-postgres", "asyncpg", "psqlpy", "psycopg-sync", "psycopg-async"}
    await _drop(driver, child, cascade=cascade)
    await _drop(driver, parent, cascade=cascade)
    suffix = " ENGINE=InnoDB" if adapter in {"aiomysql", "asyncmy", "mysqlconnector", "pymysql"} else ""
    await _execute(driver, f"CREATE TABLE {parent} ({_id_column(adapter)}, name {_text_type(adapter)}){suffix}")
    await _execute(
        driver,
        f"CREATE TABLE {child} ({_id_column(adapter)}, parent_id INT NOT NULL, "
        f"FOREIGN KEY (parent_id) REFERENCES {parent}(id)){suffix}",
    )


@pytest.mark.parametrize("adapter", UNIQUE_ADAPTERS)
async def test_adapter_unique_violation(adapter: str, request: pytest.FixtureRequest) -> None:
    """Adapters map unique constraint failures to UniqueViolationError."""
    table_name = _name(adapter, "unique")
    async with _driver(adapter, request) as driver:
        await _create_unique_table(driver, adapter, table_name)
        await _execute(driver, f"INSERT INTO {table_name} (id, email) VALUES (1, 'test@example.com')")

        with pytest.raises(UniqueViolationError):
            await _execute(driver, f"INSERT INTO {table_name} (id, email) VALUES (2, 'test@example.com')")

        await _rollback(driver)
        await _drop(driver, table_name, oracle=adapter.startswith("oracle"))


@pytest.mark.parametrize("adapter", CONSTRAINT_ADAPTERS)
async def test_adapter_foreign_key_violation(adapter: str, request: pytest.FixtureRequest) -> None:
    """Adapters map foreign key failures to ForeignKeyViolationError."""
    parent = _name(adapter, "fk_parent")
    child = _name(adapter, "fk_child")
    cascade = adapter in {"adbc-postgres", "asyncpg", "psqlpy", "psycopg-sync", "psycopg-async"}
    async with _driver(adapter, request) as driver:
        await _create_fk_tables(driver, adapter, parent, child)

        with pytest.raises(ForeignKeyViolationError):
            await _execute(driver, f"INSERT INTO {child} (id, parent_id) VALUES (1, 999)")

        await _rollback(driver)
        await _drop(driver, child, cascade=cascade)
        await _drop(driver, parent, cascade=cascade)


@pytest.mark.parametrize("adapter", NOT_NULL_ADAPTERS)
async def test_adapter_not_null_violation(adapter: str, request: pytest.FixtureRequest) -> None:
    """Adapters map NOT NULL failures to NotNullViolationError."""
    table_name = _name(adapter, "notnull")
    async with _driver(adapter, request) as driver:
        await _create_not_null_table(driver, adapter, table_name)

        with pytest.raises(NotNullViolationError):
            await _execute(driver, f"INSERT INTO {table_name} (id) VALUES (1)")

        await _rollback(driver)
        await _drop(driver, table_name, oracle=adapter.startswith("oracle"))


@pytest.mark.parametrize("adapter", CONSTRAINT_ADAPTERS)
async def test_adapter_check_violation(adapter: str, request: pytest.FixtureRequest) -> None:
    """Adapters map CHECK failures to CheckViolationError."""
    table_name = _name(adapter, "check")
    async with _driver(adapter, request) as driver:
        await _create_check_table(driver, adapter, table_name)

        with pytest.raises(CheckViolationError):
            await _execute(driver, f"INSERT INTO {table_name} (id, age) VALUES (1, 15)")

        await _rollback(driver)
        await _drop(driver, table_name)


@pytest.mark.parametrize("adapter", SQL_PARSING_ADAPTERS)
async def test_adapter_sql_parsing_error(adapter: str, request: pytest.FixtureRequest) -> None:
    """Adapters map syntax failures to SQLParsingError."""
    async with _driver(adapter, request) as driver:
        with pytest.raises(SQLParsingError):
            await _execute(driver, "SELCT * FROM nonexistent_table")
        await _rollback(driver)


def test_duckdb_catalog_exception_not_found() -> None:
    """DuckDB maps missing table catalog failures to NotFoundError."""
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as driver:
            with pytest.raises(NotFoundError):
                driver.execute("SELECT * FROM nonexistent_table_xyz")
    finally:
        config.close_pool()


@pytest.mark.postgres
@pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow missing")
@xfail_if_driver_missing
def test_adbc_select_to_arrow_raises_mapped_exception(request: pytest.FixtureRequest) -> None:
    """ADBC select_to_arrow re-raises mapped database errors."""
    config = _make_config("adbc-postgres", request)
    try:
        with config.provide_session() as driver:
            with pytest.raises(SQLParsingError):
                driver.select_to_arrow("SELECT * FROM missing_arrow_table")
    finally:
        config.close_pool()


@pytest.mark.spanner
def test_spanner_dml_in_read_only_session(
    spanner_contract_config: SpannerSyncConfig, spanner_contract_users_table: str
) -> None:
    """Spanner maps DML attempted in a read-only session."""
    with spanner_contract_config.provide_session() as session:
        with pytest.raises(SQLConversionError, match="Cannot execute DML"):
            session.execute(
                f"INSERT INTO {spanner_contract_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
                {"id": str(uuid4()), "name": "Test", "email": "test@example.com", "age": 30},
            )


@pytest.mark.spanner
def test_spanner_sql_parsing_error_invalid_syntax(spanner_contract_config: SpannerSyncConfig) -> None:
    """Spanner maps invalid SQL syntax through SQLSpec exception types."""
    with spanner_contract_config.provide_session() as session:
        with pytest.raises((SQLParsingError, SQLConversionError)):
            session.execute("selectall * FORM users")


@pytest.mark.spanner
def test_spanner_sql_parsing_error_invalid_table(spanner_contract_config: SpannerSyncConfig) -> None:
    """Spanner maps missing tables through SQLSpec exception types."""
    with spanner_contract_config.provide_session() as session:
        with pytest.raises((NotFoundError, SQLParsingError)):
            session.select(f"SELECT * FROM nonexistent_table_{uuid4().hex[:8]}")


@pytest.mark.spanner
def test_spanner_unique_violation_duplicate_key(
    spanner_contract_config: SpannerSyncConfig, spanner_contract_users_table: str
) -> None:
    """Spanner maps duplicate primary keys to UniqueViolationError."""
    user_id = str(uuid4())

    with spanner_contract_config.provide_write_session() as session:
        session.execute(
            f"INSERT INTO {spanner_contract_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
            {"id": user_id, "name": "First", "email": "first@example.com", "age": 25},
        )

    with spanner_contract_config.provide_write_session() as session:
        with pytest.raises(UniqueViolationError):
            session.execute(
                f"INSERT INTO {spanner_contract_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
                {"id": user_id, "name": "Duplicate", "email": "dup@example.com", "age": 30},
            )

    with spanner_contract_config.provide_write_session() as session:
        session.execute(f"DELETE FROM {spanner_contract_users_table} WHERE id = @id", {"id": user_id})


@pytest.mark.spanner
def test_spanner_invalid_parameter_type(spanner_contract_config: SpannerSyncConfig) -> None:
    """Spanner maps invalid parameter typing to SQLParsingError."""
    with spanner_contract_config.provide_session() as session:
        with pytest.raises(SQLParsingError):
            session.select_value("SELECT @num + 1", {"num": "not_a_number"})


@pytest.mark.spanner
def test_spanner_execute_many_in_read_only_session(
    spanner_contract_config: SpannerSyncConfig, spanner_contract_users_table: str
) -> None:
    """Spanner maps execute_many in read-only sessions to SQLConversionError."""
    parameters = [
        {"id": str(uuid4()), "name": "User 1", "email": "u1@example.com", "age": 20},
        {"id": str(uuid4()), "name": "User 2", "email": "u2@example.com", "age": 25},
    ]

    with spanner_contract_config.provide_session() as session:
        with pytest.raises(SQLConversionError, match="execute_many requires"):
            session.execute_many(
                f"INSERT INTO {spanner_contract_users_table} (id, name, email, age) VALUES (@id, @name, @email, @age)",
                parameters,
            )


@pytest.mark.spanner
def test_spanner_select_one_no_results(
    spanner_contract_config: SpannerSyncConfig, spanner_contract_users_table: str
) -> None:
    """Spanner maps select_one with no rows to NotFoundError."""
    with spanner_contract_config.provide_session() as session:
        with pytest.raises(NotFoundError):
            session.select_one(
                f"SELECT * FROM {spanner_contract_users_table} WHERE id = @id", {"id": "definitely-does-not-exist"}
            )


@pytest.mark.spanner
def test_spanner_invalid_column_name(
    spanner_contract_config: SpannerSyncConfig, spanner_contract_users_table: str
) -> None:
    """Spanner maps invalid columns through SQLSpec exception types."""
    with spanner_contract_config.provide_session() as session:
        with pytest.raises((SQLParsingError, NotFoundError)):
            session.select(f"SELECT nonexistent_column FROM {spanner_contract_users_table}")


@pytest.mark.spanner
def test_spanner_update_nonexistent_row_no_error(
    spanner_contract_config: SpannerSyncConfig, spanner_contract_users_table: str
) -> None:
    """Spanner returns zero affected rows for updates that match no rows."""
    with spanner_contract_config.provide_write_session() as session:
        result = session.execute(
            f"UPDATE {spanner_contract_users_table} SET name = @name WHERE id = @id",
            {"id": "nonexistent-id", "name": "Should Not Exist"},
        )
        assert result.rows_affected == 0


@pytest.mark.spanner
def test_spanner_delete_nonexistent_row_no_error(
    spanner_contract_config: SpannerSyncConfig, spanner_contract_users_table: str
) -> None:
    """Spanner returns zero affected rows for deletes that match no rows."""
    with spanner_contract_config.provide_write_session() as session:
        result = session.execute(f"DELETE FROM {spanner_contract_users_table} WHERE id = @id", {"id": "nonexistent-id"})
        assert result.rows_affected == 0


@pytest.mark.xdist_group("bigquery")
@pytest.mark.skip(reason="BigQuery emulator config missing")
def test_bigquery_not_found_error() -> None:
    """BigQuery missing-table mapping is preserved as a skipped emulator-gated contract."""


@pytest.mark.xdist_group("bigquery")
@pytest.mark.skip(reason="BigQuery emulator config missing")
def test_bigquery_sql_parsing_error() -> None:
    """BigQuery syntax-error mapping is preserved as a skipped emulator-gated contract."""


@pytest.mark.xdist_group("bigquery")
@pytest.mark.skip(reason="BigQuery emulator config missing")
def test_bigquery_unique_violation_table_exists() -> None:
    """BigQuery duplicate-table mapping is preserved as a skipped emulator-gated contract."""
