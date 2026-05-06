# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
"""Cross-adapter EXPLAIN plan contract tests."""

import inspect
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import pytest

from sqlspec import SQLResult
from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.oracledb import OracleSyncConfig
from sqlspec.adapters.psqlpy import PsqlpyConfig
from sqlspec.adapters.psycopg import PsycopgSyncConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.builder import Explain, sql
from sqlspec.core import SQL, ExplainOptions
from tests.integration.adapters.contracts._mysql_async import (
    MYSQL_ASYNC_ADAPTERS,
    close_mysql_async_config,
    mysql_async_config,
)


@dataclass(frozen=True)
class ExplainAdapter:
    """Per-adapter EXPLAIN contract setup."""

    adapter: str
    dialect: str
    table_name: str
    create_table: str
    insert_statement: str
    update_statement: str
    delete_statement: str
    text_type: str
    expect_data: bool = True
    is_async: bool = False
    query_builder_analyze: bool = False
    factory_analyze: bool = False
    sql_object_analyze: bool = False
    write_analyze: bool = False
    raw_builder: bool = False
    raw_sql_object: bool = False
    drop_if_exists: bool = True
    rollback_before_cleanup: bool = False


EXPLAIN_ADAPTERS = {
    "sqlite": ExplainAdapter(
        adapter="sqlite",
        dialect="sqlite",
        table_name="explain_test",
        create_table="""
            CREATE TABLE IF NOT EXISTS explain_test (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0
            )
            """,
        insert_statement="INSERT INTO explain_test (name, value) VALUES ('test', 1)",
        update_statement="UPDATE explain_test SET value = 100 WHERE id = 1",
        delete_statement="DELETE FROM explain_test WHERE id = 1",
        text_type="TEXT",
    ),
    "aiosqlite": ExplainAdapter(
        adapter="aiosqlite",
        dialect="sqlite",
        table_name="explain_test",
        create_table="""
            CREATE TABLE IF NOT EXISTS explain_test (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0
            )
            """,
        insert_statement="INSERT INTO explain_test (name, value) VALUES ('test', 1)",
        update_statement="UPDATE explain_test SET value = 100 WHERE id = 1",
        delete_statement="DELETE FROM explain_test WHERE id = 1",
        text_type="TEXT",
        is_async=True,
    ),
    "aiomysql": ExplainAdapter(
        adapter="aiomysql",
        dialect="mysql",
        table_name="explain_test",
        create_table="""
            CREATE TABLE IF NOT EXISTS explain_test (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                value INT DEFAULT 0
            )
            """,
        insert_statement="INSERT INTO explain_test (name, value) VALUES ('test', 1)",
        update_statement="UPDATE explain_test SET value = 100 WHERE id = 1",
        delete_statement="DELETE FROM explain_test WHERE id = 1",
        text_type="VARCHAR(255)",
        is_async=True,
        raw_builder=True,
        raw_sql_object=True,
    ),
    "asyncmy": ExplainAdapter(
        adapter="asyncmy",
        dialect="mysql",
        table_name="explain_test",
        create_table="""
            CREATE TABLE IF NOT EXISTS explain_test (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                value INT DEFAULT 0
            )
            """,
        insert_statement="INSERT INTO explain_test (name, value) VALUES ('test', 1)",
        update_statement="UPDATE explain_test SET value = 100 WHERE id = 1",
        delete_statement="DELETE FROM explain_test WHERE id = 1",
        text_type="VARCHAR(255)",
        is_async=True,
        raw_builder=True,
        raw_sql_object=True,
    ),
    "adbc-postgres": ExplainAdapter(
        adapter="adbc-postgres",
        dialect="postgres",
        table_name="explain_test_adbc",
        create_table="""
            CREATE TABLE IF NOT EXISTS explain_test_adbc (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0
            )
            """,
        insert_statement="INSERT INTO explain_test_adbc (name, value) VALUES ('test', 1)",
        update_statement="UPDATE explain_test_adbc SET value = 100 WHERE id = 1",
        delete_statement="DELETE FROM explain_test_adbc WHERE id = 1",
        text_type="TEXT",
        query_builder_analyze=True,
        factory_analyze=True,
        sql_object_analyze=True,
        write_analyze=True,
    ),
    "asyncpg": ExplainAdapter(
        adapter="asyncpg",
        dialect="postgres",
        table_name="explain_test",
        create_table="""
            CREATE TABLE IF NOT EXISTS explain_test (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0
            )
            """,
        insert_statement="INSERT INTO explain_test (name, value) VALUES ('test', 1)",
        update_statement="UPDATE explain_test SET value = 100 WHERE id = 1",
        delete_statement="DELETE FROM explain_test WHERE id = 1",
        text_type="TEXT",
        is_async=True,
        query_builder_analyze=True,
        factory_analyze=True,
        sql_object_analyze=True,
    ),
    "duckdb": ExplainAdapter(
        adapter="duckdb",
        dialect="duckdb",
        table_name="explain_test",
        create_table="""
            CREATE TABLE IF NOT EXISTS explain_test (
                id INTEGER PRIMARY KEY,
                name VARCHAR NOT NULL,
                value INTEGER DEFAULT 0
            )
            """,
        insert_statement="INSERT INTO explain_test (id, name, value) VALUES (1, 'test', 1)",
        update_statement="UPDATE explain_test SET value = 100 WHERE id = 1",
        delete_statement="DELETE FROM explain_test WHERE id = 1",
        text_type="VARCHAR",
    ),
    "psqlpy": ExplainAdapter(
        adapter="psqlpy",
        dialect="postgres",
        table_name="explain_test_psqlpy",
        create_table="""
            CREATE TABLE IF NOT EXISTS explain_test_psqlpy (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0
            )
            """,
        insert_statement="INSERT INTO explain_test_psqlpy (name, value) VALUES ('test', 1)",
        update_statement="UPDATE explain_test_psqlpy SET value = 100 WHERE id = 1",
        delete_statement="DELETE FROM explain_test_psqlpy WHERE id = 1",
        text_type="TEXT",
        is_async=True,
        query_builder_analyze=True,
        factory_analyze=True,
        sql_object_analyze=True,
    ),
    "psycopg-sync": ExplainAdapter(
        adapter="psycopg-sync",
        dialect="postgres",
        table_name="explain_test_psycopg_sync",
        create_table="""
            CREATE TABLE IF NOT EXISTS explain_test_psycopg_sync (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0
            )
            """,
        insert_statement="INSERT INTO explain_test_psycopg_sync (name, value) VALUES ('test', 1)",
        update_statement="UPDATE explain_test_psycopg_sync SET value = 100 WHERE id = 1",
        delete_statement="DELETE FROM explain_test_psycopg_sync WHERE id = 1",
        text_type="TEXT",
        query_builder_analyze=True,
        factory_analyze=True,
        sql_object_analyze=True,
        write_analyze=True,
        rollback_before_cleanup=True,
    ),
    "oracle-sync": ExplainAdapter(
        adapter="oracle-sync",
        dialect="oracle",
        table_name="explain_test",
        create_table="""
            CREATE TABLE explain_test (
                id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                name VARCHAR2(255) NOT NULL,
                value NUMBER DEFAULT 0
            )
            """,
        insert_statement="INSERT INTO explain_test (name, value) VALUES ('test', 1)",
        update_statement="UPDATE explain_test SET value = 100 WHERE id = 1",
        delete_statement="DELETE FROM explain_test WHERE id = 1",
        text_type="VARCHAR2(255)",
        expect_data=False,
        raw_builder=True,
        raw_sql_object=True,
        drop_if_exists=False,
    ),
    "bigquery": ExplainAdapter(
        adapter="bigquery",
        dialect="bigquery",
        table_name="explain_test",
        create_table="",
        insert_statement="",
        update_statement="",
        delete_statement="",
        text_type="STRING",
    ),
    "spanner": ExplainAdapter(
        adapter="spanner",
        dialect="spanner",
        table_name="explain_test",
        create_table="",
        insert_statement="",
        update_statement="",
        delete_statement="",
        text_type="STRING(255)",
    ),
}

BASE_SQLITE_SCENARIOS = (
    "basic_select",
    "where",
    "join",
    "query_builder",
    "sql_factory",
    "sql_object",
    "insert",
    "update",
    "delete",
    "subquery",
)
MYSQL_SCENARIOS = (
    "basic_select",
    "analyze",
    "format_json",
    "format_tree",
    "format_traditional",
    "query_builder",
    "sql_factory",
    "sql_object",
    "insert",
    "update",
    "delete",
)
POSTGRES_CORE_SCENARIOS = (
    "basic_select",
    "analyze",
    "format_json",
    "verbose",
    "full_options",
    "query_builder",
    "sql_factory",
    "sql_object",
)

SCENARIOS_BY_ADAPTER = {
    "sqlite": BASE_SQLITE_SCENARIOS,
    "aiosqlite": ("basic_select", "where", "query_builder", "sql_factory", "sql_object", "insert", "update", "delete"),
    "aiomysql": MYSQL_SCENARIOS,
    "asyncmy": MYSQL_SCENARIOS,
    "adbc-postgres": (*POSTGRES_CORE_SCENARIOS, "insert", "update", "delete"),
    "asyncpg": (
        "basic_select",
        "analyze",
        "format_json",
        "buffers",
        "verbose",
        "full_options",
        "query_builder",
        "sql_factory",
        "sql_object",
    ),
    "duckdb": (
        "basic_select",
        "analyze",
        "format_json",
        "where",
        "join",
        "query_builder",
        "sql_factory",
        "sql_object",
        "insert",
        "update",
        "delete",
        "aggregate",
    ),
    "psqlpy": POSTGRES_CORE_SCENARIOS,
    "psycopg-sync": (
        "basic_select",
        "analyze",
        "format_json",
        "buffers",
        "timing",
        "verbose",
        "full_options",
        "query_builder",
        "sql_factory",
        "sql_object",
        "insert",
        "update",
        "delete",
        "costs_disabled",
        "summary",
    ),
    "oracle-sync": (
        "basic_select",
        "where",
        "query_builder",
        "sql_factory",
        "sql_object",
        "insert",
        "update",
        "delete",
        "display_plan",
    ),
    "bigquery": ("basic_select", "where", "query_builder", "sql_factory", "sql_object", "aggregate"),
    "spanner": ("basic_select", "where", "query_builder", "sql_factory", "sql_object"),
}

ADAPTER_MARKS = {
    "sqlite": [pytest.mark.xdist_group("sqlite")],
    "aiosqlite": [pytest.mark.xdist_group("sqlite")],
    "aiomysql": [pytest.mark.xdist_group("mysql"), pytest.mark.aiomysql],
    "asyncmy": [pytest.mark.xdist_group("mysql"), pytest.mark.asyncmy],
    "adbc-postgres": [
        pytest.mark.xdist_group("postgres"),
        pytest.mark.skip(reason="ADBC COPY incompatible with EXPLAIN"),
    ],
    "asyncpg": [pytest.mark.xdist_group("postgres"), pytest.mark.asyncpg],
    "duckdb": [pytest.mark.xdist_group("duckdb")],
    "psqlpy": [pytest.mark.xdist_group("postgres")],
    "psycopg-sync": [pytest.mark.xdist_group("postgres")],
    "oracle-sync": [pytest.mark.xdist_group("oracle"), pytest.mark.oracle],
    "bigquery": [pytest.mark.xdist_group("bigquery"), pytest.mark.skip(reason="BigQuery emulator EXPLAIN unsupported")],
    "spanner": [pytest.mark.xdist_group("spanner"), pytest.mark.skip(reason="Spanner uses query_mode=PLAN")],
}

EXPLAIN_CASES = [
    pytest.param(adapter, scenario, marks=ADAPTER_MARKS[adapter], id=f"{adapter}-{scenario}")
    for adapter, scenarios in SCENARIOS_BY_ADAPTER.items()
    for scenario in scenarios
]


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


def _make_config(adapter: str, request: pytest.FixtureRequest) -> Any:
    if adapter == "sqlite":
        return SqliteConfig(connection_config={"database": ":memory:"})
    if adapter == "aiosqlite":
        return AiosqliteConfig()
    if adapter in {"aiomysql", "asyncmy"}:
        return mysql_async_config(adapter, request.getfixturevalue("mysql_service"))
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
    if adapter == "duckdb":
        return DuckDBConfig(connection_config={"database": ":memory:"})
    if adapter == "psqlpy":
        return PsqlpyConfig(connection_config={"dsn": _postgres_url(request, scheme="postgres"), "max_db_pool_size": 5})
    if adapter == "psycopg-sync":
        return PsycopgSyncConfig(connection_config={"conninfo": _postgres_url(request)})
    service = request.getfixturevalue("oracle_service")
    return OracleSyncConfig(
        connection_config={
            "user": service.user,
            "password": service.password,
            "dsn": f"{service.host}:{service.port}/{service.service_name}",
        }
    )


@asynccontextmanager
async def _provide_driver(spec: ExplainAdapter, config: Any) -> AsyncGenerator[Any, None]:
    try:
        if spec.is_async:
            async with config.provide_session() as driver:
                yield driver
        else:
            with config.provide_session() as driver:
                yield driver
    finally:
        if spec.adapter in MYSQL_ASYNC_ADAPTERS:
            await close_mysql_async_config(config)
        else:
            await _close_config(config)


async def _execute(driver: Any, statement: Any) -> SQLResult:
    result = driver.execute(statement)
    return await _maybe_await(result)


async def _execute_script(driver: Any, statement: str) -> None:
    result = driver.execute_script(statement)
    await _maybe_await(result)


async def _commit(driver: Any) -> None:
    commit = getattr(driver, "commit", None)
    if commit is not None:
        await _maybe_await(commit())


async def _rollback(driver: Any) -> None:
    rollback = getattr(driver, "rollback", None)
    if rollback is not None:
        await _maybe_await(rollback())


async def _drop_table(driver: Any, spec: ExplainAdapter, table_name: str) -> None:
    statement = f"DROP TABLE IF EXISTS {table_name}" if spec.drop_if_exists else f"DROP TABLE {table_name}"
    try:
        await _execute_script(driver, statement)
        await _commit(driver)
    except Exception:
        if spec.drop_if_exists:
            raise


async def _setup_explain_table(driver: Any, spec: ExplainAdapter) -> None:
    await _drop_table(driver, spec, spec.table_name)
    await _execute_script(driver, spec.create_table)
    await _commit(driver)


async def _cleanup_explain_table(driver: Any, spec: ExplainAdapter) -> None:
    if spec.rollback_before_cleanup:
        await _rollback(driver)
    await _drop_table(driver, spec, spec.table_name)


async def _setup_join_table(driver: Any, spec: ExplainAdapter) -> str:
    table_name = f"{spec.table_name}_join"
    await _drop_table(driver, spec, table_name)
    await _execute_script(
        driver,
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY,
            test_id INTEGER,
            data {spec.text_type}
        )
        """,
    )
    await _commit(driver)
    return table_name


def _select_sql(spec: ExplainAdapter) -> str:
    return f"SELECT * FROM {spec.table_name}"


def _build_statement(spec: ExplainAdapter, scenario: str) -> Any:
    select_sql = _select_sql(spec)
    if scenario == "basic_select":
        return Explain(select_sql, dialect=spec.dialect).build()
    if scenario == "where":
        return Explain(f"{select_sql} WHERE id = 1", dialect=spec.dialect).build()
    if scenario == "join":
        join_table = f"{spec.table_name}_join"
        return Explain(
            f"SELECT * FROM {spec.table_name} e JOIN {join_table} e2 ON e.id = e2.test_id", dialect=spec.dialect
        ).build()
    if scenario == "query_builder":
        if spec.raw_builder:
            return Explain(f"{select_sql} WHERE id > 0", dialect=spec.dialect).analyze().build()
        return (
            sql
            .select("*")
            .from_(spec.table_name)
            .where("id > :id", id=0)
            .explain(analyze=spec.query_builder_analyze)
            .build()
        )
    if scenario == "sql_factory":
        return sql.explain(select_sql, analyze=spec.factory_analyze, dialect=spec.dialect).build()
    if scenario == "sql_object":
        stmt = SQL(select_sql)
        if spec.raw_sql_object:
            return Explain(stmt.sql, dialect=spec.dialect).build()
        return stmt.explain(analyze=spec.sql_object_analyze)
    if scenario == "insert":
        explain = Explain(spec.insert_statement, dialect=spec.dialect)
        return explain.analyze().build() if spec.write_analyze else explain.build()
    if scenario == "update":
        explain = Explain(spec.update_statement, dialect=spec.dialect)
        return explain.analyze().build() if spec.write_analyze else explain.build()
    if scenario == "delete":
        explain = Explain(spec.delete_statement, dialect=spec.dialect)
        return explain.analyze().build() if spec.write_analyze else explain.build()
    if scenario == "subquery":
        return Explain(
            f"SELECT * FROM {spec.table_name} WHERE id IN (SELECT id FROM {spec.table_name} WHERE value > 0)",
            dialect=spec.dialect,
        ).build()
    if scenario == "aggregate":
        return Explain(
            f"SELECT COUNT(*), SUM(value) FROM {spec.table_name} GROUP BY name", dialect=spec.dialect
        ).build()
    if scenario == "analyze":
        return Explain(select_sql, dialect=spec.dialect).analyze().build()
    if scenario == "format_json":
        return Explain(select_sql, dialect=spec.dialect).format("json").build()
    if scenario == "format_tree":
        return Explain(select_sql, dialect=spec.dialect).format("tree").build()
    if scenario == "format_traditional":
        return Explain(select_sql, dialect=spec.dialect).format("traditional").build()
    if scenario == "buffers":
        return Explain(select_sql, dialect=spec.dialect).analyze().buffers().build()
    if scenario == "timing":
        return Explain(select_sql, dialect=spec.dialect).analyze().timing().build()
    if scenario == "verbose":
        return Explain(select_sql, dialect=spec.dialect).verbose().build()
    if scenario == "full_options":
        return Explain(select_sql, dialect=spec.dialect).analyze().verbose().buffers().timing().format("json").build()
    if scenario == "costs_disabled":
        return Explain(select_sql, dialect=spec.dialect, options=ExplainOptions(costs=False)).build()
    if scenario == "summary":
        return Explain(select_sql, dialect=spec.dialect).analyze().summary().build()
    msg = f"Unknown EXPLAIN scenario: {scenario}"
    raise AssertionError(msg)


def _assert_explain_result(result: SQLResult, spec: ExplainAdapter) -> None:
    assert isinstance(result, SQLResult)
    if spec.expect_data:
        assert result.data is not None


@pytest.mark.parametrize(("adapter", "scenario"), EXPLAIN_CASES)
async def test_explain_contract(adapter: str, scenario: str, request: pytest.FixtureRequest) -> None:
    """Adapters execute their supported EXPLAIN statement shapes."""
    spec = EXPLAIN_ADAPTERS[adapter]
    config = _make_config(adapter, request)

    async with _provide_driver(spec, config) as driver:
        await _setup_explain_table(driver, spec)
        try:
            if scenario == "join":
                await _setup_join_table(driver, spec)
            if scenario == "display_plan":
                await _execute(driver, Explain(_select_sql(spec) + " WHERE id = 1", dialect=spec.dialect).build())
                result = await _execute(driver, "SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY())")
            else:
                result = await _execute(driver, _build_statement(spec, scenario))

            _assert_explain_result(result, spec)
        finally:
            if scenario == "join":
                await _drop_table(driver, spec, f"{spec.table_name}_join")
            await _cleanup_explain_table(driver, spec)
