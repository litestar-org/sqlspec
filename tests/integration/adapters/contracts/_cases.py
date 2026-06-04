"""Case records for shared adapter contract tests."""

from dataclasses import dataclass
from typing import Literal

import pytest
from _pytest.mark.structures import Mark, MarkDecorator

from tests.integration.adapters.contracts._schema import (
    DEFAULT_CONTRACT_TABLE,
    DUCKDB_CONTRACT_TABLE,
    MYSQL_CONTRACT_TABLE,
    POSTGRES_CONTRACT_TABLE,
    ContractTable,
)


@dataclass(frozen=True)
class DriverCase:
    """Driver fixture and capability metadata for contract tests."""

    id: str
    fixture_name: str
    adapter: str
    dialect: str
    mode: Literal["sync", "async"]
    marks: tuple[Mark | MarkDecorator, ...] = ()
    integration_status: Literal["active", "local-only", "unit-only", "deferred"] = "active"
    reason: str | None = None
    table: ContractTable = DEFAULT_CONTRACT_TABLE
    supports_arrow: bool = False
    supports_explain: bool = False
    supports_execute_many: bool = True
    supports_execute_script: bool = True
    supports_filtered_statement: bool = True
    supports_loader_input: bool = True
    supports_migrations: bool = False
    supports_schema_qualified_ddl: bool = False
    supports_storage_bridge: bool = False
    supports_transactions: bool = True
    supports_for_update: bool = False
    supports_returning: bool = False
    supports_json: bool = False
    supports_arrays: bool = False
    supports_vector: bool = False
    supports_exception_translation: bool = True
    deviations: tuple[str, ...] = ()


@dataclass(frozen=True)
class DriverCaseContext:
    """Resolved driver instance paired with its case metadata."""

    case: DriverCase
    driver: object


SQLITE_XDIST_MARK = pytest.mark.xdist_group("sqlite")
DUCKDB_XDIST_MARK = pytest.mark.xdist_group("duckdb")
MYSQL_XDIST_MARK = pytest.mark.xdist_group("mysql")
POSTGRES_XDIST_MARK = pytest.mark.xdist_group("postgres")
COCKROACH_XDIST_MARK = pytest.mark.xdist_group("cockroachdb")
ADBC_MARK = pytest.mark.adbc

SYNC_DRIVER_CASES = (
    DriverCase(
        id="sqlite-sync",
        fixture_name="contract_sqlite_driver",
        adapter="sqlite",
        dialect="sqlite",
        mode="sync",
        marks=(SQLITE_XDIST_MARK,),
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_storage_bridge=True,
    ),
    DriverCase(
        id="duckdb-sync",
        fixture_name="contract_duckdb_driver",
        adapter="duckdb",
        dialect="duckdb",
        mode="sync",
        marks=(DUCKDB_XDIST_MARK,),
        table=DUCKDB_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_storage_bridge=True,
    ),
    DriverCase(
        id="mysqlconnector-sync",
        fixture_name="contract_mysqlconnector_sync_driver",
        adapter="mysqlconnector",
        dialect="mysql",
        mode="sync",
        marks=(MYSQL_XDIST_MARK,),
        table=MYSQL_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=False,
        supports_execute_many=True,
        supports_migrations=True,
        supports_storage_bridge=True,
        deviations=("no-returning", "autocommit-ddl", "explain-unread-result"),
    ),
    DriverCase(
        id="pymysql-sync",
        fixture_name="contract_pymysql_driver",
        adapter="pymysql",
        dialect="mysql",
        mode="sync",
        marks=(MYSQL_XDIST_MARK,),
        table=MYSQL_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_storage_bridge=True,
        deviations=("no-returning", "autocommit-ddl"),
    ),
    DriverCase(
        id="psycopg-sync",
        fixture_name="contract_psycopg_sync_driver",
        adapter="psycopg",
        dialect="postgres",
        mode="sync",
        marks=(POSTGRES_XDIST_MARK,),
        table=POSTGRES_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_schema_qualified_ddl=True,
        supports_storage_bridge=True,
        supports_for_update=True,
        supports_returning=True,
        supports_json=True,
        supports_arrays=True,
    ),
    DriverCase(
        id="cockroach-psycopg-sync",
        fixture_name="contract_cockroach_psycopg_sync_driver",
        adapter="cockroach_psycopg",
        dialect="postgres",
        mode="sync",
        marks=(COCKROACH_XDIST_MARK,),
        table=POSTGRES_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_schema_qualified_ddl=True,
        supports_storage_bridge=True,
        supports_for_update=True,
        supports_returning=True,
        supports_json=True,
        supports_arrays=True,
        deviations=("cockroach-serializable-transactions",),
    ),
    DriverCase(
        id="adbc-sqlite-sync",
        fixture_name="contract_adbc_sqlite_driver",
        adapter="adbc",
        dialect="sqlite",
        mode="sync",
        marks=(ADBC_MARK,),
        supports_arrow=True,
        supports_execute_many=False,
        supports_exception_translation=False,
        deviations=("execute-rows-affected-unavailable",),
    ),
    DriverCase(
        id="adbc-duckdb-sync",
        fixture_name="contract_adbc_duckdb_driver",
        adapter="adbc",
        dialect="duckdb",
        mode="sync",
        marks=(ADBC_MARK,),
        table=DUCKDB_CONTRACT_TABLE,
        supports_arrow=True,
        supports_execute_many=False,
        deviations=("execute-rows-affected-unavailable",),
    ),
    DriverCase(
        id="adbc-postgres-sync",
        fixture_name="contract_adbc_postgres_driver",
        adapter="adbc",
        dialect="postgres",
        mode="sync",
        marks=(ADBC_MARK, POSTGRES_XDIST_MARK),
        table=POSTGRES_CONTRACT_TABLE,
        supports_arrow=True,
        supports_execute_many=True,
        deviations=("execute-rows-affected-unavailable",),
    ),
)

ASYNC_DRIVER_CASES = (
    DriverCase(
        id="aiosqlite-async",
        fixture_name="contract_aiosqlite_driver",
        adapter="aiosqlite",
        dialect="sqlite",
        mode="async",
        marks=(SQLITE_XDIST_MARK, pytest.mark.anyio),
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_storage_bridge=True,
    ),
    DriverCase(
        id="aiomysql-async",
        fixture_name="contract_aiomysql_driver",
        adapter="aiomysql",
        dialect="mysql",
        mode="async",
        marks=(MYSQL_XDIST_MARK, pytest.mark.anyio),
        table=MYSQL_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_storage_bridge=True,
        deviations=("no-returning", "autocommit-ddl"),
    ),
    DriverCase(
        id="asyncmy-async",
        fixture_name="contract_asyncmy_driver",
        adapter="asyncmy",
        dialect="mysql",
        mode="async",
        marks=(MYSQL_XDIST_MARK, pytest.mark.anyio),
        table=MYSQL_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_storage_bridge=True,
        deviations=("no-returning", "autocommit-ddl"),
    ),
    DriverCase(
        id="mysqlconnector-async",
        fixture_name="contract_mysqlconnector_async_driver",
        adapter="mysqlconnector",
        dialect="mysql",
        mode="async",
        marks=(MYSQL_XDIST_MARK, pytest.mark.anyio),
        table=MYSQL_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=False,
        supports_execute_many=True,
        supports_migrations=True,
        supports_storage_bridge=True,
        deviations=("no-returning", "autocommit-ddl", "explain-unread-result"),
    ),
    DriverCase(
        id="asyncpg-async",
        fixture_name="contract_asyncpg_driver",
        adapter="asyncpg",
        dialect="postgres",
        mode="async",
        marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio),
        table=POSTGRES_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_schema_qualified_ddl=True,
        supports_storage_bridge=True,
        supports_for_update=True,
        supports_returning=True,
        supports_json=True,
        supports_arrays=True,
    ),
    DriverCase(
        id="psqlpy-async",
        fixture_name="contract_psqlpy_driver",
        adapter="psqlpy",
        dialect="postgres",
        mode="async",
        marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio),
        table=POSTGRES_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_schema_qualified_ddl=True,
        supports_storage_bridge=True,
        supports_for_update=True,
        supports_returning=True,
        supports_json=True,
        supports_arrays=True,
        deviations=("execute-rows-affected-unavailable",),
    ),
    DriverCase(
        id="psycopg-async",
        fixture_name="contract_psycopg_async_driver",
        adapter="psycopg",
        dialect="postgres",
        mode="async",
        marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio),
        table=POSTGRES_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_schema_qualified_ddl=True,
        supports_storage_bridge=True,
        supports_for_update=True,
        supports_returning=True,
        supports_json=True,
        supports_arrays=True,
    ),
    DriverCase(
        id="cockroach-asyncpg-async",
        fixture_name="contract_cockroach_asyncpg_driver",
        adapter="cockroach_asyncpg",
        dialect="postgres",
        mode="async",
        marks=(COCKROACH_XDIST_MARK, pytest.mark.anyio),
        table=POSTGRES_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_schema_qualified_ddl=True,
        supports_storage_bridge=True,
        supports_for_update=True,
        supports_returning=True,
        supports_json=True,
        supports_arrays=True,
        deviations=("cockroach-serializable-transactions",),
    ),
    DriverCase(
        id="cockroach-psycopg-async",
        fixture_name="contract_cockroach_psycopg_async_driver",
        adapter="cockroach_psycopg",
        dialect="postgres",
        mode="async",
        marks=(COCKROACH_XDIST_MARK, pytest.mark.anyio),
        table=POSTGRES_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_schema_qualified_ddl=True,
        supports_storage_bridge=True,
        supports_for_update=True,
        supports_returning=True,
        supports_json=True,
        supports_arrays=True,
        deviations=("cockroach-serializable-transactions",),
    ),
)

DEFERRED_DRIVER_CASES = (
    DriverCase(
        "arrow-odbc-sync",
        "",
        "arrow_odbc",
        "odbc",
        "sync",
        integration_status="deferred",
        reason="No active integration fixture exists for arrow_odbc.",
    ),
    DriverCase(
        "bigquery-sync",
        "",
        "bigquery",
        "bigquery",
        "sync",
        integration_status="deferred",
        reason="BigQuery remains optional and needs existing opt-in gate wiring.",
    ),
    DriverCase(
        "mssql-python-sync",
        "",
        "mssql_python",
        "tsql",
        "sync",
        integration_status="deferred",
        reason="No active integration fixture exists for mssql_python.",
    ),
    DriverCase(
        "oracledb-sync",
        "",
        "oracledb",
        "oracle",
        "sync",
        integration_status="deferred",
        reason="Oracle service-backed contract fixture is not wired into the C5 harness yet.",
    ),
    DriverCase(
        "oracledb-async",
        "",
        "oracledb",
        "oracle",
        "async",
        integration_status="deferred",
        reason="Oracle service-backed contract fixture is not wired into the C5 harness yet.",
    ),
    DriverCase(
        "spanner-sync",
        "",
        "spanner",
        "spanner",
        "sync",
        integration_status="deferred",
        reason="Spanner remains optional and needs existing opt-in gate wiring.",
    ),
)

ACTIVE_DRIVER_CASES = SYNC_DRIVER_CASES + ASYNC_DRIVER_CASES
DRIVER_CASES = ACTIVE_DRIVER_CASES + DEFERRED_DRIVER_CASES
DRIVER_CASE_BY_ID = {case.id: case for case in DRIVER_CASES}
SYNC_DRIVER_PARAMS = tuple(pytest.param(case, id=case.id, marks=case.marks) for case in SYNC_DRIVER_CASES)
ASYNC_DRIVER_PARAMS = tuple(pytest.param(case, id=case.id, marks=case.marks) for case in ASYNC_DRIVER_CASES)
DRIVER_PARAMS = SYNC_DRIVER_PARAMS


def get_driver_case(case_id: str) -> DriverCase:
    """Return a registered driver case by id."""
    return DRIVER_CASE_BY_ID[case_id]
