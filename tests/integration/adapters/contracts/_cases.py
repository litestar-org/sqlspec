"""Case records for shared adapter contract tests."""

from dataclasses import dataclass
from typing import Literal

import pytest
from _pytest.mark.structures import Mark, MarkDecorator

from tests.integration.adapters.contracts._schema import DEFAULT_CONTRACT_TABLE, DUCKDB_CONTRACT_TABLE, ContractTable


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
    deviations: tuple[str, ...] = ()


@dataclass(frozen=True)
class DriverCaseContext:
    """Resolved driver instance paired with its case metadata."""

    case: DriverCase
    driver: object


SQLITE_XDIST_MARK = pytest.mark.xdist_group("sqlite")
DUCKDB_XDIST_MARK = pytest.mark.xdist_group("duckdb")

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
)

DEFERRED_DRIVER_CASES = (
    DriverCase(
        "adbc-postgres-sync",
        "",
        "adbc",
        "postgres",
        "sync",
        integration_status="deferred",
        reason="ADBC contract fixtures require backend-specific setup.",
    ),
    DriverCase(
        "adbc-sqlite-sync",
        "",
        "adbc",
        "sqlite",
        "sync",
        integration_status="deferred",
        reason="ADBC SQLite contract fixture not wired into the C5 harness yet.",
    ),
    DriverCase(
        "adbc-duckdb-sync",
        "",
        "adbc",
        "duckdb",
        "sync",
        integration_status="deferred",
        reason="ADBC DuckDB contract fixture not wired into the C5 harness yet.",
    ),
    DriverCase(
        "aiomysql-async",
        "",
        "aiomysql",
        "mysql",
        "async",
        integration_status="deferred",
        reason="MySQL service-backed contract fixture is not wired into the C5 harness yet.",
    ),
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
        "asyncmy-async",
        "",
        "asyncmy",
        "mysql",
        "async",
        integration_status="deferred",
        reason="MySQL service-backed contract fixture is not wired into the C5 harness yet.",
    ),
    DriverCase(
        "asyncpg-async",
        "",
        "asyncpg",
        "postgres",
        "async",
        integration_status="deferred",
        reason="PostgreSQL service-backed contract fixture is not wired into the C5 harness yet.",
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
        "cockroach-asyncpg-async",
        "",
        "cockroach_asyncpg",
        "postgres",
        "async",
        integration_status="deferred",
        reason="CockroachDB service-backed contract fixture is not wired into the C5 harness yet.",
    ),
    DriverCase(
        "cockroach-psycopg-sync",
        "",
        "cockroach_psycopg",
        "postgres",
        "sync",
        integration_status="deferred",
        reason="CockroachDB service-backed contract fixture is not wired into the C5 harness yet.",
    ),
    DriverCase(
        "cockroach-psycopg-async",
        "",
        "cockroach_psycopg",
        "postgres",
        "async",
        integration_status="deferred",
        reason="CockroachDB service-backed contract fixture is not wired into the C5 harness yet.",
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
        "mysqlconnector-sync",
        "",
        "mysqlconnector",
        "mysql",
        "sync",
        integration_status="deferred",
        reason="MySQL service-backed contract fixture is not wired into the C5 harness yet.",
    ),
    DriverCase(
        "mysqlconnector-async",
        "",
        "mysqlconnector",
        "mysql",
        "async",
        integration_status="deferred",
        reason="MySQL service-backed contract fixture is not wired into the C5 harness yet.",
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
        "psqlpy-async",
        "",
        "psqlpy",
        "postgres",
        "async",
        integration_status="deferred",
        reason="PostgreSQL service-backed contract fixture is not wired into the C5 harness yet.",
    ),
    DriverCase(
        "psycopg-sync",
        "",
        "psycopg",
        "postgres",
        "sync",
        integration_status="deferred",
        reason="PostgreSQL service-backed contract fixture is not wired into the C5 harness yet.",
    ),
    DriverCase(
        "psycopg-async",
        "",
        "psycopg",
        "postgres",
        "async",
        integration_status="deferred",
        reason="PostgreSQL service-backed contract fixture is not wired into the C5 harness yet.",
    ),
    DriverCase(
        "pymysql-sync",
        "",
        "pymysql",
        "mysql",
        "sync",
        integration_status="deferred",
        reason="MySQL service-backed contract fixture is not wired into the C5 harness yet.",
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
