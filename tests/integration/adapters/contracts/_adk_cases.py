"""Case records for shared ADK session/event store contract tests."""

from dataclasses import dataclass

import pytest
from _pytest.mark.structures import Mark, MarkDecorator, ParameterSet

from tests.integration.adapters.contracts._cases import (
    ADBC_MARK,
    ARROW_ODBC_MARK,
    COCKROACH_XDIST_MARK,
    DUCKDB_XDIST_MARK,
    MSSQL_MARK,
    MSSQL_XDIST_MARK,
    MYSQL_XDIST_MARK,
    ORACLE_XDIST_MARK,
    POSTGRES_XDIST_MARK,
    SQLITE_XDIST_MARK,
)


@dataclass(frozen=True)
class AdkStoreCase:
    """Store-factory and capability metadata for ADK store contract tests."""

    id: str
    factory_fixture: str
    adapter: str
    marks: tuple[Mark | MarkDecorator, ...] = ()
    supports_atomic_state_update: bool = True


@dataclass(frozen=True)
class AdkStoreCaseContext:
    """Resolved store factory paired with its case metadata."""

    case: AdkStoreCase
    make_store: object


ADK_STORE_CASES = (
    AdkStoreCase("sqlite", "adk_store_sqlite", "sqlite", marks=(SQLITE_XDIST_MARK, pytest.mark.anyio)),
    AdkStoreCase("aiosqlite", "adk_store_aiosqlite", "aiosqlite", marks=(SQLITE_XDIST_MARK, pytest.mark.anyio)),
    AdkStoreCase(
        "duckdb",
        "adk_store_duckdb",
        "duckdb",
        marks=(DUCKDB_XDIST_MARK, pytest.mark.anyio),
        supports_atomic_state_update=False,
    ),
    AdkStoreCase("aiomysql", "adk_store_aiomysql", "aiomysql", marks=(MYSQL_XDIST_MARK, pytest.mark.anyio)),
    AdkStoreCase("asyncmy", "adk_store_asyncmy", "asyncmy", marks=(MYSQL_XDIST_MARK, pytest.mark.anyio)),
    AdkStoreCase(
        "mysqlconnector-async",
        "adk_store_mysqlconnector_async",
        "mysqlconnector",
        marks=(MYSQL_XDIST_MARK, pytest.mark.anyio),
    ),
    AdkStoreCase(
        "mysqlconnector-sync",
        "adk_store_mysqlconnector_sync",
        "mysqlconnector",
        marks=(MYSQL_XDIST_MARK, pytest.mark.anyio),
    ),
    AdkStoreCase("asyncpg", "adk_store_asyncpg", "asyncpg", marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio)),
    AdkStoreCase("psqlpy", "adk_store_psqlpy", "psqlpy", marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio)),
    AdkStoreCase("psycopg-async", "adk_store_psycopg_async", "psycopg", marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio)),
    AdkStoreCase("psycopg-sync", "adk_store_psycopg_sync", "psycopg", marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio)),
    AdkStoreCase("pymysql", "adk_store_pymysql", "pymysql", marks=(MYSQL_XDIST_MARK, pytest.mark.anyio)),
    AdkStoreCase(
        "cockroach-asyncpg",
        "adk_store_cockroach_asyncpg",
        "cockroach_asyncpg",
        marks=(COCKROACH_XDIST_MARK, pytest.mark.anyio),
    ),
    AdkStoreCase(
        "cockroach-psycopg-async",
        "adk_store_cockroach_psycopg_async",
        "cockroach_psycopg",
        marks=(COCKROACH_XDIST_MARK, pytest.mark.anyio),
    ),
    AdkStoreCase(
        "cockroach-psycopg-sync",
        "adk_store_cockroach_psycopg_sync",
        "cockroach_psycopg",
        marks=(COCKROACH_XDIST_MARK, pytest.mark.anyio),
    ),
    AdkStoreCase("oracledb-async", "adk_store_oracle_async", "oracledb", marks=(ORACLE_XDIST_MARK, pytest.mark.anyio)),
    AdkStoreCase("oracledb-sync", "adk_store_oracle_sync", "oracledb", marks=(ORACLE_XDIST_MARK, pytest.mark.anyio)),
    AdkStoreCase("adbc-sqlite", "adk_store_adbc_sqlite", "adbc", marks=(ADBC_MARK, pytest.mark.anyio)),
    AdkStoreCase(
        "adbc-duckdb",
        "adk_store_adbc_duckdb",
        "adbc",
        marks=(ADBC_MARK, pytest.mark.anyio),
        supports_atomic_state_update=False,
    ),
    AdkStoreCase(
        "adbc-postgres", "adk_store_adbc_postgres", "adbc", marks=(ADBC_MARK, POSTGRES_XDIST_MARK, pytest.mark.anyio)
    ),
    AdkStoreCase(
        "arrow-odbc",
        "adk_store_arrow_odbc_mssql",
        "arrow_odbc",
        marks=(MSSQL_MARK, MSSQL_XDIST_MARK, ARROW_ODBC_MARK, pytest.mark.anyio),
    ),
)

ADK_STORE_PARAMS = tuple(pytest.param(case, id=case.id, marks=case.marks) for case in ADK_STORE_CASES)


def adk_store_params_with(*capability_names: str) -> "tuple[ParameterSet, ...]":
    """Return ADK store params that opt into at least one named capability."""
    return tuple(
        pytest.param(case, id=case.id, marks=case.marks)
        for case in ADK_STORE_CASES
        if any(getattr(case, name) for name in capability_names)
    )
