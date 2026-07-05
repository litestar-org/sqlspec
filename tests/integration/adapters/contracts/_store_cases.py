"""Case records for shared Litestar session-store contract tests."""

from dataclasses import dataclass

import pytest
from _pytest.mark.structures import Mark, MarkDecorator

from tests.integration.adapters.contracts._cases import (
    ADBC_MARK,
    ARROW_ODBC_MARK,
    DUCKDB_XDIST_MARK,
    MSSQL_MARK,
    MSSQL_XDIST_MARK,
    MYSQL_XDIST_MARK,
    ORACLE_XDIST_MARK,
    POSTGRES_XDIST_MARK,
    SQLITE_XDIST_MARK,
)


@dataclass(frozen=True)
class StoreCase:
    """Store fixture and capability metadata for Litestar store contract tests."""

    id: str
    fixture_name: str
    adapter: str
    marks: tuple[Mark | MarkDecorator, ...] = ()


@dataclass(frozen=True)
class StoreCaseContext:
    """Resolved store instance paired with its case metadata."""

    case: StoreCase
    store: object


STORE_CASES = (
    StoreCase("sqlite", "contract_sqlite_store", "sqlite", marks=(SQLITE_XDIST_MARK, pytest.mark.anyio)),
    StoreCase("aiosqlite", "contract_aiosqlite_store", "aiosqlite", marks=(SQLITE_XDIST_MARK, pytest.mark.anyio)),
    StoreCase("duckdb", "contract_duckdb_store", "duckdb", marks=(DUCKDB_XDIST_MARK, pytest.mark.anyio)),
    StoreCase(
        "arrow-odbc",
        "contract_arrow_odbc_store",
        "arrow_odbc",
        marks=(MSSQL_MARK, MSSQL_XDIST_MARK, ARROW_ODBC_MARK, pytest.mark.anyio),
    ),
    StoreCase("asyncpg", "contract_asyncpg_store", "asyncpg", marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio)),
    StoreCase("psqlpy", "contract_psqlpy_store", "psqlpy", marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio)),
    StoreCase(
        "psycopg-async", "contract_psycopg_async_store", "psycopg", marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio)
    ),
    StoreCase("psycopg-sync", "contract_psycopg_sync_store", "psycopg", marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio)),
    StoreCase("aiomysql", "contract_aiomysql_store", "aiomysql", marks=(MYSQL_XDIST_MARK, pytest.mark.anyio)),
    StoreCase("asyncmy", "contract_asyncmy_store", "asyncmy", marks=(MYSQL_XDIST_MARK, pytest.mark.anyio)),
    StoreCase(
        "mysqlconnector-async",
        "contract_mysqlconnector_async_store",
        "mysqlconnector",
        marks=(MYSQL_XDIST_MARK, pytest.mark.anyio),
    ),
    StoreCase(
        "mysqlconnector-sync",
        "contract_mysqlconnector_sync_store",
        "mysqlconnector",
        marks=(MYSQL_XDIST_MARK, pytest.mark.anyio),
    ),
    StoreCase("pymysql", "contract_pymysql_store", "pymysql", marks=(MYSQL_XDIST_MARK, pytest.mark.anyio)),
    StoreCase("adbc", "contract_adbc_store", "adbc", marks=(ADBC_MARK, POSTGRES_XDIST_MARK, pytest.mark.anyio)),
    StoreCase(
        "oracledb-async", "contract_oracle_async_store", "oracledb", marks=(ORACLE_XDIST_MARK, pytest.mark.anyio)
    ),
    StoreCase("oracledb-sync", "contract_oracle_sync_store", "oracledb", marks=(ORACLE_XDIST_MARK, pytest.mark.anyio)),
)

STORE_PARAMS = tuple(pytest.param(case, id=case.id, marks=case.marks) for case in STORE_CASES)
