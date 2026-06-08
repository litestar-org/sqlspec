"""Case records for shared ADK session/event store contract tests."""

from dataclasses import dataclass

import pytest
from _pytest.mark.structures import Mark, MarkDecorator

from tests.integration.adapters.contracts._cases import (
    DUCKDB_XDIST_MARK,
    MYSQL_XDIST_MARK,
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
)
# NOTE: psycopg-async/sync are excluded pending sqlspec-cne7 — the psycopg ADK store read
# methods index tuple-cursor rows by string key (TypeError). asyncpg/psqlpy cover postgres here.

ADK_STORE_PARAMS = tuple(pytest.param(case, id=case.id, marks=case.marks) for case in ADK_STORE_CASES)
