"""Case records for shared migration-lifecycle contract tests."""

from dataclasses import dataclass
from typing import Literal

import pytest
from _pytest.mark.structures import Mark, MarkDecorator

from tests.integration.adapters.contracts._cases import (
    DUCKDB_XDIST_MARK,
    MYSQL_XDIST_MARK,
    POSTGRES_XDIST_MARK,
    SQLITE_XDIST_MARK,
)


@dataclass(frozen=True)
class MigrationCase:
    """Config-factory and capability metadata for migration contract tests."""

    id: str
    factory_fixture: str
    adapter: str
    mode: Literal["sync", "async"]
    marks: tuple[Mark | MarkDecorator, ...] = ()


@dataclass(frozen=True)
class MigrationCaseContext:
    """Resolved config factory paired with its case metadata."""

    case: MigrationCase
    make_config: object


SYNC_MIGRATION_CASES = (
    MigrationCase("sqlite-sync", "migration_config_sqlite", "sqlite", "sync", marks=(SQLITE_XDIST_MARK,)),
    MigrationCase("duckdb-sync", "migration_config_duckdb", "duckdb", "sync", marks=(DUCKDB_XDIST_MARK,)),
    MigrationCase("pymysql-sync", "migration_config_pymysql", "pymysql", "sync", marks=(MYSQL_XDIST_MARK,)),
    MigrationCase("psycopg-sync", "migration_config_psycopg_sync", "psycopg", "sync", marks=(POSTGRES_XDIST_MARK,)),
)

ASYNC_MIGRATION_CASES = (
    MigrationCase(
        "aiosqlite-async",
        "migration_config_aiosqlite",
        "aiosqlite",
        "async",
        marks=(SQLITE_XDIST_MARK, pytest.mark.anyio),
    ),
    MigrationCase(
        "asyncmy-async", "migration_config_asyncmy", "asyncmy", "async", marks=(MYSQL_XDIST_MARK, pytest.mark.anyio)
    ),
    MigrationCase(
        "aiomysql-async", "migration_config_aiomysql", "aiomysql", "async", marks=(MYSQL_XDIST_MARK, pytest.mark.anyio)
    ),
    MigrationCase(
        "mysqlconnector-async",
        "migration_config_mysqlconnector_async",
        "mysqlconnector",
        "async",
        marks=(MYSQL_XDIST_MARK, pytest.mark.anyio),
    ),
    MigrationCase(
        "psqlpy-async", "migration_config_psqlpy", "psqlpy", "async", marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio)
    ),
    MigrationCase(
        "psycopg-async",
        "migration_config_psycopg_async",
        "psycopg",
        "async",
        marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio),
    ),
)

ACTIVE_MIGRATION_CASES = SYNC_MIGRATION_CASES + ASYNC_MIGRATION_CASES
SYNC_MIGRATION_PARAMS = tuple(pytest.param(case, id=case.id, marks=case.marks) for case in SYNC_MIGRATION_CASES)
ASYNC_MIGRATION_PARAMS = tuple(pytest.param(case, id=case.id, marks=case.marks) for case in ASYNC_MIGRATION_CASES)
