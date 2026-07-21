"""Case records for shared migration-lifecycle contract tests."""

from dataclasses import dataclass
from typing import Literal

import pytest
from _pytest.mark.structures import Mark, MarkDecorator, ParameterSet

from tests.integration.adapters._shared._cases import (
    ADBC_MARK,
    DUCKDB_XDIST_MARK,
    MYSQL_XDIST_MARK,
    ORACLE_XDIST_MARK,
    POSTGRES_XDIST_MARK,
    SQLITE_XDIST_MARK,
)

MigrationSchemaDialect = Literal["duckdb", "postgres"]


@dataclass(frozen=True)
class MigrationCase:
    """Config-factory and capability metadata for migration contract tests."""

    id: str
    factory_fixture: str
    adapter: str
    mode: Literal["sync", "async"]
    schema_dialect: MigrationSchemaDialect | None = None
    supports_default_schema: bool = False
    supports_non_transactional_default_schema: bool = False
    supports_multi_schema_migrations: bool = False
    supports_missing_schema_validation: bool = False
    marks: tuple[Mark | MarkDecorator, ...] = ()


@dataclass(frozen=True)
class MigrationCaseContext:
    """Resolved config factory paired with its case metadata."""

    case: MigrationCase
    make_config: object


SYNC_MIGRATION_CASES = (
    MigrationCase("sqlite-sync", "migration_config_sqlite", "sqlite", "sync", marks=(SQLITE_XDIST_MARK,)),
    MigrationCase(
        "duckdb-sync",
        "migration_config_duckdb",
        "duckdb",
        "sync",
        schema_dialect="duckdb",
        supports_default_schema=True,
        supports_multi_schema_migrations=True,
        supports_missing_schema_validation=True,
        marks=(DUCKDB_XDIST_MARK,),
    ),
    MigrationCase("pymysql-sync", "migration_config_pymysql", "pymysql", "sync", marks=(MYSQL_XDIST_MARK,)),
    MigrationCase(
        "psycopg-sync",
        "migration_config_psycopg_sync",
        "psycopg",
        "sync",
        schema_dialect="postgres",
        supports_default_schema=True,
        supports_multi_schema_migrations=True,
        supports_missing_schema_validation=True,
        marks=(POSTGRES_XDIST_MARK,),
    ),
    MigrationCase(
        "adbc-postgres-sync",
        "migration_config_adbc_postgres",
        "adbc",
        "sync",
        schema_dialect="postgres",
        supports_default_schema=True,
        supports_multi_schema_migrations=True,
        supports_missing_schema_validation=True,
        marks=(ADBC_MARK, POSTGRES_XDIST_MARK),
    ),
    MigrationCase(
        "adbc-sqlite-sync", "migration_config_adbc_sqlite", "adbc", "sync", marks=(ADBC_MARK, SQLITE_XDIST_MARK)
    ),
    MigrationCase("oracledb-sync", "migration_config_oracle_sync", "oracledb", "sync", marks=(ORACLE_XDIST_MARK,)),
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
        "asyncpg-async",
        "migration_config_asyncpg",
        "asyncpg",
        "async",
        schema_dialect="postgres",
        supports_default_schema=True,
        supports_non_transactional_default_schema=True,
        supports_multi_schema_migrations=True,
        supports_missing_schema_validation=True,
        marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio),
    ),
    MigrationCase(
        "psqlpy-async",
        "migration_config_psqlpy",
        "psqlpy",
        "async",
        schema_dialect="postgres",
        supports_default_schema=True,
        supports_non_transactional_default_schema=True,
        supports_multi_schema_migrations=True,
        supports_missing_schema_validation=True,
        marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio),
    ),
    MigrationCase(
        "psycopg-async",
        "migration_config_psycopg_async",
        "psycopg",
        "async",
        schema_dialect="postgres",
        supports_default_schema=True,
        supports_multi_schema_migrations=True,
        supports_missing_schema_validation=True,
        marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio),
    ),
    MigrationCase(
        "oracledb-async",
        "migration_config_oracle_async",
        "oracledb",
        "async",
        marks=(ORACLE_XDIST_MARK, pytest.mark.anyio),
    ),
)

ACTIVE_MIGRATION_CASES = SYNC_MIGRATION_CASES + ASYNC_MIGRATION_CASES
SYNC_MIGRATION_PARAMS = tuple(pytest.param(case, id=case.id, marks=case.marks) for case in SYNC_MIGRATION_CASES)
ASYNC_MIGRATION_PARAMS = tuple(pytest.param(case, id=case.id, marks=case.marks) for case in ASYNC_MIGRATION_CASES)


def _migration_params_where(cases: tuple[MigrationCase, ...], *capability_names: str) -> tuple[ParameterSet, ...]:
    return tuple(
        pytest.param(case, id=case.id, marks=case.marks)
        for case in cases
        if any(getattr(case, name) for name in capability_names)
    )


def sync_migration_params_with(*capability_names: str) -> tuple[ParameterSet, ...]:
    """Return sync migration params that opt into at least one named capability."""
    return _migration_params_where(SYNC_MIGRATION_CASES, *capability_names)


def async_migration_params_with(*capability_names: str) -> tuple[ParameterSet, ...]:
    """Return async migration params that opt into at least one named capability."""
    return _migration_params_where(ASYNC_MIGRATION_CASES, *capability_names)
