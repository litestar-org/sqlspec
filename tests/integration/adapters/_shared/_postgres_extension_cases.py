"""Case records for PostgreSQL extension contract tests."""

from dataclasses import dataclass
from typing import Literal

import pytest
from _pytest.mark.structures import Mark, MarkDecorator, ParameterSet

PostgresExtensionDialect = Literal["pgvector", "paradedb"]
PostgresExtensionMode = Literal["sync", "async"]
InnerProductStrategy = Literal["driver", "psycopg_cursor", "psqlpy_fetch"]


@dataclass(frozen=True)
class PostgresExtensionCase:
    """Config fixture and capability metadata for PostgreSQL extension contracts."""

    id: str
    adapter: str
    mode: PostgresExtensionMode
    dialect: PostgresExtensionDialect
    config_fixture_name: str
    inner_product_strategy: InnerProductStrategy = "driver"
    marks: tuple[Mark | MarkDecorator, ...] = ()


@dataclass(frozen=True)
class PostgresExtensionCaseContext:
    """Resolved config/driver pair for a PostgreSQL extension case."""

    case: PostgresExtensionCase
    config: object
    driver: object


PGVECTOR_MARK = pytest.mark.xdist_group("pgvector")
PARADEDB_MARK = pytest.mark.xdist_group("paradedb")

SYNC_POSTGRES_EXTENSION_CASES = (
    PostgresExtensionCase(
        id="adbc-pgvector-sync",
        adapter="adbc",
        mode="sync",
        dialect="pgvector",
        config_fixture_name="pgvector_config_adbc",
        marks=(PGVECTOR_MARK,),
    ),
    PostgresExtensionCase(
        id="psycopg-pgvector-sync",
        adapter="psycopg",
        mode="sync",
        dialect="pgvector",
        config_fixture_name="pgvector_config_psycopg",
        inner_product_strategy="psycopg_cursor",
        marks=(PGVECTOR_MARK,),
    ),
    PostgresExtensionCase(
        id="adbc-paradedb-sync",
        adapter="adbc",
        mode="sync",
        dialect="paradedb",
        config_fixture_name="paradedb_config_adbc",
        marks=(PARADEDB_MARK,),
    ),
    PostgresExtensionCase(
        id="psycopg-paradedb-sync",
        adapter="psycopg",
        mode="sync",
        dialect="paradedb",
        config_fixture_name="paradedb_config_psycopg",
        inner_product_strategy="psycopg_cursor",
        marks=(PARADEDB_MARK,),
    ),
)

ASYNC_POSTGRES_EXTENSION_CASES = (
    PostgresExtensionCase(
        id="asyncpg-pgvector-async",
        adapter="asyncpg",
        mode="async",
        dialect="pgvector",
        config_fixture_name="pgvector_config_asyncpg",
        marks=(PGVECTOR_MARK, pytest.mark.anyio),
    ),
    PostgresExtensionCase(
        id="psqlpy-pgvector-async",
        adapter="psqlpy",
        mode="async",
        dialect="pgvector",
        config_fixture_name="pgvector_config_psqlpy",
        inner_product_strategy="psqlpy_fetch",
        marks=(PGVECTOR_MARK, pytest.mark.anyio),
    ),
    PostgresExtensionCase(
        id="asyncpg-paradedb-async",
        adapter="asyncpg",
        mode="async",
        dialect="paradedb",
        config_fixture_name="paradedb_config_asyncpg",
        marks=(PARADEDB_MARK, pytest.mark.anyio),
    ),
    PostgresExtensionCase(
        id="psqlpy-paradedb-async",
        adapter="psqlpy",
        mode="async",
        dialect="paradedb",
        config_fixture_name="paradedb_config_psqlpy",
        inner_product_strategy="psqlpy_fetch",
        marks=(PARADEDB_MARK, pytest.mark.anyio),
    ),
)


def _postgres_extension_params(cases: tuple[PostgresExtensionCase, ...]) -> tuple[ParameterSet, ...]:
    return tuple(pytest.param(case, id=case.id, marks=case.marks) for case in cases)


def _postgres_extension_params_with(
    cases: tuple[PostgresExtensionCase, ...], dialect: PostgresExtensionDialect
) -> tuple[ParameterSet, ...]:
    return _postgres_extension_params(tuple(case for case in cases if case.dialect == dialect))


SYNC_POSTGRES_EXTENSION_PARAMS = _postgres_extension_params(SYNC_POSTGRES_EXTENSION_CASES)
ASYNC_POSTGRES_EXTENSION_PARAMS = _postgres_extension_params(ASYNC_POSTGRES_EXTENSION_CASES)


def sync_postgres_extension_params_with(dialect: PostgresExtensionDialect) -> tuple[ParameterSet, ...]:
    """Return sync Postgres extension params for one extension dialect."""
    return _postgres_extension_params_with(SYNC_POSTGRES_EXTENSION_CASES, dialect)


def async_postgres_extension_params_with(dialect: PostgresExtensionDialect) -> tuple[ParameterSet, ...]:
    """Return async Postgres extension params for one extension dialect."""
    return _postgres_extension_params_with(ASYNC_POSTGRES_EXTENSION_CASES, dialect)
