# pyright: reportPrivateUsage=false
"""Tests for the packaged ADK schema migration."""

import importlib
from typing import Any

import pytest

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig
from sqlspec.adapters.psqlpy import PsqlpyConfig, PsqlpyPoolParams
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgPoolParams
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.migrations.context import MigrationContext

migration = importlib.import_module("sqlspec.extensions.adk.migrations.0001_create_adk_tables")

CREATE_VECTOR_EXTENSION = "CREATE EXTENSION IF NOT EXISTS vector"
POSTGRES_CONNECTION = {"host": "localhost", "port": 5432, "user": "adk", "password": "adk", "database": "adk"}


class _VectorMemoryStore:
    """Memory store stub whose DDL declares a vector column."""

    memory_table = "adk_memory"

    def __init__(self, config: Any) -> None:
        self._config = config

    def _memory_table_ddl(self) -> str:
        return "CREATE TABLE adk_memory (id VARCHAR(128) PRIMARY KEY, embedding VECTOR(3))"


def _sqlite_config(adk_settings: "dict[str, Any] | None" = None) -> SqliteConfig:
    return SqliteConfig(connection_config={"database": ":memory:"}, extension_config={"adk": adk_settings or {}})


def _asyncpg_config(adk_settings: "dict[str, Any] | None" = None) -> AsyncpgConfig:
    return AsyncpgConfig(connection_config=dict(POSTGRES_CONNECTION), extension_config={"adk": adk_settings or {}})


def _spy_on(monkeypatch: pytest.MonkeyPatch, attribute: str) -> "list[str]":
    calls: list[str] = []
    original = getattr(migration, attribute)

    def spy(context: Any) -> Any:
        calls.append(attribute)
        return original(context)

    monkeypatch.setattr(migration, attribute, spy)
    return calls


@pytest.mark.parametrize("enable_sessions", [True, False])
@pytest.mark.parametrize("enable_memory", [True, False])
async def test_create_migration_emits_ddl_only_for_enabled_features(enable_sessions: bool, enable_memory: bool) -> None:
    """Both feature flags gate their own DDL in both migration directions."""
    config = _sqlite_config({"enable_sessions": enable_sessions, "enable_memory": enable_memory})
    context = MigrationContext(config=config, dialect="sqlite")

    up_statements = await migration.up(context)
    down_statements = await migration.down(context)

    assert any("CREATE TABLE IF NOT EXISTS adk_session " in sql for sql in up_statements) is enable_sessions
    assert any("CREATE TABLE IF NOT EXISTS adk_event " in sql for sql in up_statements) is enable_sessions
    assert any("CREATE TABLE IF NOT EXISTS adk_memory " in sql for sql in up_statements) is enable_memory
    assert ("DROP TABLE IF EXISTS adk_session" in down_statements) is enable_sessions
    assert ("DROP TABLE IF EXISTS adk_memory" in down_statements) is enable_memory


async def test_create_migration_skips_store_resolution_for_disabled_features(monkeypatch: pytest.MonkeyPatch) -> None:
    """A disabled feature never resolves its store class in either direction."""
    config = _sqlite_config({"enable_sessions": False, "enable_memory": False})
    context = MigrationContext(config=config, dialect="sqlite")
    session_calls = _spy_on(monkeypatch, "_get_store_class")
    memory_calls = _spy_on(monkeypatch, "_get_memory_store_class")

    assert await migration.up(context) == []
    assert await migration.down(context) == []
    assert session_calls == []
    assert memory_calls == []


async def test_create_migration_resolves_only_the_enabled_store_class(monkeypatch: pytest.MonkeyPatch) -> None:
    """Enabling one feature resolves that store class and no other."""
    config = _sqlite_config({"enable_sessions": False, "enable_memory": True})
    context = MigrationContext(config=config, dialect="sqlite")
    session_calls = _spy_on(monkeypatch, "_get_store_class")
    memory_calls = _spy_on(monkeypatch, "_get_memory_store_class")

    await migration.up(context)

    assert session_calls == []
    assert memory_calls == ["_get_memory_store_class"]


@pytest.mark.parametrize("dialect", ["postgres", "postgresql", "pgvector", "paradedb"])
async def test_asyncpg_memory_migration_installs_vector_extension_before_first_use(dialect: str) -> None:
    """PostgreSQL vector memory DDL is preceded by exactly one extension statement."""
    context = MigrationContext(config=_asyncpg_config(), dialect=dialect)

    statements = await migration.up(context)

    assert statements.count(CREATE_VECTOR_EXTENSION) == 1
    extension_index = statements.index(CREATE_VECTOR_EXTENSION)
    vector_index = next(index for index, sql in enumerate(statements) if "embedding VECTOR(" in sql)
    assert extension_index < vector_index


async def test_psycopg_memory_migration_installs_vector_extension() -> None:
    """The second PostgreSQL adapter with vector DDL also gets the prerequisite."""
    config = PsycopgAsyncConfig(
        connection_config=PsycopgPoolParams(conninfo="postgresql://adk@localhost/adk"), extension_config={"adk": {}}
    )
    context = MigrationContext(config=config, dialect="postgres")

    statements = await migration.up(context)

    assert statements.count(CREATE_VECTOR_EXTENSION) == 1
    assert statements.index(CREATE_VECTOR_EXTENSION) < next(
        index for index, sql in enumerate(statements) if "embedding VECTOR(" in sql
    )


async def test_disabled_memory_emits_no_vector_extension() -> None:
    """Disabling memory suppresses the memory DDL and its extension prerequisite."""
    context = MigrationContext(config=_asyncpg_config({"enable_memory": False}), dialect="postgres")

    statements = await migration.up(context)

    assert CREATE_VECTOR_EXTENSION not in statements
    assert not any("embedding VECTOR(" in sql for sql in statements)


async def test_psqlpy_postgres_memory_migration_emits_no_vector_extension() -> None:
    """A PostgreSQL store whose memory DDL has no vector column installs nothing."""
    config = PsqlpyConfig(
        connection_config=PsqlpyPoolParams(dsn="postgresql://adk@localhost/adk"), extension_config={"adk": {}}
    )
    context = MigrationContext(config=config, dialect="postgres")

    statements = await migration.up(context)

    assert CREATE_VECTOR_EXTENSION not in statements
    assert any("CREATE TABLE IF NOT EXISTS adk_memory " in sql for sql in statements)


async def test_cockroach_memory_migration_emits_no_vector_extension() -> None:
    """CockroachDB reports the postgres dialect but never gets extension DDL."""
    config = CockroachAsyncpgConfig(connection_config=dict(POSTGRES_CONNECTION), extension_config={"adk": {}})
    context = MigrationContext(config=config, dialect="postgres")

    statements = await migration.up(context)

    assert CREATE_VECTOR_EXTENSION not in statements
    assert any("CREATE TABLE IF NOT EXISTS adk_memory " in sql for sql in statements)


async def test_non_postgres_dialect_with_vector_ddl_emits_no_extension(monkeypatch: pytest.MonkeyPatch) -> None:
    """Vector syntax outside PostgreSQL never triggers PostgreSQL extension DDL."""
    context = MigrationContext(config=_sqlite_config(), dialect="duckdb")
    monkeypatch.setattr(migration, "_get_memory_store_class", lambda _context: _VectorMemoryStore)

    statements = await migration.up(context)

    assert CREATE_VECTOR_EXTENSION not in statements
    assert any("embedding VECTOR(3)" in sql for sql in statements)


def test_reset_migration_is_not_packaged() -> None:
    """Only the canonical schema migration ships with the ADK extension."""
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("sqlspec.extensions.adk.migrations.0002_reset_adk_tables")
