# pyright: reportPrivateUsage=false
"""Tests for the packaged ADK schema migration."""

import importlib
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.adk.store import AiosqliteADKStore
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig
from sqlspec.adapters.psqlpy import PsqlpyConfig, PsqlpyPoolParams
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgPoolParams
from sqlspec.adapters.spanner import SpannerAsyncConfig, SpannerSyncConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.adk.store import SqliteADKStore
from sqlspec.core import StatementConfig
from sqlspec.exceptions import MigrationError
from sqlspec.migrations.context import MigrationContext

migration = importlib.import_module("sqlspec.extensions.adk.migrations.0001_create_adk_tables")

CREATE_VECTOR_EXTENSION = "CREATE EXTENSION IF NOT EXISTS vector"
CREATE_PG_TEXTSEARCH_EXTENSION = "CREATE EXTENSION IF NOT EXISTS pg_textsearch"
POSTGRES_CONNECTION = {"host": "localhost", "port": 5432, "user": "adk", "password": "adk", "database": "adk"}


class _VectorMemoryStore:
    """Memory store stub whose DDL declares a vector column."""

    memory_table = "adk_memory"

    def __init__(self, config: Any) -> None:
        self._config = config

    def _memory_table_ddl(self) -> str:
        return "CREATE TABLE adk_memory (id VARCHAR(128) PRIMARY KEY, embedding VECTOR(3))"


class _BM25MemoryStore(_VectorMemoryStore):
    """Memory store stub whose DDL uses BM25 without a vector column."""

    def _memory_table_ddl(self) -> str:
        return "CREATE INDEX idx_adk_memory_bm25 ON adk_memory USING bm25 (content_text)"


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


async def test_create_migration_down_awaits_async_store_reset_drops() -> None:
    """Rolling back through async stores emits only the drops their awaited reset hooks keep."""
    config = SpannerAsyncConfig(
        connection_config={"project": "p", "instance_id": "i", "database_id": "d"},
        extension_config={"adk": {"enable_memory": True}},
    )
    database = MagicMock()

    async def _list_tables() -> Any:
        yield SimpleNamespace(table_id="adk_event")
        yield SimpleNamespace(table_id="adk_memory_entries")

    database.list_tables.side_effect = _list_tables

    with patch.object(SpannerAsyncConfig, "get_database", AsyncMock(return_value=database)):
        statements = await migration.down(MigrationContext(config=config, dialect="spanner"))

    assert statements == [
        "DROP INDEX idx_adk_memory_entries_event_id",
        "DROP INDEX idx_adk_memory_entries_session",
        "DROP INDEX idx_adk_memory_entries_app_scope_user_time",
        "DROP INDEX idx_adk_memory_entries_scope",
        "DROP TABLE adk_memory_entries",
        "DROP INDEX idx_adk_event_timestamp",
        "DROP TABLE adk_event",
    ]


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


@pytest.mark.parametrize("dialect", ["postgres", "postgresql", "pgvector", "paradedb", "pg_textsearch", "pgtextsearch"])
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


@pytest.mark.parametrize("dialect", ["postgres", "pg_textsearch", "pgtextsearch"])
async def test_postgres_bm25_migration_installs_pg_textsearch_extension(dialect: str) -> None:
    """BM25 DDL is preceded by an idempotent pg_textsearch enablement statement."""
    context = MigrationContext(config=_asyncpg_config({"enable_bm25": True}), dialect=dialect)

    statements = await migration.up(context)

    assert statements.count(CREATE_PG_TEXTSEARCH_EXTENSION) == 1
    extension_index = statements.index(CREATE_PG_TEXTSEARCH_EXTENSION)
    bm25_index = next(index for index, sql in enumerate(statements) if "USING bm25 (content_text)" in sql)
    assert extension_index < bm25_index


async def test_postgres_memory_migration_omits_pg_textsearch_when_bm25_is_disabled() -> None:
    """The optional BM25 extension is not enabled for vector-only memory stores."""
    context = MigrationContext(config=_asyncpg_config({"enable_bm25": False}), dialect="postgres")

    statements = await migration.up(context)

    assert CREATE_PG_TEXTSEARCH_EXTENSION not in statements


async def test_disabled_memory_emits_no_vector_extension() -> None:
    """Disabling memory suppresses the memory DDL and its extension prerequisite."""
    context = MigrationContext(config=_asyncpg_config({"enable_memory": False}), dialect="postgres")

    statements = await migration.up(context)

    assert CREATE_VECTOR_EXTENSION not in statements
    assert not any("embedding VECTOR(" in sql for sql in statements)


async def test_psqlpy_postgres_memory_migration_installs_vector_extension() -> None:
    """Psqlpy memory DDL with vector column also gets the vector extension prerequisite."""
    config = PsqlpyConfig(
        connection_config=PsqlpyPoolParams(dsn="postgresql://adk@localhost/adk"), extension_config={"adk": {}}
    )
    context = MigrationContext(config=config, dialect="postgres")

    statements = await migration.up(context)

    assert statements.count(CREATE_VECTOR_EXTENSION) == 1
    assert statements.index(CREATE_VECTOR_EXTENSION) < next(
        index for index, sql in enumerate(statements) if "embedding VECTOR(" in sql
    )
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


async def test_postgres_bm25_only_ddl_installs_only_pg_textsearch(monkeypatch: pytest.MonkeyPatch) -> None:
    """BM25 and vector extension prerequisites are independent."""
    context = MigrationContext(config=_asyncpg_config({"enable_sessions": False}), dialect="postgres")
    monkeypatch.setattr(migration, "_get_memory_store_class", lambda _context: _BM25MemoryStore)

    statements = await migration.up(context)

    assert CREATE_VECTOR_EXTENSION not in statements
    assert statements == [
        CREATE_PG_TEXTSEARCH_EXTENSION,
        "CREATE INDEX idx_adk_memory_bm25 ON adk_memory USING bm25 (content_text)",
    ]


def test_reset_migration_is_not_packaged() -> None:
    """Only the canonical schema migration ships with the ADK extension."""
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("sqlspec.extensions.adk.migrations.0002_reset_adk_tables")


async def test_adk_migration_runs_for_external_config_with_spanner_adapter_name() -> None:
    """External config subclass setting adapter_name = 'spanner' resolves Spanner ADK stores and runs migrations."""

    class ExternalSpannerConfig(SpannerSyncConfig):
        adapter_name = "spanner"

    ExternalSpannerConfig.__module__ = "myapp.database.config"
    config = ExternalSpannerConfig(
        connection_config={"project": "p", "instance_id": "i", "database_id": "d"},
        extension_config={"adk": {"enable_memory": True}},
    )
    context = MigrationContext(config=config, dialect="spanner")

    up_statements = await migration.up(context)

    assert any("CREATE TABLE adk_session" in sql for sql in up_statements)
    assert any("CREATE TABLE adk_event" in sql for sql in up_statements)
    assert any("CREATE TABLE adk_memory" in sql for sql in up_statements)


def _external_config(
    adapter_name: str | None, dialect: str | None, connection_config: dict[str, Any] | None = None
) -> Any:
    stmt_config = StatementConfig(dialect=dialect) if dialect is not None else SimpleNamespace(dialect=None)

    class _ExternalStubConfig:
        statement_config = stmt_config
        connection_config: dict[str, Any]
        extension_config: dict[str, Any]
        adk_store_class: Any = None

        def __init__(self) -> None:
            self.connection_config = connection_config or {}
            self.extension_config = {"adk": {"migration_mode": "reconcile"}}

    if adapter_name is not None:
        setattr(_ExternalStubConfig, "adapter_name", adapter_name)
    return _ExternalStubConfig()


@pytest.mark.parametrize(
    "config_factory",
    [
        lambda: _external_config("acme", None, {}),
        lambda: _external_config("arrow_odbc", "postgres", {}),
        lambda: _external_config("adbc", "sqlite", {"driver_name": "adbc_driver_flightsql"}),
        lambda: AdbcConfig(
            connection_config={"driver_name": "adbc_driver_flightsql"},
            extension_config={"adk": {"migration_mode": "reconcile"}},
        ),
        lambda: _external_config(None, "sqlite", {}),
    ],
    ids=[
        "external-acme",
        "arrow-odbc-postgres",
        "external-adbc-flightsql",
        "adbc-config-flightsql",
        "missing-adapter-name",
    ],
)
def test_adk_reconcile_rejects_unsupported_adapter_before_ddl(config_factory: Any) -> None:
    """Unsupported adapters and unclassified ADBC FlightSQL raise MigrationError before touching driver."""
    reconcile_mod = importlib.import_module("sqlspec.extensions.adk.migrations.reconcile")
    driver = MagicMock()
    config = config_factory()

    with pytest.raises(MigrationError):
        reconcile_mod.reconcile(MigrationContext(config=config, dialect="sqlite", driver=driver))

    assert driver.mock_calls == []


@pytest.mark.anyio
async def test_adk_reconcile_rejects_store_driver_mismatch() -> None:
    """Mismatched sync/async ADK stores and drivers raise MigrationError naming the store."""
    reconcile_mod = importlib.import_module("sqlspec.extensions.adk.migrations.reconcile")

    class MismatchedSqliteConfig(SqliteConfig):
        adk_store_class = AiosqliteADKStore

    class MismatchedAiosqliteConfig(AiosqliteConfig):
        adk_store_class = SqliteADKStore

    sync_config = MismatchedSqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"adk": {"migration_mode": "reconcile", "enable_memory": False}},
    )
    async_config = MismatchedAiosqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"adk": {"migration_mode": "reconcile", "enable_memory": False}},
    )
    try:
        with sync_config.provide_session() as sync_driver, pytest.raises(MigrationError, match="AiosqliteADKStore"):
            reconcile_mod.reconcile(MigrationContext(config=sync_config, dialect="sqlite", driver=sync_driver))

        async with async_config.provide_session() as async_driver:
            with pytest.raises(MigrationError, match="SqliteADKStore"):
                reconcile_mod.reconcile(MigrationContext(config=async_config, dialect="sqlite", driver=async_driver))
    finally:
        sync_config.close_pool()
        await async_config.close_pool()


@pytest.mark.anyio
async def test_adk_reconcile_store_resolution_and_disabled_feature_edges(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing memory store, invalid ADK settings, invalid table names, and disabled features behave deterministically."""
    reconcile_mod = importlib.import_module("sqlspec.extensions.adk.migrations.reconcile")
    driver = MagicMock()

    bad_mode_cfg = _external_config("sqlite", "sqlite")
    bad_mode_cfg.extension_config["adk"]["migration_mode"] = "bogus"
    with pytest.raises(MigrationError, match="migration_mode"):
        reconcile_mod.reconcile(MigrationContext(config=bad_mode_cfg, dialect="sqlite", driver=driver))

    unknown_key_cfg = _external_config("sqlite", "sqlite")
    unknown_key_cfg.extension_config["adk"]["unknown_key"] = 1
    with pytest.raises(MigrationError, match="unknown_key"):
        reconcile_mod.reconcile(MigrationContext(config=unknown_key_cfg, dialect="sqlite", driver=driver))

    bad_table_cfg = _external_config("sqlite", "sqlite")
    bad_table_cfg.adk_store_class = SqliteADKStore
    bad_table_cfg.extension_config["adk"] = {
        "migration_mode": "reconcile",
        "enable_memory": False,
        "session_table": "bad;table",
    }
    with pytest.raises(MigrationError):
        reconcile_mod.reconcile(MigrationContext(config=bad_table_cfg, dialect="sqlite", driver=driver))

    missing_mem_cfg = _sqlite_config({"migration_mode": "reconcile", "enable_sessions": False, "enable_memory": True})
    disabled_sync_cfg = _sqlite_config({
        "migration_mode": "reconcile",
        "enable_sessions": False,
        "enable_memory": False,
    })
    disabled_async_cfg = AiosqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"adk": {"migration_mode": "reconcile", "enable_sessions": False, "enable_memory": False}},
    )
    try:
        with monkeypatch.context() as m:
            m.setattr(reconcile_mod, "_adk_memory_migration_store_class", lambda _cfg: None)
            with pytest.raises(MigrationError, match="ADKMemoryStore"):
                reconcile_mod.reconcile(MigrationContext(config=missing_mem_cfg, dialect="sqlite", driver=driver))

        assert (
            reconcile_mod.reconcile(MigrationContext(config=disabled_sync_cfg, dialect="sqlite", driver=driver)) is None
        )
        assert driver.mock_calls == []

        async with disabled_async_cfg.provide_session() as async_driver:
            coro = reconcile_mod.reconcile(
                MigrationContext(config=disabled_async_cfg, dialect="sqlite", driver=async_driver)
            )
            assert coro is not None
            await coro
    finally:
        missing_mem_cfg.close_pool()
        disabled_sync_cfg.close_pool()
        await disabled_async_cfg.close_pool()


def test_adk_reconcile_requires_driver_and_config() -> None:
    """Missing context.driver or context.config raises MigrationError before store or driver execution."""
    reconcile_mod = importlib.import_module("sqlspec.extensions.adk.migrations.reconcile")
    config = _sqlite_config({"migration_mode": "reconcile"})
    driver = MagicMock()
    try:
        with pytest.raises(MigrationError, match="driver"):
            reconcile_mod.reconcile(MigrationContext(config=config, dialect="sqlite"))

        with pytest.raises(MigrationError, match="config"):
            reconcile_mod.reconcile(MigrationContext(dialect="sqlite", driver=driver))

        assert driver.mock_calls == []
    finally:
        config.close_pool()
