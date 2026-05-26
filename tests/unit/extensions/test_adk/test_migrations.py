# pyright: reportPrivateUsage = false
"""Unit tests for the ADK clean-break cutover migrations.

Covers ``sqlspec/extensions/adk/migrations/0001_create_adk_tables.py`` (no-op
after Revision 8) and ``sqlspec/extensions/adk/migrations/0002_reset_adk_tables.py``
(drop-and-recreate cutover). The reference adapter is asyncpg; the migration's
statement-set contract is the same regardless of which adapter resolves the
store class.
"""

import importlib

import pytest

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.exceptions import SQLSpecError
from sqlspec.migrations.context import MigrationContext

migration_0001 = importlib.import_module("sqlspec.extensions.adk.migrations.0001_create_adk_tables")
migration_0002 = importlib.import_module("sqlspec.extensions.adk.migrations.0002_reset_adk_tables")


def _build_config(adk: "dict[str, object] | None" = None) -> AsyncpgConfig:
    return AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"}, extension_config={"adk": adk or {}})


def _build_context(adk: "dict[str, object] | None" = None) -> MigrationContext:
    return MigrationContext(config=_build_config(adk))


def _index_of(statements: "list[str]", needle: str) -> int:
    for idx, statement in enumerate(statements):
        if needle in statement:
            return idx
    msg = f"Expected to find {needle!r} in statements: {statements}"
    raise AssertionError(msg)


class _Table:
    def __init__(self, table_id: str) -> None:
        self.table_id = table_id


class _SpannerDatabase:
    def __init__(self, table_ids: "list[str]") -> None:
        self._table_ids = table_ids
        self.list_tables_calls = 0

    def list_tables(self) -> "list[_Table]":
        self.list_tables_calls += 1
        return [_Table(table_id) for table_id in self._table_ids]


class _SpannerConfig:
    extension_config = {"adk": {"enable_memory": False}}

    def __init__(self, database: _SpannerDatabase) -> None:
        self._database = database

    def get_database(self) -> _SpannerDatabase:
        return self._database


class _SpannerDropStore:
    connector_name = "spanner"

    def __init__(self, config: _SpannerConfig) -> None:
        self._config = config

    async def _get_create_sessions_table_sql(self) -> str:
        return "CREATE TABLE adk_session"

    async def _get_create_events_table_sql(self) -> str:
        return "CREATE TABLE adk_event"

    async def _get_create_app_states_table_sql(self) -> str:
        return "CREATE TABLE adk_app_state"

    async def _get_create_user_states_table_sql(self) -> str:
        return "CREATE TABLE adk_user_state"

    async def _get_create_metadata_table_sql(self) -> str:
        return "CREATE TABLE adk_metadata"

    async def _get_seed_metadata_sql(self) -> str:
        return "INSERT INTO adk_metadata"

    def _get_drop_tables_sql(self) -> "list[str]":
        return [
            "DROP TABLE adk_metadata",
            "DROP TABLE adk_user_state",
            "DROP TABLE adk_app_state",
            "DROP TABLE adk_event",
            "DROP TABLE adk_session",
        ]


class _SpannerMemoryDropStore:
    memory_table = "adk_memory"

    def __init__(self, config: _SpannerConfig) -> None:
        self._config = config

    def _get_drop_memory_table_sql(self) -> "list[str]":
        return ["DROP INDEX idx_adk_memory_session", "DROP TABLE adk_memory"]


async def test_0001_up_is_noop_with_context() -> None:
    assert await migration_0001.up(_build_context()) == []


async def test_0001_down_is_noop_with_context() -> None:
    assert await migration_0001.down(_build_context()) == []


async def test_0001_up_is_noop_without_context() -> None:
    assert await migration_0001.up(None) == []


async def test_0001_down_is_noop_without_context() -> None:
    assert await migration_0001.down(None) == []


async def test_0002_up_with_memory_enabled_emits_full_statement_set() -> None:
    statements = await migration_0002.up(_build_context())

    memory_drop_idx = _index_of(statements, "DROP TABLE IF EXISTS adk_memory")
    metadata_drop_idx = _index_of(statements, "DROP TABLE IF EXISTS adk_metadata")
    events_drop_idx = _index_of(statements, "DROP TABLE IF EXISTS adk_event")
    session_drop_idx = _index_of(statements, "DROP TABLE IF EXISTS adk_session")
    session_create_idx = _index_of(statements, "CREATE TABLE IF NOT EXISTS adk_session")
    events_create_idx = _index_of(statements, "CREATE TABLE IF NOT EXISTS adk_event")
    app_state_create_idx = _index_of(statements, "CREATE TABLE IF NOT EXISTS adk_app_state")
    user_state_create_idx = _index_of(statements, "CREATE TABLE IF NOT EXISTS adk_user_state")
    metadata_create_idx = _index_of(statements, "CREATE TABLE IF NOT EXISTS adk_metadata")
    seed_idx = _index_of(statements, "INSERT INTO adk_metadata")
    memory_create_idx = _index_of(statements, "CREATE TABLE IF NOT EXISTS adk_memory")

    assert memory_drop_idx < metadata_drop_idx, "memory drop must precede session-store drops"
    assert metadata_drop_idx < events_drop_idx < session_drop_idx, "drops must be FK-safe"
    assert session_drop_idx < session_create_idx, "creates must follow drops"
    assert (
        session_create_idx
        < events_create_idx
        < app_state_create_idx
        < user_state_create_idx
        < metadata_create_idx
        < seed_idx
    ), "create order must satisfy FK and seed dependencies"
    assert seed_idx < memory_create_idx, "memory create runs after the session-store cutover"
    assert "schema_version" in statements[seed_idx]


async def test_0002_up_with_memory_disabled_drops_memory_table_but_skips_create() -> None:
    statements = await migration_0002.up(_build_context({"enable_memory": False}))

    assert any("DROP TABLE IF EXISTS adk_memory" in stmt for stmt in statements), (
        "memory drop must be unconditional so enable_memory=True->False transitions clean up"
    )
    assert all("CREATE TABLE IF NOT EXISTS adk_memory" not in stmt for stmt in statements), (
        "memory create must be skipped when memory is disabled"
    )
    assert any("CREATE TABLE IF NOT EXISTS adk_session" in stmt for stmt in statements)
    assert any("INSERT INTO adk_metadata" in stmt for stmt in statements)


async def test_0002_up_with_no_memory_store_class_skips_memory_branch_entirely(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(migration_0002, "_get_adk_memory_migration_store_class", lambda _config: None)

    statements = await migration_0002.up(_build_context())

    assert all("memory" not in stmt.lower() for stmt in statements), (
        "no memory drop or create when the adapter ships no memory store"
    )
    assert any("DROP TABLE IF EXISTS adk_session" in stmt for stmt in statements)
    assert any("CREATE TABLE IF NOT EXISTS adk_session" in stmt for stmt in statements)
    assert any("INSERT INTO adk_metadata" in stmt for stmt in statements)


async def test_0002_up_spanner_fresh_database_skips_missing_table_drops(monkeypatch: pytest.MonkeyPatch) -> None:
    database = _SpannerDatabase([])
    context = MigrationContext(config=_SpannerConfig(database))  # type: ignore[arg-type]
    monkeypatch.setattr(migration_0002, "_get_store_class", lambda _context: _SpannerDropStore)
    monkeypatch.setattr(migration_0002, "_get_memory_store_class", lambda _context: None)

    statements = await migration_0002.up(context)

    assert database.list_tables_calls == 1
    assert all("DROP TABLE" not in statement for statement in statements)
    assert statements[:6] == [
        "CREATE TABLE adk_session",
        "CREATE TABLE adk_event",
        "CREATE TABLE adk_app_state",
        "CREATE TABLE adk_user_state",
        "CREATE TABLE adk_metadata",
        "INSERT INTO adk_metadata",
    ]


async def test_0002_up_spanner_existing_database_keeps_fk_safe_drop_order(monkeypatch: pytest.MonkeyPatch) -> None:
    database = _SpannerDatabase(["adk_session", "adk_event", "adk_app_state", "adk_user_state", "adk_metadata"])
    context = MigrationContext(config=_SpannerConfig(database))  # type: ignore[arg-type]
    monkeypatch.setattr(migration_0002, "_get_store_class", lambda _context: _SpannerDropStore)
    monkeypatch.setattr(migration_0002, "_get_memory_store_class", lambda _context: None)

    statements = await migration_0002.up(context)

    drops = [statement for statement in statements if statement.startswith("DROP TABLE")]
    assert drops == [
        "DROP TABLE adk_metadata",
        "DROP TABLE adk_user_state",
        "DROP TABLE adk_app_state",
        "DROP TABLE adk_event",
        "DROP TABLE adk_session",
    ]


async def test_0002_up_spanner_memory_drops_are_grouped_by_existing_memory_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _SpannerDatabase(["adk_memory"])
    context = MigrationContext(config=_SpannerConfig(database))  # type: ignore[arg-type]
    monkeypatch.setattr(migration_0002, "_get_store_class", lambda _context: _SpannerDropStore)
    monkeypatch.setattr(migration_0002, "_get_memory_store_class", lambda _context: _SpannerMemoryDropStore)

    statements = await migration_0002.up(context)

    assert statements[:2] == ["DROP INDEX idx_adk_memory_session", "DROP TABLE adk_memory"]


async def test_0002_down_with_memory_enabled_drops_memory_then_new_tables() -> None:
    statements = await migration_0002.down(_build_context())

    memory_drop_idx = _index_of(statements, "DROP TABLE IF EXISTS adk_memory")
    session_drop_idx = _index_of(statements, "DROP TABLE IF EXISTS adk_session")
    metadata_drop_idx = _index_of(statements, "DROP TABLE IF EXISTS adk_metadata")

    assert memory_drop_idx < metadata_drop_idx < session_drop_idx, (
        "down() drops memory first, then new tables FK-safe (children before parents)"
    )
    assert all("CREATE TABLE" not in stmt for stmt in statements)


async def test_0002_down_with_memory_disabled_drops_only_new_tables() -> None:
    statements = await migration_0002.down(_build_context({"enable_memory": False}))

    assert all("adk_memory" not in stmt for stmt in statements), (
        "down() does not touch memory when memory is disabled for this config"
    )
    assert any("DROP TABLE IF EXISTS adk_session" in stmt for stmt in statements)
    assert any("DROP TABLE IF EXISTS adk_metadata" in stmt for stmt in statements)


async def test_0002_up_raises_when_context_missing() -> None:
    with pytest.raises(SQLSpecError, match="Migration context must have a config"):
        await migration_0002.up(None)


async def test_0002_down_raises_when_context_missing() -> None:
    with pytest.raises(SQLSpecError, match="Migration context must have a config"):
        await migration_0002.down(None)


async def test_0002_up_raises_when_context_config_missing() -> None:
    with pytest.raises(SQLSpecError, match="Migration context must have a config"):
        await migration_0002.up(MigrationContext(config=None))


async def test_0002_down_raises_when_context_config_missing() -> None:
    with pytest.raises(SQLSpecError, match="Migration context must have a config"):
        await migration_0002.down(MigrationContext(config=None))
