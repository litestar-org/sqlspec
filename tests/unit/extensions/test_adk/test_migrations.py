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
