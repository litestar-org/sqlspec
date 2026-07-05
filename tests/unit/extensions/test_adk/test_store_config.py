# pyright: reportPrivateUsage=false
"""Tests for shared ADK store configuration behavior."""

import importlib
import inspect
import logging
from datetime import datetime
from typing import Any

import pytest

import sqlspec.extensions.adk.artifact.store as artifact_store_module
import sqlspec.extensions.adk.store as session_store_module
from sqlspec.extensions.adk import EventRecord, SessionRecord
from sqlspec.extensions.adk.artifact._types import ArtifactRecord
from sqlspec.extensions.adk.artifact.store import BaseSyncADKArtifactStore
from sqlspec.extensions.adk.memory import MemoryRecord
from sqlspec.extensions.adk.memory import store as memory_store_module
from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore
from sqlspec.extensions.adk.store import BaseAsyncADKStore, BaseSyncADKStore


class _Config:
    extension_config: dict[str, dict[str, Any]]

    def __init__(self, adk_config: "dict[str, Any] | None" = None) -> None:
        self.extension_config = {"adk": adk_config or {}}

    def provide_connection(self) -> str:
        return "original-connection"

    def provide_session(self) -> str:
        return "original-session"


class _AsyncSessionStore(BaseAsyncADKStore[Any]):
    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: dict[str, Any], owner_id: Any | None = None
    ) -> SessionRecord:
        return SessionRecord(
            id=session_id,
            app_name=app_name,
            user_id=user_id,
            state=state,
            create_time=datetime.now(),
            update_time=datetime.now(),
        )

    async def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: Any | None = None
    ) -> SessionRecord | None:
        return None

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        return None

    async def list_sessions(self, app_name: str, user_id: str | None = None) -> list[SessionRecord]:
        return []

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        return None

    async def append_event(self, event_record: EventRecord) -> None:
        return None

    async def append_event_and_update_state(
        self,
        event_record: EventRecord,
        app_name: str,
        user_id: str,
        session_id: str,
        state: dict[str, Any],
        *,
        app_state: dict[str, Any] | None = None,
        user_state: dict[str, Any] | None = None,
    ) -> SessionRecord:
        return await self.create_session(session_id, app_name, user_id, state)

    async def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> list[EventRecord]:
        return []

    async def delete_expired_events(self, before: datetime) -> int:
        return 0

    async def delete_idle_sessions(self, updated_before: datetime) -> int:
        return 0

    async def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        return None

    async def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        return None

    async def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        return None

    async def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        return None

    async def get_metadata(self, key: str) -> str | None:
        return None

    async def set_metadata(self, key: str, value: str) -> None:
        return None

    async def create_tables(self) -> None:
        return None

    async def _get_create_sessions_table_sql(self) -> str:
        return ""

    async def _get_create_events_table_sql(self) -> str:
        return ""

    async def _get_create_app_states_table_sql(self) -> str:
        return ""

    async def _get_create_user_states_table_sql(self) -> str:
        return ""

    async def _get_create_metadata_table_sql(self) -> str:
        return ""

    async def _get_seed_metadata_sql(self) -> str:
        return ""

    def _get_drop_app_states_table_sql(self) -> str:
        return ""

    def _get_drop_user_states_table_sql(self) -> str:
        return ""

    def _get_drop_metadata_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self._metadata_table}"

    def _get_drop_tables_sql(self) -> list[str]:
        return [
            self._get_drop_metadata_table_sql(),
            f"DROP TABLE IF EXISTS {self._user_state_table}",
            f"DROP TABLE IF EXISTS {self._app_state_table}",
            f"DROP TABLE IF EXISTS {self._events_table}",
            f"DROP TABLE IF EXISTS {self._session_table}",
        ]


class _SyncSessionStore(BaseSyncADKStore[Any]):
    def __init__(self, config: _Config) -> None:
        super().__init__(config)
        self.create_tables_called = False

    def create_session(
        self, session_id: str, app_name: str, user_id: str, state: dict[str, Any], owner_id: Any | None = None
    ) -> SessionRecord:
        return SessionRecord(
            id=session_id,
            app_name=app_name,
            user_id=user_id,
            state=state,
            create_time=datetime.now(),
            update_time=datetime.now(),
        )

    def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: Any | None = None
    ) -> SessionRecord | None:
        return None

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        return None

    def list_sessions(self, app_name: str, user_id: str | None = None) -> list[SessionRecord]:
        return []

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        return None

    def append_event(self, event_record: EventRecord) -> None:
        return None

    def append_event_and_update_state(
        self,
        event_record: EventRecord,
        app_name: str,
        user_id: str,
        session_id: str,
        state: dict[str, Any],
        *,
        app_state: dict[str, Any] | None = None,
        user_state: dict[str, Any] | None = None,
    ) -> SessionRecord:
        return self.create_session(session_id, app_name, user_id, state)

    def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> list[EventRecord]:
        return []

    def delete_expired_events(self, before: datetime) -> int:
        return 0

    def delete_idle_sessions(self, updated_before: datetime) -> int:
        return 0

    def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        return None

    def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        return None

    def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        return None

    def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        return None

    def get_metadata(self, key: str) -> str | None:
        return None

    def set_metadata(self, key: str, value: str) -> None:
        return None

    def create_tables(self) -> None:
        self.create_tables_called = True

    def _get_create_sessions_table_sql(self) -> str:
        return ""

    def _get_create_events_table_sql(self) -> str:
        return ""

    def _get_create_app_states_table_sql(self) -> str:
        return ""

    def _get_create_user_states_table_sql(self) -> str:
        return ""

    def _get_create_metadata_table_sql(self) -> str:
        return ""

    def _get_seed_metadata_sql(self) -> str:
        return ""

    def _get_drop_app_states_table_sql(self) -> str:
        return ""

    def _get_drop_user_states_table_sql(self) -> str:
        return ""

    def _get_drop_metadata_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self._metadata_table}"

    def _get_drop_tables_sql(self) -> list[str]:
        return [
            self._get_drop_metadata_table_sql(),
            f"DROP TABLE IF EXISTS {self._user_state_table}",
            f"DROP TABLE IF EXISTS {self._app_state_table}",
            f"DROP TABLE IF EXISTS {self._events_table}",
            f"DROP TABLE IF EXISTS {self._session_table}",
        ]


class _SyncMemoryStore(BaseSyncADKMemoryStore[Any]):
    def __init__(self, config: _Config) -> None:
        super().__init__(config)
        self.create_tables_called = False

    def create_tables(self) -> None:
        self.create_tables_called = True

    def insert_memory_entries(self, entries: list[MemoryRecord], owner_id: object | None = None) -> int:
        return len(entries)

    def search_entries(self, query: str, app_name: str, user_id: str, limit: int | None = None) -> list[MemoryRecord]:
        return []

    def delete_entries_by_session(self, session_id: str) -> int:
        return 0

    def delete_entries_older_than(self, days: int) -> int:
        return 0

    def _get_create_memory_table_sql(self) -> str | list[str]:
        return ""

    def _get_drop_memory_table_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]


class _SyncArtifactStore(BaseSyncADKArtifactStore[Any]):
    def insert_artifact(self, record: ArtifactRecord) -> None:
        return None

    def get_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None, version: int | None = None
    ) -> ArtifactRecord | None:
        return None

    def list_artifact_keys(self, app_name: str, user_id: str, session_id: str | None = None) -> list[str]:
        return []

    def list_artifact_versions(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[ArtifactRecord]:
        return []

    def delete_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: str | None = None
    ) -> list[ArtifactRecord]:
        return []

    def get_next_version(self, app_name: str, user_id: str, filename: str, session_id: str | None = None) -> int:
        return 0

    def create_table(self) -> None:
        return None


@pytest.mark.parametrize("store_cls", [_AsyncSessionStore, _SyncSessionStore, _SyncMemoryStore, _SyncArtifactStore])
def test_adk_base_stores_keep_original_config(store_cls: type[Any]) -> None:
    config = _Config()
    store = store_cls(config)

    assert store.config is config


def test_adk_base_stores_do_not_keep_dead_private_helpers() -> None:
    assert "_value_to_bytes" not in BaseAsyncADKStore.__dict__
    assert "_value_to_bytes" not in BaseSyncADKStore.__dict__
    assert "_adk_config" not in BaseSyncADKArtifactStore.__dict__


def test_adk_table_helpers_have_one_private_owner() -> None:
    for module in (session_store_module, memory_store_module, artifact_store_module):
        source = inspect.getsource(module)
        assert "def _ensure_table_name(" not in source
        assert "VALID_TABLE_NAME_PATTERN" not in source
        assert "MAX_TABLE_NAME_LENGTH" not in source

    assert "def _unique_statements(" not in inspect.getsource(session_store_module)
    assert "def _unique_statements(" not in inspect.getsource(memory_store_module)
    assert "def _owner_id_column_name(" not in inspect.getsource(session_store_module)
    assert "def _owner_id_column_name(" not in inspect.getsource(memory_store_module)


def test_sync_session_store_ensure_tables_runs_sync_create_tables() -> None:
    store = _SyncSessionStore(_Config({"session_table": "sessions", "events_table": "events"}))

    store.ensure_tables()

    assert store.create_tables_called


def test_sync_memory_store_logs_ready_with_log_with_context(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    def fake_log_with_context(logger: Any, level: int, event: str, **context: Any) -> None:
        calls.append({"level": level, "event": event, "context": context})

    monkeypatch.setattr(memory_store_module, "log_with_context", fake_log_with_context)
    store = _SyncMemoryStore(_Config({"memory_table": "test_memories"}))

    store.ensure_tables()

    assert store.create_tables_called
    assert len(calls) == 1
    assert calls[0]["level"] == logging.DEBUG
    assert calls[0]["event"] == "adk.memory.table.ready"
    assert calls[0]["context"]["memory_table"] == "test_memories"
    assert "db_system" in calls[0]["context"]


def test_sync_memory_store_logs_disabled_with_log_with_context(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    def fake_log_with_context(logger: Any, level: int, event: str, **context: Any) -> None:
        calls.append({"level": level, "event": event, "context": context})

    monkeypatch.setattr(memory_store_module, "log_with_context", fake_log_with_context)
    store = _SyncMemoryStore(_Config({"enable_memory": False, "memory_table": "test_memories"}))

    store.ensure_tables()

    assert not store.create_tables_called
    assert len(calls) == 1
    assert calls[0]["level"] == logging.DEBUG
    assert calls[0]["event"] == "adk.memory.table.skipped"
    assert calls[0]["context"]["memory_table"] == "test_memories"
    assert calls[0]["context"]["reason"] == "disabled"
    assert "db_system" in calls[0]["context"]


def test_session_store_reset_drop_tables_includes_legacy_metadata_table() -> None:
    store = _AsyncSessionStore(_Config())

    statements = store._get_reset_drop_tables_sql()

    assert "DROP TABLE IF EXISTS adk_internal_metadata" in statements
    assert "DROP TABLE IF EXISTS adk_metadata" in statements
    assert "DROP TABLE IF EXISTS adk_session" in statements
    assert "DROP TABLE IF EXISTS adk_event" in statements
    assert "DROP TABLE IF EXISTS adk_app_state" in statements
    assert "DROP TABLE IF EXISTS adk_user_state" in statements
    assert "DROP TABLE IF EXISTS adk_sessions" in statements
    assert "DROP TABLE IF EXISTS adk_events" in statements
    assert "DROP TABLE IF EXISTS adk_app_states" in statements
    assert "DROP TABLE IF EXISTS adk_user_states" in statements
    assert store.metadata_table == "adk_internal_metadata"


def test_session_store_reset_drop_tables_does_not_duplicate_configured_legacy_metadata_table() -> None:
    store = _AsyncSessionStore(_Config({"metadata_table": "adk_metadata"}))

    statements = store._get_reset_drop_tables_sql()

    assert statements.count("DROP TABLE IF EXISTS adk_metadata") == 1
    assert store.metadata_table == "adk_metadata"


@pytest.mark.anyio
async def test_reset_migration_accepts_sync_session_store(monkeypatch: pytest.MonkeyPatch) -> None:
    migration = importlib.import_module("sqlspec.extensions.adk.migrations.0002_reset_adk_tables")
    context = type("Context", (), {"config": _Config()})()

    monkeypatch.setattr(migration, "_get_store_class", lambda _context: _SyncSessionStore)
    monkeypatch.setattr(migration, "_get_memory_store_class", lambda _context: None)

    statements = await migration.up(context)

    assert "" in statements


def test_sync_memory_store_reset_drop_tables_uses_drop_sql() -> None:
    store = _SyncMemoryStore(_Config({"memory_table": "agent_memory"}))

    assert store._get_reset_drop_memory_table_sql() == [
        "DROP TABLE IF EXISTS agent_memory",
        "DROP TABLE IF EXISTS adk_memory",
        "DROP TABLE IF EXISTS adk_memory_entries",
    ]
