# pyright: reportPrivateUsage=false
"""Tests for shared ADK store configuration behavior."""

import logging
from datetime import datetime
from typing import Any

import pytest

from sqlspec.extensions.adk import EventRecord, SessionRecord
from sqlspec.extensions.adk.artifact._types import ArtifactRecord
from sqlspec.extensions.adk.artifact.store import BaseSyncADKArtifactStore
from sqlspec.extensions.adk.memory import MemoryRecord
from sqlspec.extensions.adk.memory import store as memory_store_module
from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore
from sqlspec.extensions.adk.store import BaseSyncADKStore


class _Config:
    extension_config: dict[str, dict[str, Any]]

    def __init__(self, adk_config: "dict[str, Any] | None" = None) -> None:
        self.extension_config = {"adk": adk_config or {}}

    def provide_connection(self) -> str:
        return "original-connection"

    def provide_session(self) -> str:
        return "original-session"


class _SyncSessionStore(BaseSyncADKStore[Any]):
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

    def get_session(self, session_id: str) -> SessionRecord | None:
        return None

    def update_session_state(self, session_id: str, state: dict[str, Any]) -> None:
        return None

    def list_sessions(self, app_name: str, user_id: str | None = None) -> list[SessionRecord]:
        return []

    def delete_session(self, session_id: str) -> None:
        return None

    def create_event(
        self,
        event_id: str,
        session_id: str,
        app_name: str,
        user_id: str,
        author: str | None = None,
        actions: bytes | None = None,
        content: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> EventRecord:
        return EventRecord(
            session_id=session_id,
            invocation_id=event_id,
            author=author or user_id,
            timestamp=datetime.now(),
            event_data=content or {},
        )

    def create_event_and_update_state(self, event_record: EventRecord, session_id: str, state: dict[str, Any]) -> None:
        return None

    def list_events(self, session_id: str) -> list[EventRecord]:
        return []

    def create_tables(self) -> None:
        return None

    def _get_create_sessions_table_sql(self) -> str:
        return ""

    def _get_create_events_table_sql(self) -> str:
        return ""

    def _get_drop_tables_sql(self) -> list[str]:
        return []


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
        return []


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


@pytest.mark.parametrize("store_cls", [_SyncSessionStore, _SyncMemoryStore, _SyncArtifactStore])
def test_adk_base_stores_keep_original_config(store_cls: type[Any]) -> None:
    config = _Config()
    store = store_cls(config)

    assert store.config is config


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
