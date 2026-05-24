# pyright: reportPrivateUsage=false
"""Tests for shared ADK store configuration behavior."""

import inspect
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

import sqlspec.extensions.adk as adk_module
from sqlspec.extensions.adk import EventRecord, SessionRecord
from sqlspec.extensions.adk import store as adk_store_module
from sqlspec.extensions.adk.artifact._types import ArtifactRecord
from sqlspec.extensions.adk.artifact.store import BaseSyncADKArtifactStore
from sqlspec.extensions.adk.memory import MemoryRecord
from sqlspec.extensions.adk.memory import store as memory_store_module
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore
from sqlspec.extensions.adk.store import BaseAsyncADKStore


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

    async def get_session(self, session_id: str, *, renew_for: int | timedelta | None = None) -> SessionRecord | None:
        return None

    async def update_session_state(self, session_id: str, state: dict[str, Any]) -> None:
        return None

    async def list_sessions(self, app_name: str, user_id: str | None = None) -> list[SessionRecord]:
        return []

    async def delete_session(self, session_id: str) -> None:
        return None

    async def append_event(self, event_record: EventRecord) -> None:
        return None

    async def append_event_and_update_state(
        self, event_record: EventRecord, session_id: str, state: dict[str, Any]
    ) -> SessionRecord:
        return SessionRecord(
            id=session_id,
            app_name="test-app",
            user_id="test-user",
            state=state,
            create_time=datetime.now(),
            update_time=datetime.now(),
        )

    async def get_events(
        self, session_id: str, after_timestamp: datetime | None = None, limit: int | None = None
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
        return ""

    def _get_drop_tables_sql(self) -> list[str]:
        return []


class _MigrationSessionStore(_AsyncSessionStore):
    async def _get_create_sessions_table_sql(self) -> str:
        return "create sessions"

    async def _get_create_events_table_sql(self) -> str:
        return "create events"

    async def _get_create_app_states_table_sql(self) -> str:
        return "create app states"

    async def _get_create_user_states_table_sql(self) -> str:
        return "create user states"

    async def _get_create_metadata_table_sql(self) -> str:
        return "create metadata"

    async def _get_seed_metadata_sql(self) -> str:
        return "seed metadata"


class _AsyncMemoryStore(BaseAsyncADKMemoryStore[Any]):
    def __init__(self, config: _Config) -> None:
        super().__init__(config)
        self.create_tables_called = False

    async def create_tables(self) -> None:
        self.create_tables_called = True

    async def insert_memory_entries(self, entries: list[MemoryRecord], owner_id: object | None = None) -> int:
        return len(entries)

    async def search_entries(
        self, query: str, app_name: str, user_id: str, limit: int | None = None
    ) -> list[MemoryRecord]:
        return []

    async def delete_entries_by_session(self, session_id: str) -> int:
        return 0

    async def delete_entries_older_than(self, days: int) -> int:
        return 0

    async def _get_create_memory_table_sql(self) -> str | list[str]:
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


@pytest.mark.parametrize("store_cls", [_AsyncSessionStore, _AsyncMemoryStore, _SyncArtifactStore])
def test_adk_base_stores_keep_original_config(store_cls: type[Any]) -> None:
    config = _Config()
    store = store_cls(config)

    assert store.config is config


def test_session_store_contract_exports_async_surface_only() -> None:
    assert "BaseSyncADKStore" not in adk_module.__all__
    assert "BaseSyncADKStore" not in adk_store_module.__all__
    assert not hasattr(adk_module, "BaseSyncADKStore")
    assert not hasattr(adk_store_module, "BaseSyncADKStore")


def test_memory_store_contract_exports_async_surface_only() -> None:
    assert "BaseSyncADKMemoryStore" not in adk_module.__all__
    assert "BaseSyncADKMemoryStore" not in memory_store_module.__all__
    assert not hasattr(adk_module, "BaseSyncADKMemoryStore")
    assert not hasattr(memory_store_module, "BaseSyncADKMemoryStore")


def test_session_store_contract_declares_cleanup_hooks() -> None:
    assert "delete_expired_events" in BaseAsyncADKStore.__abstractmethods__
    assert "delete_idle_sessions" in BaseAsyncADKStore.__abstractmethods__


def test_session_store_contract_declares_schema_parity_hooks() -> None:
    expected_methods = {
        "_get_create_app_states_table_sql",
        "_get_create_user_states_table_sql",
        "_get_create_metadata_table_sql",
        "_get_drop_app_states_table_sql",
        "_get_drop_user_states_table_sql",
        "_get_drop_metadata_table_sql",
        "_get_seed_metadata_sql",
        "get_app_state",
        "get_user_state",
        "upsert_app_state",
        "upsert_user_state",
        "get_metadata",
        "set_metadata",
    }

    assert expected_methods <= BaseAsyncADKStore.__abstractmethods__
    for method_name in expected_methods:
        assert inspect.getdoc(getattr(BaseAsyncADKStore, method_name))


def test_session_store_resolves_schema_parity_table_names() -> None:
    store = _AsyncSessionStore(
        _Config({
            "schema": {
                "app_state_table": "agent_app_states",
                "user_state_table": "agent_user_states",
                "metadata_table": "agent_metadata",
            }
        })
    )

    assert store.app_state_table == "agent_app_states"
    assert store.user_state_table == "agent_user_states"
    assert store.metadata_table == "agent_metadata"


def test_session_store_uses_singular_default_table_names() -> None:
    store = _AsyncSessionStore(_Config())

    assert store.session_table == "adk_session"
    assert store.events_table == "adk_event"
    assert store.app_state_table == "adk_app_state"
    assert store.user_state_table == "adk_user_state"
    assert store.metadata_table == "adk_internal_metadata"


@pytest.mark.anyio
async def test_adk_migration_up_includes_schema_parity_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    migration = __import__("sqlspec.extensions.adk.migrations.0001_create_adk_tables", fromlist=["up"])
    context = type("MigrationContext", (), {"config": _Config()})()

    monkeypatch.setattr(migration, "_get_store_class", lambda _context: _MigrationSessionStore)
    monkeypatch.setattr(migration, "_is_memory_enabled", lambda _context: False)

    statements = await migration.up(context)

    assert statements == [
        "create sessions",
        "create events",
        "create app states",
        "create user states",
        "create metadata",
        "seed metadata",
    ]


@pytest.mark.parametrize("field", ["app_state_table", "user_state_table", "metadata_table"])
def test_session_store_validates_schema_parity_table_names(field: str) -> None:
    with pytest.raises(ValueError, match="Invalid table name"):
        _AsyncSessionStore(_Config({"schema": {field: "invalid-name"}}))


def test_session_store_contract_get_session_accepts_renew_for_kwarg() -> None:
    signature = inspect.signature(BaseAsyncADKStore.get_session)

    parameter = signature.parameters["renew_for"]
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
    assert parameter.default is None


def test_session_store_contract_exposes_concrete_table_lifecycle_methods() -> None:
    assert "drop_tables" not in BaseAsyncADKStore.__abstractmethods__
    assert "recreate_tables" not in BaseAsyncADKStore.__abstractmethods__
    assert inspect.iscoroutinefunction(BaseAsyncADKStore.drop_tables)
    assert inspect.iscoroutinefunction(BaseAsyncADKStore.recreate_tables)


@pytest.mark.parametrize("expires_in", [None, 0, timedelta(seconds=-5)])
def test_async_session_store_calculate_expires_at_returns_none_for_non_positive_values(
    expires_in: int | timedelta | None,
) -> None:
    store = _AsyncSessionStore(_Config())

    assert store._calculate_expires_at(expires_in) is None


@pytest.mark.parametrize("expires_in", [3600, timedelta(hours=1)])
def test_async_session_store_calculate_expires_at_returns_utc_expiration(expires_in: int | timedelta) -> None:
    store = _AsyncSessionStore(_Config())
    before = datetime.now(timezone.utc) + timedelta(seconds=3598)

    expires_at = store._calculate_expires_at(expires_in)

    after = datetime.now(timezone.utc) + timedelta(seconds=3602)
    assert expires_at is not None
    assert expires_at.tzinfo is timezone.utc
    assert before <= expires_at <= after


def test_async_session_store_value_to_bytes_encodes_strings() -> None:
    store = _AsyncSessionStore(_Config())

    assert store._value_to_bytes("abc") == b"abc"


def test_async_session_store_value_to_bytes_returns_existing_bytes() -> None:
    store = _AsyncSessionStore(_Config())
    value = b"abc"

    assert store._value_to_bytes(value) is value


async def test_async_memory_store_logs_ready_with_log_with_context(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    def fake_log_with_context(logger: Any, level: int, event: str, **context: Any) -> None:
        calls.append({"level": level, "event": event, "context": context})

    monkeypatch.setattr(memory_store_module, "log_with_context", fake_log_with_context)
    store = _AsyncMemoryStore(_Config({"memory_table": "test_memories"}))

    await store.ensure_tables()

    assert store.create_tables_called
    assert len(calls) == 1
    assert calls[0]["level"] == logging.DEBUG
    assert calls[0]["event"] == "adk.memory.table.ready"
    assert calls[0]["context"]["memory_table"] == "test_memories"
    assert "db_system" in calls[0]["context"]


async def test_async_memory_store_logs_disabled_with_log_with_context(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    def fake_log_with_context(logger: Any, level: int, event: str, **context: Any) -> None:
        calls.append({"level": level, "event": event, "context": context})

    monkeypatch.setattr(memory_store_module, "log_with_context", fake_log_with_context)
    store = _AsyncMemoryStore(_Config({"enable_memory": False, "memory_table": "test_memories"}))

    await store.ensure_tables()

    assert not store.create_tables_called
    assert len(calls) == 1
    assert calls[0]["level"] == logging.DEBUG
    assert calls[0]["event"] == "adk.memory.table.skipped"
    assert calls[0]["context"]["memory_table"] == "test_memories"
    assert calls[0]["context"]["reason"] == "disabled"
    assert "db_system" in calls[0]["context"]
