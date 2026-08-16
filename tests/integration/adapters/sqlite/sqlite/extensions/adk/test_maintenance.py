"""Integration tests for ADK retention on SQLite."""

import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.adk import SqliteADKMemoryStore, SqliteADKStore
from sqlspec.adapters.sqlite.adk.store import _datetime_to_julian
from sqlspec.extensions.adk import StoredEvent, StoredMemory
from sqlspec.extensions.adk.maintenance import prune_events_sync, prune_memory_sync, prune_sessions_sync

pytestmark = pytest.mark.xdist_group("sqlite")


def _build_memory_record(
    *, session_id: str, event_id: str, content_text: str, inserted_at: datetime, scope: str = "user"
) -> StoredMemory:
    now = datetime.now(timezone.utc)
    return StoredMemory(
        id=str(uuid4()),
        session_id=session_id,
        app_name="agent_app",
        user_id="user_1",
        scope=scope,
        event_id=event_id,
        author="user",
        timestamp=now,
        content_json={"text": content_text},
        content_text=content_text,
        metadata_json=None,
        inserted_at=inserted_at,
        embedding=None,
    )


def test_sqlite_prune_sessions_and_scoped_memory() -> None:
    """Prune stale sessions and user-scoped memories while preserving app-scoped ones."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = SqliteConfig(connection_config={"database": tmp.name})
        session_store = SqliteADKStore(config)
        memory_store = SqliteADKMemoryStore(config)
        session_store.create_tables()
        memory_store.create_tables()

        now = datetime.now(timezone.utc)
        old_time = now - timedelta(days=60)

        with config.provide_connection() as conn:
            conn.execute(
                "INSERT INTO adk_session (id, app_name, user_id, state, create_time, update_time) VALUES (?, ?, ?, ?, julianday(?), julianday(?))",
                ("s-old", "agent_app", "user_1", '{"k": "v"}', old_time.isoformat(), old_time.isoformat()),
            )
            conn.execute(
                "INSERT INTO adk_session (id, app_name, user_id, state, create_time, update_time) VALUES (?, ?, ?, ?, julianday(?), julianday(?))",
                ("s-new", "agent_app", "user_1", '{"k": "v"}', now.isoformat(), now.isoformat()),
            )
            conn.commit()

        prune_sess_report = prune_sessions_sync(session_store, idle_days=30)
        assert prune_sess_report["deleted_count"] == 1
        assert prune_sess_report["table"] == "adk_session"

        m_old_user = _build_memory_record(
            session_id="s-old",
            event_id="evt-old-user",
            content_text="old user memory",
            inserted_at=old_time,
            scope="user",
        )
        m_old_app = _build_memory_record(
            session_id="s-old", event_id="evt-old-app", content_text="old app memory", inserted_at=old_time, scope="app"
        )
        m_new_user = _build_memory_record(
            session_id="s-new", event_id="evt-new-user", content_text="new user memory", inserted_at=now, scope="user"
        )
        memory_store.insert_memory_entries([m_old_user, m_old_app, m_new_user])

        prune_mem_report = prune_memory_sync(memory_store, older_than_days=30, scope="user")
        assert prune_mem_report["deleted_count"] == 1

        remaining_mem = memory_store.search_entries(
            query="memory", app_name="agent_app", user_id="user_1", scope_filter="all"
        )
        assert len(remaining_mem) == 2
        remaining_ids = {m["event_id"] for m in remaining_mem}
        assert "evt-old-app" in remaining_ids
        assert "evt-new-user" in remaining_ids


def test_sqlite_prune_sessions_scopes_to_app_name() -> None:
    """Pruning with app_name must leave other applications' sessions untouched."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name
    config = SqliteConfig(connection_config={"database": db_path})
    try:
        store = SqliteADKStore(config)
        store.create_tables()

        stale = datetime.now(timezone.utc) - timedelta(days=90)
        for app in ("app_keep", "app_prune"):
            store.create_session(f"session_{app}", app, "user_1", {})
        with config.provide_connection() as conn:
            conn.execute(f"UPDATE {store.session_table} SET update_time = ?", (_datetime_to_julian(stale),))
            conn.commit()

        report = prune_sessions_sync(store, idle_days=30, app_name="app_prune")

        assert report["deleted_count"] == 1
        assert store.get_session("app_prune", "user_1", "session_app_prune") is None
        assert store.get_session("app_keep", "user_1", "session_app_keep") is not None
    finally:
        config.close_pool()
        Path(db_path).unlink(missing_ok=True)


def test_sqlite_prune_events_scopes_to_app_name() -> None:
    """Pruning events with app_name must leave other applications' events untouched."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name
    config = SqliteConfig(connection_config={"database": db_path})
    try:
        store = SqliteADKStore(config)
        store.create_tables()

        stale = datetime.now(timezone.utc) - timedelta(days=200)
        for app in ("app_keep", "app_prune"):
            store.create_session(f"session_{app}", app, "user_1", {})
            store.append_event(
                StoredEvent(
                    id=f"event_{app}",
                    app_name=app,
                    user_id="user_1",
                    session_id=f"session_{app}",
                    invocation_id="inv_1",
                    timestamp=stale,
                    event_data={"text": "hello"},
                )
            )

        report = prune_events_sync(store, older_than_days=90, app_name="app_prune")

        assert report["deleted_count"] == 1
        assert store.get_events("app_prune", "user_1", "session_app_prune") == []
        assert len(store.get_events("app_keep", "user_1", "session_app_keep")) == 1
    finally:
        config.close_pool()
        Path(db_path).unlink(missing_ok=True)
