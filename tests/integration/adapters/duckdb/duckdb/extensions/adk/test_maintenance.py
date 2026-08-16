"""Integration tests for ADK maintenance and pruning on DuckDB."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from sqlspec.adapters.duckdb.adk import DuckdbADKMemoryStore, DuckdbADKStore
from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.extensions.adk import StoredEvent, StoredMemory
from sqlspec.extensions.adk.maintenance import (
    maintain_tables_sync,
    prune_events_sync,
    prune_memory_sync,
    prune_sessions_sync,
    prune_user_state_sync,
)

pytestmark = [pytest.mark.duckdb, pytest.mark.integration]


def _config(tmp_path: Path, name: str) -> DuckDBConfig:
    return DuckDBConfig(connection_config={"database": str(tmp_path / f"{name}.duckdb")})


def _memory_record(*, app_name: str, inserted_at: datetime, scope: str = "user") -> StoredMemory:
    now = datetime.now(timezone.utc)
    return StoredMemory(
        id=str(uuid4()),
        session_id="s1",
        app_name=app_name,
        user_id="user",
        scope=scope,
        event_id=str(uuid4()),
        author="user",
        timestamp=now,
        content_json={"text": "note"},
        content_text="note",
        metadata_json=None,
        inserted_at=inserted_at,
        embedding=None,
    )


def _event(*, app_name: str, session_id: str, timestamp: datetime) -> StoredEvent:
    return StoredEvent(
        id=str(uuid4()),
        app_name=app_name,
        user_id="user",
        session_id=session_id,
        invocation_id="inv-1",
        timestamp=timestamp,
        event_data={"text": "hello"},
    )


def test_duckdb_prune_sessions_scopes_to_app_name(tmp_path: Path) -> None:
    """Pruning sessions for one application leaves other applications untouched."""
    config = _config(tmp_path, "prune_sessions")
    store = DuckdbADKStore(config)
    store.create_tables()

    stale = datetime.now(timezone.utc) - timedelta(days=120)
    for app in ("app_keep", "app_prune"):
        store.create_session(f"session_{app}", app, "user", {})
    with config.provide_connection() as conn:
        conn.execute(f"UPDATE {store.session_table} SET update_time = ?", (stale,))
        conn.commit()

    report = prune_sessions_sync(store, idle_days=30, app_name="app_prune")

    assert report["deleted_count"] == 1
    assert store.get_session("app_prune", "user", "session_app_prune") is None
    assert store.get_session("app_keep", "user", "session_app_keep") is not None


def test_duckdb_prune_events_scopes_to_app_name(tmp_path: Path) -> None:
    """Pruning events for one application leaves other applications' events intact."""
    config = _config(tmp_path, "prune_events")
    store = DuckdbADKStore(config)
    store.create_tables()

    stale = datetime.now(timezone.utc) - timedelta(days=200)
    for app in ("app_keep", "app_prune"):
        store.create_session(f"session_{app}", app, "user", {})
        store.append_event(_event(app_name=app, session_id=f"session_{app}", timestamp=stale))

    report = prune_events_sync(store, older_than_days=90, app_name="app_prune")

    assert report["deleted_count"] == 1
    assert store.get_events("app_prune", "user", "session_app_prune") == []
    assert len(store.get_events("app_keep", "user", "session_app_keep")) == 1


def test_duckdb_prune_user_state_scopes_to_app_name(tmp_path: Path) -> None:
    """User state pruning honors both the idle threshold and the application filter."""
    config = _config(tmp_path, "prune_user_state")
    store = DuckdbADKStore(config)
    store.create_tables()

    for app in ("app_keep", "app_prune"):
        store.upsert_user_state(app, "user", {"theme": "dark"})
    stale = datetime.now(timezone.utc) - timedelta(days=400)
    with config.provide_connection() as conn:
        conn.execute(f"UPDATE {store.user_state_table} SET update_time = ?", (stale,))
        conn.commit()

    report = prune_user_state_sync(store, idle_days=180, app_name="app_prune")

    assert report["deleted_count"] == 1
    assert store.get_user_state("app_prune", "user") is None
    assert store.get_user_state("app_keep", "user") == {"theme": "dark"}


def test_duckdb_prune_memory_respects_scope(tmp_path: Path) -> None:
    """Memory pruning removes user-scoped entries while preserving app-scoped ones."""
    config = _config(tmp_path, "prune_memory")
    store = DuckdbADKMemoryStore(config)
    store.create_tables()

    stale = datetime.now(timezone.utc) - timedelta(days=200)
    store.insert_memory_entries(
        [
            _memory_record(app_name="app", inserted_at=stale, scope="user"),
            _memory_record(app_name="app", inserted_at=stale, scope="app"),
        ]
    )

    report = prune_memory_sync(store, older_than_days=90, app_name="app", scope="user")

    assert report["deleted_count"] == 1
    remaining = store.search_entries(query="note", app_name="app", user_id="user", scope_filter="all")
    assert len(remaining) == 1
    assert remaining[0]["scope"] == "app"


def test_duckdb_maintain_tables_executes_dialect_branch(tmp_path: Path) -> None:
    """The DuckDB maintenance branch runs against a real database without error."""
    config = _config(tmp_path, "maintain")
    store = DuckdbADKStore(config)
    store.create_tables()

    report = maintain_tables_sync(config)

    assert report["total_elapsed_ms"] >= 0.0
    assert report["operations"]
