# pyright: reportPrivateUsage=false
"""Integration tests for DuckDB ADK memory store."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from sqlspec.adapters.duckdb.adk import DuckdbADKMemoryStore
from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.extensions.adk import StoredMemory

pytestmark = [pytest.mark.duckdb, pytest.mark.integration]


def _build_record(
    *, session_id: str, event_id: str, content_text: str, inserted_at: datetime, scope: str = "user"
) -> StoredMemory:
    now = datetime.now(timezone.utc)
    return StoredMemory(
        id=str(uuid4()),
        session_id=session_id,
        app_name="app",
        user_id="user",
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


def _build_store(tmp_path: Path) -> DuckdbADKMemoryStore:
    db_path = tmp_path / "test_adk_memory.duckdb"
    config = DuckDBConfig(connection_config={"database": str(db_path)})
    store = DuckdbADKMemoryStore(config)
    store.create_tables()
    return store


def _build_fts_store(tmp_path: Path) -> DuckdbADKMemoryStore:
    db_path = tmp_path / "test_adk_memory_fts.duckdb"
    config = DuckDBConfig(
        connection_config={"database": str(db_path)}, extension_config={"adk": {"memory_use_fts": True}}
    )
    store = DuckdbADKMemoryStore(config)
    with config.provide_connection() as conn:
        if not store._ensure_fts_extension(conn):  # pyright: ignore[reportPrivateUsage]
            pytest.skip("DuckDB FTS extension is unavailable")
    store.create_tables()
    return store


def test_duckdb_memory_store_insert_search_dedup(tmp_path: Path) -> None:
    """Insert memory entries, search by text, and skip duplicates."""
    store = _build_store(tmp_path)

    now = datetime.now(timezone.utc)
    record1 = _build_record(session_id="s1", event_id="evt-1", content_text="espresso", inserted_at=now)
    record2 = _build_record(session_id="s1", event_id="evt-2", content_text="latte", inserted_at=now)

    inserted = store.insert_memory_entries([record1, record2])
    assert inserted == 2

    results = store.search_entries(query="espresso", app_name="app", user_id="user")
    assert len(results) == 1
    assert results[0]["event_id"] == "evt-1"

    deduped = store.insert_memory_entries([record1])
    assert deduped == 0


def test_duckdb_memory_store_delete_by_session(tmp_path: Path) -> None:
    """Delete memory entries by session id."""
    store = _build_store(tmp_path)

    now = datetime.now(timezone.utc)
    record1 = _build_record(session_id="s1", event_id="evt-1", content_text="espresso", inserted_at=now)
    record2 = _build_record(session_id="s2", event_id="evt-2", content_text="latte", inserted_at=now)
    store.insert_memory_entries([record1, record2])

    deleted = store.delete_entries_by_session("s1")
    assert deleted == 1

    remaining = store.search_entries(query="latte", app_name="app", user_id="user")
    assert len(remaining) == 1
    assert remaining[0]["session_id"] == "s2"


def test_duckdb_memory_store_delete_older_than(tmp_path: Path) -> None:
    """Delete memory entries older than a cutoff."""
    store = _build_store(tmp_path)

    now = datetime.now(timezone.utc)
    old = now - timedelta(days=40)
    record1 = _build_record(session_id="s1", event_id="evt-1", content_text="old", inserted_at=old)
    record2 = _build_record(session_id="s1", event_id="evt-2", content_text="new", inserted_at=now)
    store.insert_memory_entries([record1, record2])

    deleted = store.delete_entries_older_than(30)
    assert deleted == 1

    remaining = store.search_entries(query="new", app_name="app", user_id="user")
    assert len(remaining) == 1
    assert remaining[0]["event_id"] == "evt-2"


def test_duckdb_memory_store_fts_search_uses_bm25_path(tmp_path: Path) -> None:
    """FTS-enabled DuckDB stores search through the BM25 index after insert refresh."""
    store = _build_fts_store(tmp_path)

    now = datetime.now(timezone.utc)
    record1 = _build_record(session_id="s1", event_id="evt-fts-1", content_text="espresso roast", inserted_at=now)
    record2 = _build_record(session_id="s1", event_id="evt-fts-2", content_text="latte foam", inserted_at=now)
    store.insert_memory_entries([record1, record2])

    results = store.search_entries(query="espresso", app_name="app", user_id="user")

    assert len(results) == 1
    assert results[0]["event_id"] == "evt-fts-1"
    assert results[0]["content_json"] == {"text": "espresso roast"}


def test_duckdb_memory_store_scoped_search_combined_default(tmp_path: Path) -> None:
    """Default search recall returns both user-scoped and app-scoped memories."""
    store = _build_store(tmp_path)

    now = datetime.now(timezone.utc)
    record_user = _build_record(
        session_id="s1", event_id="evt-u1", content_text="project architecture guideline", inserted_at=now, scope="user"
    )
    record_app = _build_record(
        session_id="s2", event_id="evt-a1", content_text="company architecture standard", inserted_at=now, scope="app"
    )
    record_other_user = _build_record(
        session_id="s3",
        event_id="evt-other",
        content_text="other user architecture note",
        inserted_at=now,
        scope="user",
    )
    record_other_user["user_id"] = "other_user"

    store.insert_memory_entries([record_user, record_app, record_other_user])

    results = store.search_entries(query="architecture", app_name="app", user_id="user")
    event_ids = {r["event_id"] for r in results}
    assert event_ids == {"evt-u1", "evt-a1"}
    assert "evt-other" not in event_ids


def test_duckdb_memory_store_explicit_scope_filters(tmp_path: Path) -> None:
    """Explicit scope filters restrict results to only user or only app memories."""
    store = _build_store(tmp_path)

    now = datetime.now(timezone.utc)
    record_user = _build_record(
        session_id="s1", event_id="evt-u1", content_text="scoped query plan", inserted_at=now, scope="user"
    )
    record_app = _build_record(
        session_id="s2", event_id="evt-a1", content_text="scoped release plan", inserted_at=now, scope="app"
    )
    store.insert_memory_entries([record_user, record_app])

    user_only = store.search_entries(query="scoped", app_name="app", user_id="user", scope_filter="user")
    assert len(user_only) == 1
    assert user_only[0]["event_id"] == "evt-u1"

    app_only = store.search_entries(query="scoped", app_name="app", user_id="user", scope_filter="app")
    assert len(app_only) == 1
    assert app_only[0]["event_id"] == "evt-a1"


def test_duckdb_memory_store_scoped_retention(tmp_path: Path) -> None:
    """Scoped retention deletes entries matching app_name and scope filters."""
    store = _build_store(tmp_path)

    now = datetime.now(timezone.utc)
    old = now - timedelta(days=40)
    record_user_old = _build_record(
        session_id="s1", event_id="evt-uo", content_text="old user memo", inserted_at=old, scope="user"
    )
    record_app_old = _build_record(
        session_id="s2", event_id="evt-ao", content_text="old app guideline", inserted_at=old, scope="app"
    )
    store.insert_memory_entries([record_user_old, record_app_old])

    deleted = store.delete_entries_older_than(30, app_name="app", scope="user")
    assert deleted == 1

    remaining = store.search_entries(query="old", app_name="app", user_id="user")
    assert len(remaining) == 1
    assert remaining[0]["event_id"] == "evt-ao"
