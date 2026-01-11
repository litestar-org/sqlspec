"""Integration tests for DuckDB ADK memory store."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from sqlspec.adapters.duckdb.adk.store import DuckdbADKMemoryStore
from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.extensions.adk import MemoryRecord

pytestmark = [pytest.mark.duckdb, pytest.mark.integration]


def _build_record(*, session_id: str, event_id: str, content_text: str, inserted_at: datetime) -> MemoryRecord:
    now = datetime.now(timezone.utc)
    return MemoryRecord(
        id=str(uuid4()),
        session_id=session_id,
        app_name="app",
        user_id="user",
        event_id=event_id,
        author="user",
        timestamp=now,
        content_json={"text": content_text},
        content_text=content_text,
        metadata_json=None,
        inserted_at=inserted_at,
    )


def _build_store(tmp_path: Path, worker_id: str) -> DuckdbADKMemoryStore:
    db_path = tmp_path / f"test_adk_memory_{worker_id}.duckdb"
    config = DuckDBConfig(connection_config={"database": str(db_path)})
    store = DuckdbADKMemoryStore(config)
    store.create_tables()
    return store


def test_duckdb_memory_store_insert_search_dedup(tmp_path: Path, worker_id: str) -> None:
    """Insert memory entries, search by text, and skip duplicates."""
    store = _build_store(tmp_path, worker_id)

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


def test_duckdb_memory_store_delete_by_session(tmp_path: Path, worker_id: str) -> None:
    """Delete memory entries by session id."""
    store = _build_store(tmp_path, worker_id)

    now = datetime.now(timezone.utc)
    record1 = _build_record(session_id="s1", event_id="evt-1", content_text="espresso", inserted_at=now)
    record2 = _build_record(session_id="s2", event_id="evt-2", content_text="latte", inserted_at=now)
    store.insert_memory_entries([record1, record2])

    deleted = store.delete_entries_by_session("s1")
    assert deleted == 1

    remaining = store.search_entries(query="latte", app_name="app", user_id="user")
    assert len(remaining) == 1
    assert remaining[0]["session_id"] == "s2"


def test_duckdb_memory_store_delete_older_than(tmp_path: Path, worker_id: str) -> None:
    """Delete memory entries older than a cutoff."""
    store = _build_store(tmp_path, worker_id)

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
