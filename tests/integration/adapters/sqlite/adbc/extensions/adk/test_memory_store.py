"""Integration tests for ADBC ADK memory store."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.adbc.adk import AdbcADKMemoryStore
from sqlspec.extensions.adk import StoredMemory

pytestmark = [pytest.mark.xdist_group("sqlite"), pytest.mark.adbc, pytest.mark.integration]


def _build_record(
    *, session_id: str, event_id: str, content_text: str, inserted_at: datetime, scope: str = "user"
) -> StoredMemory:
    now = datetime.now(UTC)
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


def _build_store(tmp_path: Path, *, use_fts: bool = False) -> AdbcADKMemoryStore:
    db_path = tmp_path / "test_adk_memory.db"
    config = AdbcConfig(
        connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"},
        extension_config={"adk": {"enable_memory": True, "memory_use_fts": use_fts}},
    )
    store = AdbcADKMemoryStore(config)
    store.create_tables()
    return store


def _sqlite_object_names(config: AdbcConfig) -> set[str]:
    with config.provide_session() as driver:
        rows = driver.select("SELECT name FROM sqlite_master WHERE type IN ('table', 'trigger')")
    return {str(row["name"]) for row in rows}


def _fts_docsize_count(config: AdbcConfig, table_name: str = "adk_memory") -> int:
    with config.provide_session() as driver:
        row = driver.select_one(f"SELECT count(*) AS cnt FROM {table_name}_fts_docsize")
    return int(row["cnt"])


def test_adbc_memory_store_insert_search_dedup(tmp_path: Path) -> None:
    """Insert memory entries, search by text, and skip duplicates."""
    store = _build_store(tmp_path)

    now = datetime.now(UTC)
    record1 = _build_record(session_id="s1", event_id="evt-1", content_text="espresso", inserted_at=now)
    record2 = _build_record(session_id="s1", event_id="evt-2", content_text="latte", inserted_at=now)

    inserted = store.insert_memory_entries([record1, record2])
    assert inserted == 2

    results = store.search_entries(query="espresso", app_name="app", user_id="user")
    assert len(results) == 1
    assert results[0]["event_id"] == "evt-1"

    deduped = store.insert_memory_entries([record1])
    assert deduped == 0


def test_adbc_memory_store_delete_by_session(tmp_path: Path) -> None:
    """Delete memory entries by session id."""
    store = _build_store(tmp_path)

    now = datetime.now(UTC)
    record1 = _build_record(session_id="s1", event_id="evt-1", content_text="espresso", inserted_at=now)
    record2 = _build_record(session_id="s2", event_id="evt-2", content_text="latte", inserted_at=now)
    store.insert_memory_entries([record1, record2])

    deleted = store.delete_entries_by_session("s1")
    assert deleted == 1

    remaining = store.search_entries(query="latte", app_name="app", user_id="user")
    assert len(remaining) == 1
    assert remaining[0]["session_id"] == "s2"


def test_adbc_memory_store_delete_older_than(tmp_path: Path) -> None:
    """Delete memory entries older than a cutoff."""
    store = _build_store(tmp_path)

    now = datetime.now(UTC)
    old = now - timedelta(days=40)
    record1 = _build_record(session_id="s1", event_id="evt-1", content_text="old", inserted_at=old)
    record2 = _build_record(session_id="s1", event_id="evt-2", content_text="new", inserted_at=now)
    store.insert_memory_entries([record1, record2])

    deleted = store.delete_entries_older_than(30)
    assert deleted == 1

    remaining = store.search_entries(query="new", app_name="app", user_id="user")
    assert len(remaining) == 1
    assert remaining[0]["event_id"] == "evt-2"


def test_adbc_sqlite_create_tables_creates_fts_objects(tmp_path: Path) -> None:
    """create_tables creates FTS5 virtual table and sync triggers on fresh SQLite databases."""
    store = _build_store(tmp_path, use_fts=True)
    objects = _sqlite_object_names(store.config)
    assert {"adk_memory_fts", "adk_memory_ai", "adk_memory_ad", "adk_memory_au"}.issubset(objects)
    assert _fts_docsize_count(store.config) == 0

    now = datetime.now(UTC)
    store.insert_memory_entries([
        _build_record(session_id="s1", event_id="evt-1", content_text="espresso shot", inserted_at=now)
    ])
    assert _fts_docsize_count(store.config) == 1

    store.drop_tables()
    after_drop = _sqlite_object_names(store.config)
    assert {"adk_memory", "adk_memory_fts", "adk_memory_ai", "adk_memory_ad", "adk_memory_au"}.isdisjoint(after_drop)


def test_adbc_sqlite_create_tables_rebuilds_existing_rows(tmp_path: Path) -> None:
    """Enabling FTS on an existing populated table creates FTS5 objects and rebuilds the index."""
    base_store = _build_store(tmp_path, use_fts=False)
    now = datetime.now(UTC)
    base_store.insert_memory_entries([
        _build_record(session_id="s1", event_id="evt-1", content_text="roasted beans", inserted_at=now)
    ])

    fts_store = _build_store(tmp_path, use_fts=True)
    assert _fts_docsize_count(fts_store.config) == 1


def test_adbc_sqlite_create_tables_repeat_keeps_index(tmp_path: Path) -> None:
    """Repeated create_tables calls preserve FTS5 docsize count without duplicating entries."""
    store = _build_store(tmp_path, use_fts=True)
    now = datetime.now(UTC)
    store.insert_memory_entries([
        _build_record(session_id="s1", event_id="evt-1", content_text="cold brew", inserted_at=now)
    ])

    store.create_tables()
    store.create_tables()
    assert _fts_docsize_count(store.config) == 1


def test_adbc_sqlite_create_tables_repairs_missing_trigger(tmp_path: Path) -> None:
    """Dropping one FTS5 trigger and re-running create_tables restores the trigger and rebuilds."""
    store = _build_store(tmp_path, use_fts=True)
    now = datetime.now(UTC)
    store.insert_memory_entries([
        _build_record(session_id="s1", event_id="evt-1", content_text="flat white", inserted_at=now)
    ])

    with store.config.provide_session() as driver:
        driver.execute("DROP TRIGGER adk_memory_au")
        driver.commit()

    store.create_tables()
    assert "adk_memory_au" in _sqlite_object_names(store.config)
    assert _fts_docsize_count(store.config) == 1


def test_adbc_sqlite_versioned_statements_execute(tmp_path: Path) -> None:
    """Each DDL entry emitted by _memory_table_ddl executes cleanly via driver.execute_script."""
    db_path = tmp_path / "versioned_adk_memory.db"
    config = AdbcConfig(
        connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"},
        extension_config={"adk": {"enable_memory": True, "memory_use_fts": True}},
    )
    store = AdbcADKMemoryStore(config)
    statements = getattr(store, "_memory_table_ddl")()
    assert isinstance(statements, list)

    with config.provide_session() as driver:
        for stmt in statements:
            driver.execute_script(stmt)
        driver.commit()

    objects = _sqlite_object_names(config)
    assert {"adk_memory", "adk_memory_fts", "adk_memory_ai", "adk_memory_ad", "adk_memory_au"}.issubset(objects)
