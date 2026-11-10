"""Integration tests for ADBC ADK memory store."""

import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.adbc.adk import AdbcADKMemoryStore, AdbcADKStore
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


def _sqlite_index_names(config: AdbcConfig) -> set[str]:
    with config.provide_session() as driver:
        rows = driver.select("SELECT name FROM sqlite_master WHERE type = 'index'")
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


def test_adbc_sqlite_fts_search_matches_tokens(tmp_path: Path) -> None:
    """FTS5 MATCH matches full tokens and does not match mid-word substrings."""
    store = _build_store(tmp_path, use_fts=True)
    now = datetime.now(UTC)
    store.insert_memory_entries([
        _build_record(session_id="s1", event_id="evt-1", content_text="espresso roast", inserted_at=now)
    ])

    exact_results = store.search_entries(query="espresso", app_name="app", user_id="user")
    assert len(exact_results) == 1
    assert exact_results[0]["event_id"] == "evt-1"

    partial_results = store.search_entries(query="press", app_name="app", user_id="user")
    assert partial_results == []


def test_adbc_sqlite_fts_search_tracks_update_and_delete(tmp_path: Path) -> None:
    """FTS5 sync triggers update search results across UPDATE and DELETE mutations."""
    store = _build_store(tmp_path, use_fts=True)
    now = datetime.now(UTC)
    store.insert_memory_entries([
        _build_record(session_id="s1", event_id="evt-1", content_text="espresso roast", inserted_at=now)
    ])

    with store.config.provide_session() as driver:
        driver.execute("UPDATE adk_memory SET content_text = 'latte foam' WHERE event_id = 'evt-1'")
        driver.commit()

    assert store.search_entries(query="espresso", app_name="app", user_id="user") == []
    updated = store.search_entries(query="latte", app_name="app", user_id="user")
    assert len(updated) == 1
    assert updated[0]["content_text"] == "latte foam"

    store.delete_entries_by_session("s1")
    assert store.search_entries(query="latte", app_name="app", user_id="user") == []


def test_adbc_sqlite_fts_search_honors_scope_filter(tmp_path: Path) -> None:
    """FTS5 search filters results by scope_filter ('app', 'user', 'all')."""
    store = _build_store(tmp_path, use_fts=True)
    now = datetime.now(UTC)
    store.insert_memory_entries([
        _build_record(session_id="s1", event_id="evt-user", content_text="cappuccino", inserted_at=now, scope="user"),
        _build_record(session_id="s2", event_id="evt-app", content_text="cappuccino", inserted_at=now, scope="app"),
    ])

    app_only = store.search_entries(query="cappuccino", app_name="app", user_id="user", scope_filter="app")
    assert [row["event_id"] for row in app_only] == ["evt-app"]

    user_only = store.search_entries(query="cappuccino", app_name="app", user_id="user", scope_filter="user")
    assert [row["event_id"] for row in user_only] == ["evt-user"]


def test_adbc_sqlite_fts_search_finds_rebuilt_rows(tmp_path: Path) -> None:
    """Rows inserted before FTS5 was enabled are returned by FTS5 search after create_tables rebuild."""
    base_store = _build_store(tmp_path, use_fts=False)
    now = datetime.now(UTC)
    base_store.insert_memory_entries([
        _build_record(session_id="s1", event_id="evt-1", content_text="espresso crema", inserted_at=now)
    ])

    fts_store = _build_store(tmp_path, use_fts=True)
    results = fts_store.search_entries(query="espresso", app_name="app", user_id="user")
    assert len(results) == 1
    assert results[0]["event_id"] == "evt-1"


def test_adbc_sqlite_fts_search_recovers_invalid_query(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Invalid FTS5 MATCH syntax logs a warning and falls back to simple LIKE search."""
    store = _build_store(tmp_path, use_fts=True)
    now = datetime.now(UTC)
    store.insert_memory_entries([
        _build_record(session_id="s1", event_id="evt-1", content_text="espresso", inserted_at=now)
    ])

    with caplog.at_level(logging.WARNING, logger="sqlspec.adapters.adbc.adk.store"):
        results = store.search_entries("foo AND", "app", "user")

    assert results == []
    assert any("FTS search failed" in rec.message for rec in caplog.records)


def test_adbc_sqlite_fts_search_recovers_missing_index(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Missing FTS5 virtual table logs a warning and returns simple LIKE search results."""
    store = _build_store(tmp_path, use_fts=True)
    now = datetime.now(UTC)
    store.insert_memory_entries([
        _build_record(session_id="s1", event_id="evt-1", content_text="espresso", inserted_at=now)
    ])

    with store.config.provide_session() as driver:
        driver.execute("DROP TABLE adk_memory_fts")
        driver.commit()

    with caplog.at_level(logging.WARNING, logger="sqlspec.adapters.adbc.adk.store"):
        results = store.search_entries("espresso", "app", "user")

    assert len(results) == 1
    assert results[0]["event_id"] == "evt-1"
    assert any("FTS search failed" in rec.message for rec in caplog.records)


def test_adbc_sqlite_fts_search_missing_memory_table(tmp_path: Path) -> None:
    """Searching when the base memory table does not exist returns an empty list."""
    db_path = tmp_path / "uninitialized_adk_memory.db"
    config = AdbcConfig(
        connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"},
        extension_config={"adk": {"enable_memory": True, "memory_use_fts": True}},
    )
    store = AdbcADKMemoryStore(config)
    assert store.search_entries("espresso", "app", "user") == []


def test_adbc_sqlite_companions_restore_dropped_indexes(tmp_path: Path) -> None:
    """Companion reconciliation restores dropped memory indexes idempotently without losing rows."""
    store = _build_store(tmp_path)
    now = datetime.now(UTC)
    store.insert_memory_entries([
        _build_record(session_id="s1", event_id="evt-1", content_text="espresso", inserted_at=now)
    ])

    with store.config.provide_session() as driver:
        driver.execute("DROP INDEX idx_adk_memory_scope")
        driver.commit()
    assert "idx_adk_memory_scope" not in _sqlite_index_names(store.config)

    with store.config.provide_session() as driver:
        store.reconcile_companion_objects_sync(driver)
        store.reconcile_companion_objects_sync(driver)
        driver.commit()

    assert "idx_adk_memory_scope" in _sqlite_index_names(store.config)
    results = store.search_entries("espresso", "app", "user")
    assert len(results) == 1
    assert results[0]["event_id"] == "evt-1"


def test_adbc_sqlite_session_companions_restore_dropped_index(tmp_path: Path) -> None:
    """Session store companion reconciliation restores a dropped event index while preserving session rows."""
    db_path = tmp_path / "test_adk_session.db"
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"})
    store = AdbcADKStore(config)
    store.create_tables()
    store.create_session("sess-1", "app", "user", {"step": 1})

    with config.provide_session() as driver:
        driver.execute("DROP INDEX idx_adk_event_session")
        driver.commit()
    assert "idx_adk_event_session" not in _sqlite_index_names(config)

    with config.provide_session() as driver:
        store.reconcile_companion_objects_sync(driver)
        driver.commit()

    assert "idx_adk_event_session" in _sqlite_index_names(config)
    restored = store.get_session("app", "user", "sess-1")
    assert restored is not None
    assert restored["state"] == {"step": 1}


def test_adbc_sqlite_fts_companions_create_for_empty_source(tmp_path: Path) -> None:
    """Companion reconciliation creates FTS5 objects on an empty memory table so subsequent inserts index."""
    _build_store(tmp_path, use_fts=False)
    db_path = tmp_path / "test_adk_memory.db"
    fts_config = AdbcConfig(
        connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"},
        extension_config={"adk": {"enable_memory": True, "memory_use_fts": True}},
    )
    fts_store = AdbcADKMemoryStore(fts_config)

    with fts_config.provide_session() as driver:
        fts_store.reconcile_companion_objects_sync(driver)
        driver.commit()

    now = datetime.now(UTC)
    fts_store.insert_memory_entries([
        _build_record(session_id="s1", event_id="evt-1", content_text="espresso", inserted_at=now)
    ])

    objects = _sqlite_object_names(fts_config)
    assert {"adk_memory_fts", "adk_memory_ai", "adk_memory_ad", "adk_memory_au"}.issubset(objects)
    assert _fts_docsize_count(fts_config) == 1


def test_adbc_sqlite_fts_companions_rebuild_populated_source(tmp_path: Path) -> None:
    """Companion reconciliation creates missing FTS5 objects and ordinary indexes and rebuilds existing rows."""
    base_store = _build_store(tmp_path, use_fts=False)
    now = datetime.now(UTC)
    base_store.insert_memory_entries([
        _build_record(session_id="s1", event_id="evt-1", content_text="espresso", inserted_at=now)
    ])
    with base_store.config.provide_session() as driver:
        driver.execute("DROP INDEX idx_adk_memory_scope")
        driver.commit()

    db_path = tmp_path / "test_adk_memory.db"
    fts_config = AdbcConfig(
        connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"},
        extension_config={"adk": {"enable_memory": True, "memory_use_fts": True}},
    )
    fts_store = AdbcADKMemoryStore(fts_config)

    with fts_config.provide_session() as driver:
        fts_store.reconcile_companion_objects_sync(driver)
        driver.commit()

    assert "adk_memory_fts" in _sqlite_object_names(fts_config)
    assert "idx_adk_memory_scope" in _sqlite_index_names(fts_config)
    assert _fts_docsize_count(fts_config) == 1
    results = fts_store.search_entries("espresso", "app", "user")
    assert len(results) == 1
    assert results[0]["event_id"] == "evt-1"


def test_adbc_sqlite_fts_companions_repeat_keeps_entries(tmp_path: Path) -> None:
    """Repeated companion reconciliation over complete FTS5 objects preserves docsize count."""
    store = _build_store(tmp_path, use_fts=True)
    now = datetime.now(UTC)
    store.insert_memory_entries([
        _build_record(session_id="s1", event_id="evt-1", content_text="espresso", inserted_at=now)
    ])

    with store.config.provide_session() as driver:
        store.reconcile_companion_objects_sync(driver)
        store.reconcile_companion_objects_sync(driver)
        driver.commit()

    assert _fts_docsize_count(store.config) == 1


def test_adbc_sqlite_fts_rebuild_rolls_back(tmp_path: Path) -> None:
    """Rolling back a transaction after companion FTS5 creation and rebuild leaves adk_memory_fts absent."""
    base_store = _build_store(tmp_path, use_fts=False)
    now = datetime.now(UTC)
    base_store.insert_memory_entries([
        _build_record(session_id="s1", event_id="evt-1", content_text="espresso", inserted_at=now)
    ])

    db_path = tmp_path / "test_adk_memory.db"
    fts_config = AdbcConfig(
        connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"},
        extension_config={"adk": {"enable_memory": True, "memory_use_fts": True}},
    )
    fts_store = AdbcADKMemoryStore(fts_config)

    with fts_config.provide_session() as driver:
        driver.begin()
        fts_store.reconcile_companion_objects_sync(driver)
        driver.rollback()

    assert "adk_memory_fts" not in _sqlite_object_names(fts_config)
