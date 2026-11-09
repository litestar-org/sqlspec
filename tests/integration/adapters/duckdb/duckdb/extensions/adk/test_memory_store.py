# pyright: reportPrivateUsage=false
"""Integration tests for DuckDB ADK memory store."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from sqlspec.adapters.duckdb.adk import DuckDBADKMemoryStore
from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.extensions.adk import StoredMemory

pytestmark = [pytest.mark.duckdb, pytest.mark.integration]


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


def _build_store(tmp_path: Path) -> DuckDBADKMemoryStore:
    db_path = tmp_path / "test_adk_memory.duckdb"
    config = DuckDBConfig(connection_config={"database": str(db_path)})
    store = DuckDBADKMemoryStore(config)
    store.create_tables()
    return store


def _build_fts_store(tmp_path: Path) -> DuckDBADKMemoryStore:
    db_path = tmp_path / "test_adk_memory_fts.duckdb"
    config = DuckDBConfig(
        connection_config={"database": str(db_path)}, extension_config={"adk": {"memory_use_fts": True}}
    )
    store = DuckDBADKMemoryStore(config)
    with config.provide_connection() as conn:
        if not store._ensure_fts_extension(conn):  # pyright: ignore[reportPrivateUsage]
            pytest.skip("DuckDB FTS extension is unavailable")
    store.create_tables()
    return store


def test_duckdb_memory_store_insert_search_dedup(tmp_path: Path) -> None:
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


def test_duckdb_memory_store_delete_by_session(tmp_path: Path) -> None:
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


def test_duckdb_memory_store_delete_older_than(tmp_path: Path) -> None:
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


def test_duckdb_memory_store_fts_search_uses_bm25_path(tmp_path: Path) -> None:
    """FTS-enabled DuckDB stores search through the BM25 index after insert refresh."""
    store = _build_fts_store(tmp_path)

    now = datetime.now(UTC)
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

    now = datetime.now(UTC)
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

    now = datetime.now(UTC)
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

    now = datetime.now(UTC)
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


def test_duckdb_memory_companions_restore_dropped_index(tmp_path: Path) -> None:
    """Companion reconciliation restores dropped DuckDB memory indexes while keeping rows."""
    store = _build_store(tmp_path)
    try:
        now = datetime.now(UTC)
        record = _build_record(session_id="s1", event_id="evt-idx-1", content_text="espresso", inserted_at=now)
        store.insert_memory_entries([record])

        with store.config.provide_session() as driver:
            driver.execute("DROP INDEX idx_adk_memory_scope")
            driver.commit()
            store.reconcile_companion_objects_sync(driver)
            driver.commit()
            rows = driver.select("SELECT index_name FROM duckdb_indexes() WHERE table_name = ?", ("adk_memory",))

        assert "idx_adk_memory_scope" in {str(row["index_name"]) for row in rows}
        results = store.search_entries(query="espresso", app_name="app", user_id="user")
        assert [r["event_id"] for r in results] == ["evt-idx-1"]
    finally:
        store.config.close_pool()


def _skip_if_duckdb_fts_unavailable(store: DuckDBADKMemoryStore) -> None:
    """Skip test if the offline environment lacks the DuckDB FTS extension."""
    with store.config.provide_connection() as conn:
        if not store._ensure_fts_extension(conn):
            pytest.skip("DuckDB FTS extension is unavailable")


def _ensure_custom_schema(config: DuckDBConfig, memory_table: str) -> None:
    """Create the parent DuckDB schema when testing schema-qualified memory tables."""
    if "." not in memory_table:
        return
    schema_name = memory_table.split(".", 1)[0]
    with config.provide_session() as driver:
        driver.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        driver.commit()


@pytest.mark.parametrize(
    ("memory_table", "fts_schema"),
    [("adk_memory", "fts_main_adk_memory"), ("custom.my_memories", "fts_custom_my_memories")],
)
def test_duckdb_fts_companion_supports_search_after_empty_setup(
    tmp_path: Path, memory_table: str, fts_schema: str
) -> None:
    """Companion reconciliation creates the FTS schema on empty tables and supports BM25 search."""
    db_path = tmp_path / f"fts_empty_{fts_schema}.duckdb"
    plain_config = DuckDBConfig(
        connection_config={"database": str(db_path)},
        extension_config={"adk": {"memory_table": memory_table, "memory_use_fts": False}},
    )
    fts_config = DuckDBConfig(
        connection_config={"database": str(db_path)},
        extension_config={"adk": {"memory_table": memory_table, "memory_use_fts": True}},
    )
    try:
        _ensure_custom_schema(plain_config, memory_table)
        plain_store = DuckDBADKMemoryStore(plain_config)
        plain_store.create_tables()

        fts_store = DuckDBADKMemoryStore(fts_config)
        _skip_if_duckdb_fts_unavailable(fts_store)

        with fts_config.provide_session() as driver:
            fts_store.reconcile_companion_objects_sync(driver)
            driver.commit()
            rows = driver.select(
                "SELECT 1 AS present FROM information_schema.schemata WHERE lower(schema_name) = lower(?)",
                (fts_schema,),
            )

        assert len(rows) == 1
        now = datetime.now(UTC)
        record = _build_record(session_id="s1", event_id="evt-empty-1", content_text="espresso roast", inserted_at=now)
        fts_store.insert_memory_entries([record])

        results = fts_store.search_entries("espresso", "app", "user")
        assert [r["event_id"] for r in results] == ["evt-empty-1"]
        stemmed = fts_store.search_entries("roasting", "app", "user")
        assert [r["event_id"] for r in stemmed] == ["evt-empty-1"]
    finally:
        plain_config.close_pool()
        fts_config.close_pool()


@pytest.mark.parametrize(
    ("memory_table", "fts_schema"),
    [("adk_memory", "fts_main_adk_memory"), ("custom.my_memories", "fts_custom_my_memories")],
)
def test_duckdb_fts_companion_repeat_keeps_populated_index(tmp_path: Path, memory_table: str, fts_schema: str) -> None:
    """Repeated companion reconciliation keeps existing populated DuckDB FTS indexes intact."""
    db_path = tmp_path / f"fts_repeat_{fts_schema}.duckdb"
    fts_config = DuckDBConfig(
        connection_config={"database": str(db_path)},
        extension_config={"adk": {"memory_table": memory_table, "memory_use_fts": True}},
    )
    try:
        _ensure_custom_schema(fts_config, memory_table)
        fts_store = DuckDBADKMemoryStore(fts_config)
        _skip_if_duckdb_fts_unavailable(fts_store)
        fts_store.create_tables()

        now = datetime.now(UTC)
        record = _build_record(session_id="s1", event_id="evt-rep-1", content_text="espresso roast", inserted_at=now)
        fts_store.insert_memory_entries([record])

        with fts_config.provide_session() as driver:
            fts_store.reconcile_companion_objects_sync(driver)
            fts_store.reconcile_companion_objects_sync(driver)
            driver.commit()

        results = fts_store.search_entries("espresso", "app", "user")
        assert [r["event_id"] for r in results] == ["evt-rep-1"]
        stemmed = fts_store.search_entries("roasting", "app", "user")
        assert [r["event_id"] for r in stemmed] == ["evt-rep-1"]
    finally:
        fts_config.close_pool()


@pytest.mark.parametrize(
    ("memory_table", "fts_schema"),
    [("adk_memory", "fts_main_adk_memory"), ("custom.my_memories", "fts_custom_my_memories")],
)
def test_duckdb_fts_companion_indexes_existing_rows(tmp_path: Path, memory_table: str, fts_schema: str) -> None:
    """Missing DuckDB FTS schema builds once and indexes pre-existing rows."""
    db_path = tmp_path / f"fts_existing_{fts_schema}.duckdb"
    plain_config = DuckDBConfig(
        connection_config={"database": str(db_path)},
        extension_config={"adk": {"memory_table": memory_table, "memory_use_fts": False}},
    )
    fts_config = DuckDBConfig(
        connection_config={"database": str(db_path)},
        extension_config={"adk": {"memory_table": memory_table, "memory_use_fts": True}},
    )
    try:
        _ensure_custom_schema(plain_config, memory_table)
        plain_store = DuckDBADKMemoryStore(plain_config)
        plain_store.create_tables()

        now = datetime.now(UTC)
        record = _build_record(session_id="s1", event_id="evt-pre-1", content_text="espresso roast", inserted_at=now)
        plain_store.insert_memory_entries([record])

        fts_store = DuckDBADKMemoryStore(fts_config)
        _skip_if_duckdb_fts_unavailable(fts_store)

        with fts_config.provide_session() as driver:
            fts_store.reconcile_companion_objects_sync(driver)
            driver.commit()
            rows = driver.select(
                "SELECT 1 AS present FROM information_schema.schemata WHERE lower(schema_name) = lower(?)",
                (fts_schema,),
            )

        assert len(rows) == 1
        results = fts_store.search_entries("espresso", "app", "user")
        assert [r["event_id"] for r in results] == ["evt-pre-1"]
        stemmed = fts_store.search_entries("roasting", "app", "user")
        assert [r["event_id"] for r in stemmed] == ["evt-pre-1"]
    finally:
        plain_config.close_pool()
        fts_config.close_pool()


def test_duckdb_fts_companion_rolls_back_with_runner(tmp_path: Path) -> None:
    """DuckDB FTS schema creation rolls back cleanly with the caller transaction."""
    db_path = tmp_path / "fts_rollback.duckdb"
    plain_config = DuckDBConfig(
        connection_config={"database": str(db_path)}, extension_config={"adk": {"memory_use_fts": False}}
    )
    fts_config = DuckDBConfig(
        connection_config={"database": str(db_path)}, extension_config={"adk": {"memory_use_fts": True}}
    )
    try:
        plain_store = DuckDBADKMemoryStore(plain_config)
        plain_store.create_tables()
        now = datetime.now(UTC)
        record = _build_record(session_id="s1", event_id="evt-rb-1", content_text="espresso roast", inserted_at=now)
        plain_store.insert_memory_entries([record])

        fts_store = DuckDBADKMemoryStore(fts_config)
        _skip_if_duckdb_fts_unavailable(fts_store)

        with fts_config.provide_session() as driver:
            driver.begin()
            fts_store.reconcile_companion_objects_sync(driver)
            in_tx_rows = driver.select(
                "SELECT 1 AS present FROM information_schema.schemata WHERE lower(schema_name) = lower(?)",
                ("fts_main_adk_memory",),
            )
            driver.rollback()
            rows = driver.select(
                "SELECT 1 AS present FROM information_schema.schemata WHERE lower(schema_name) = lower(?)",
                ("fts_main_adk_memory",),
            )

        assert len(in_tx_rows) == 1
        assert rows == []
    finally:
        plain_config.close_pool()
        fts_config.close_pool()
