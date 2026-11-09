"""Integration tests for SQLite ADK memory store."""

import tempfile
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite._typing import sqlite_module as sqlite3
from sqlspec.utils.module_loader import import_optional_attr

if TYPE_CHECKING:
    from sqlspec.adapters.sqlite.adk import SqliteADKMemoryStore, SqliteADKStore
    from sqlspec.extensions.adk import StoredMemory
else:
    SqliteADKMemoryStore = import_optional_attr("sqlspec.adapters.sqlite.adk", "SqliteADKMemoryStore")
    SqliteADKStore = import_optional_attr("sqlspec.adapters.sqlite.adk", "SqliteADKStore")
    StoredMemory = import_optional_attr("sqlspec.extensions.adk.memory._types", "StoredMemory")

pytest.importorskip("google.adk")

pytestmark = pytest.mark.xdist_group("sqlite")


def test_sqlite_memory_store_insert_search_dedup() -> None:
    """Insert memory entries, search by text, and skip duplicates."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = SqliteConfig(connection_config={"database": tmp.name})
        store = SqliteADKMemoryStore(config)
        store.create_tables()

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


def test_sqlite_memory_store_fts_search() -> None:
    """FTS-enabled memory stores search through the FTS5 virtual table."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = SqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"adk": {"memory_use_fts": True}}
        )
        store = SqliteADKMemoryStore(config)
        store.create_tables()

        now = datetime.now(UTC)
        record1 = _build_record(session_id="s1", event_id="evt-fts-1", content_text="espresso roast", inserted_at=now)
        record2 = _build_record(session_id="s1", event_id="evt-fts-2", content_text="latte foam", inserted_at=now)
        store.insert_memory_entries([record1, record2])

        results = store.search_entries(query="espresso", app_name="app", user_id="user")

        assert len(results) == 1
        assert results[0]["event_id"] == "evt-fts-1"


def test_sqlite_memory_store_disabled_lifecycle() -> None:
    """Disabled memory stores skip table creation and reject memory operations."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = SqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"adk": {"enable_memory": False}}
        )
        store = SqliteADKMemoryStore(config)
        store.create_tables()

        with config.provide_connection() as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", ("adk_memory_entries",)
            )
            row = cursor.fetchone()

        assert row is None

        now = datetime.now(UTC)
        record = _build_record(session_id="s1", event_id="evt-disabled", content_text="espresso", inserted_at=now)
        with pytest.raises(RuntimeError, match="Memory store is disabled"):
            store.insert_memory_entries([record])
        with pytest.raises(RuntimeError, match="Memory store is disabled"):
            store.search_entries(query="espresso", app_name="app", user_id="user")


def test_sqlite_memory_store_delete_by_session() -> None:
    """Delete memory entries by session id."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = SqliteConfig(connection_config={"database": tmp.name})
        store = SqliteADKMemoryStore(config)
        store.create_tables()

        now = datetime.now(UTC)
        record1 = _build_record(session_id="s1", event_id="evt-1", content_text="espresso", inserted_at=now)
        record2 = _build_record(session_id="s2", event_id="evt-2", content_text="latte", inserted_at=now)
        store.insert_memory_entries([record1, record2])

        deleted = store.delete_entries_by_session("s1")
        assert deleted == 1

        remaining = store.search_entries(query="latte", app_name="app", user_id="user")
        assert len(remaining) == 1
        assert remaining[0]["session_id"] == "s2"


def test_sqlite_memory_store_delete_older_than() -> None:
    """Delete memory entries older than a cutoff."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = SqliteConfig(connection_config={"database": tmp.name})
        store = SqliteADKMemoryStore(config)
        store.create_tables()

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


def test_sqlite_memory_store_scoped_search_combined_default() -> None:
    """Default search recall returns both user-scoped and app-scoped memories."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = SqliteConfig(connection_config={"database": tmp.name})
        store = SqliteADKMemoryStore(config)
        store.create_tables()

        now = datetime.now(UTC)
        record_user = _build_record(
            session_id="s1",
            event_id="evt-u1",
            content_text="project architecture guideline",
            inserted_at=now,
            scope="user",
        )
        record_app = _build_record(
            session_id="s2",
            event_id="evt-a1",
            content_text="company architecture standard",
            inserted_at=now,
            scope="app",
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


def test_sqlite_memory_store_explicit_scope_filters() -> None:
    """Explicit scope filters restrict results to only user or only app memories."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = SqliteConfig(connection_config={"database": tmp.name})
        store = SqliteADKMemoryStore(config)
        store.create_tables()

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


def test_sqlite_memory_store_scoped_retention() -> None:
    """Scoped retention deletes entries matching app_name and scope filters."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = SqliteConfig(connection_config={"database": tmp.name})
        store = SqliteADKMemoryStore(config)
        store.create_tables()

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


def _build_record(
    *, session_id: str, event_id: str, content_text: str, inserted_at: datetime, scope: str = "user"
) -> "StoredMemory":
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


def _normalized(sql: str) -> str:
    """Collapse whitespace runs for SQL comparison."""
    return " ".join(sql.split())


def test_sqlite_fts_companions_create_for_empty_source() -> None:
    """Empty-table SQLite FTS5 companion creation synchronizes subsequent writes."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        plain_config = SqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"adk": {"memory_use_fts": False}}
        )
        fts_config = SqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"adk": {"memory_use_fts": True}}
        )
        try:
            plain_store = SqliteADKMemoryStore(plain_config)
            plain_store.create_tables()

            fts_store = SqliteADKMemoryStore(fts_config)
            with fts_config.provide_session() as driver:
                fts_store.reconcile_companion_objects_sync(driver)
                driver.commit()
                rows = driver.select(
                    "SELECT name FROM sqlite_master WHERE type IN ('table', 'trigger') AND name IN (?, ?, ?, ?)",
                    ("adk_memory_fts", "adk_memory_ai", "adk_memory_ad", "adk_memory_au"),
                )

            assert {str(row["name"]) for row in rows} == {
                "adk_memory_fts",
                "adk_memory_ai",
                "adk_memory_ad",
                "adk_memory_au",
            }

            now = datetime.now(UTC)
            record = _build_record(session_id="s1", event_id="evt-1", content_text="espresso roast", inserted_at=now)
            fts_store.insert_memory_entries([record])
            assert len(fts_store.search_entries("espresso", "app", "user")) == 1

            with fts_config.provide_session() as driver:
                driver.execute("UPDATE adk_memory SET content_text = ? WHERE event_id = ?", ("latte foam", "evt-1"))
                driver.commit()

            assert len(fts_store.search_entries("latte", "app", "user")) == 1
            assert len(fts_store.search_entries("espresso", "app", "user")) == 0

            fts_store.delete_entries_by_session("s1")
            assert len(fts_store.search_entries("latte", "app", "user")) == 0
        finally:
            plain_config.close_pool()
            fts_config.close_pool()


def test_sqlite_fts_companions_repeat_keeps_entries() -> None:
    """Repeated SQLite FTS5 companion reconciliation leaves docsize entries untouched."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = SqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"adk": {"memory_use_fts": True}}
        )
        try:
            store = SqliteADKMemoryStore(config)
            store.create_tables()
            now = datetime.now(UTC)
            record = _build_record(session_id="s1", event_id="evt-1", content_text="espresso roast", inserted_at=now)
            store.insert_memory_entries([record])

            with config.provide_session() as driver:
                store.reconcile_companion_objects_sync(driver)
                store.reconcile_companion_objects_sync(driver)
                driver.commit()
                docsize_rows = driver.select("SELECT count(*) AS total FROM adk_memory_fts_docsize")

            assert int(docsize_rows[0]["total"]) == 1
            assert [r["event_id"] for r in store.search_entries("espresso", "app", "user")] == ["evt-1"]
        finally:
            config.close_pool()


def test_sqlite_fts_companions_rebuild_populated_source() -> None:
    """Missing SQLite FTS5 objects and ordinary indexes repair and rebuild over existing rows."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        plain_config = SqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"adk": {"memory_use_fts": False}}
        )
        fts_config = SqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"adk": {"memory_use_fts": True}}
        )
        try:
            plain_store = SqliteADKMemoryStore(plain_config)
            plain_store.create_tables()
            now = datetime.now(UTC)
            record = _build_record(session_id="s1", event_id="evt-1", content_text="espresso roast", inserted_at=now)
            plain_store.insert_memory_entries([record])

            fts_store = SqliteADKMemoryStore(fts_config)
            with fts_config.provide_session() as driver:
                driver.execute("DROP INDEX idx_adk_memory_scope")
                driver.commit()
                fts_store.reconcile_companion_objects_sync(driver)
                driver.commit()
                objects = driver.select(
                    "SELECT name FROM sqlite_master WHERE name IN (?, ?)", ("adk_memory_fts", "idx_adk_memory_scope")
                )
                docsize_rows = driver.select("SELECT count(*) AS total FROM adk_memory_fts_docsize")

            assert {str(row["name"]) for row in objects} == {"adk_memory_fts", "idx_adk_memory_scope"}
            assert int(docsize_rows[0]["total"]) == 1
            assert [r["event_id"] for r in fts_store.search_entries("espresso", "app", "user")] == ["evt-1"]
        finally:
            plain_config.close_pool()
            fts_config.close_pool()


def test_sqlite_fts_companions_rebuild_after_partial_objects() -> None:
    """Dropping a single FTS5 trigger on a populated table recreates it and rebuilds once."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = SqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"adk": {"memory_use_fts": True}}
        )
        try:
            store = SqliteADKMemoryStore(config)
            store.create_tables()
            now = datetime.now(UTC)
            record = _build_record(session_id="s1", event_id="evt-1", content_text="espresso roast", inserted_at=now)
            store.insert_memory_entries([record])

            with config.provide_session() as driver:
                driver.execute("DROP TRIGGER adk_memory_au")
                driver.execute("UPDATE adk_memory SET content_text = ? WHERE event_id = ?", ("cortado crema", "evt-1"))
                driver.commit()
                store.reconcile_companion_objects_sync(driver)
                driver.commit()
                trigger_rows = driver.select(
                    "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name = ?", ("adk_memory_au",)
                )
                docsize_rows = driver.select("SELECT count(*) AS total FROM adk_memory_fts_docsize")

            assert len(trigger_rows) == 1
            assert int(docsize_rows[0]["total"]) == 1
            assert [r["event_id"] for r in store.search_entries("cortado", "app", "user")] == ["evt-1"]
        finally:
            config.close_pool()


def test_sqlite_fts_rebuild_runs_once() -> None:
    """External-content FTS5 rebuild runs once when objects are created and skips when complete."""
    config = SqliteConfig(
        extension_config={"adk": {"memory_use_fts": True, "fts_tokenize": "porter unicode61", "fts_detail": "column"}}
    )
    store = SqliteADKMemoryStore(config)
    session_store = SqliteADKStore(config)

    combined_session_ddl = _normalized(f"{session_store._sessions_table_ddl()} {session_store._events_table_ddl()}")
    for stmt in session_store._companion_index_statements():
        assert _normalized(stmt) in combined_session_ddl

    memory_ddl = _normalized(store._memory_table_ddl())
    for stmt in store._memory_index_statements():
        assert _normalized(stmt) in memory_ddl
    fts_stmts = store._memory_fts_statements()
    assert "tokenize = 'porter unicode61'" in fts_stmts["adk_memory_fts"]
    assert "detail = column" in fts_stmts["adk_memory_fts"]
    for stmt in fts_stmts.values():
        assert _normalized(stmt) in memory_ddl

    executed: list[str] = []
    master_responses: list[list[dict[str, object]]] = [
        [],
        [{"name": "ADK_MEMORY_FTS"}, {"name": "adk_memory_ai"}, {"name": "adk_memory_ad"}, {"name": "adk_memory_au"}],
    ]

    def _select(sql: str, *params: Any) -> list[dict[str, object]]:
        del params
        if "sqlite_master" in sql:
            return master_responses.pop(0)
        return [{"present": 1}]

    def _execute(sql: str, *params: Any) -> MagicMock:
        del params
        executed.append(sql)
        return MagicMock()

    driver = MagicMock()
    driver.select.side_effect = _select
    driver.execute.side_effect = _execute

    store.reconcile_companion_objects_sync(driver)
    store.reconcile_companion_objects_sync(driver)

    rebuild_calls = [stmt for stmt in executed if "VALUES('rebuild')" in stmt]
    assert len(rebuild_calls) == 1
    assert driver.commit.call_count == 0


def test_sqlite_fts_rebuild_rolls_back() -> None:
    """SQLite FTS5 companion creation and rebuild roll back with the caller transaction."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        plain_config = SqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"adk": {"memory_use_fts": False}}
        )
        fts_config = SqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"adk": {"memory_use_fts": True}}
        )
        try:
            plain_store = SqliteADKMemoryStore(plain_config)
            plain_store.create_tables()
            now = datetime.now(UTC)
            record = _build_record(session_id="s1", event_id="evt-1", content_text="espresso roast", inserted_at=now)
            plain_store.insert_memory_entries([record])

            fts_store = SqliteADKMemoryStore(fts_config)
            with fts_config.provide_session() as driver:
                driver.begin()
                fts_store.reconcile_companion_objects_sync(driver)
                in_tx_rows = driver.select("SELECT name FROM sqlite_master WHERE name = ?", ("adk_memory_fts",))
                driver.rollback()
                after_rows = driver.select("SELECT name FROM sqlite_master WHERE name = ?", ("adk_memory_fts",))

            assert len(in_tx_rows) == 1
            assert after_rows == []
        finally:
            plain_config.close_pool()
            fts_config.close_pool()


def test_sqlite_create_tables_rebuilds_existing_rows() -> None:
    """Runtime create_tables() rebuilds FTS5 when enabled on an already-populated table."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        plain_config = SqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"adk": {"memory_use_fts": False}}
        )
        fts_config = SqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"adk": {"memory_use_fts": True}}
        )
        try:
            plain_store = SqliteADKMemoryStore(plain_config)
            plain_store.create_tables()
            now = datetime.now(UTC)
            record = _build_record(session_id="s1", event_id="evt-1", content_text="espresso roast", inserted_at=now)
            plain_store.insert_memory_entries([record])

            fts_store = SqliteADKMemoryStore(fts_config)
            fts_store.create_tables()

            assert [r["event_id"] for r in fts_store.search_entries("espresso", "app", "user")] == ["evt-1"]
        finally:
            plain_config.close_pool()
            fts_config.close_pool()


def test_sqlite_fts_companions_create_only_missing_trigger_on_empty_source() -> None:
    """Partial FTS5 repair on an empty table creates only the missing trigger without rebuild."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = SqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"adk": {"memory_use_fts": True}}
        )
        try:
            store = SqliteADKMemoryStore(config)
            store.create_tables()

            with config.provide_session() as driver:
                driver.execute("DROP TRIGGER adk_memory_au")
                driver.commit()
                store.reconcile_companion_objects_sync(driver)
                driver.commit()
                rows = driver.select(
                    "SELECT name FROM sqlite_master WHERE type IN ('table', 'trigger') AND name IN (?, ?, ?, ?)",
                    ("adk_memory_fts", "adk_memory_ai", "adk_memory_ad", "adk_memory_au"),
                )

            assert {str(row["name"]) for row in rows} == {
                "adk_memory_fts",
                "adk_memory_ai",
                "adk_memory_ad",
                "adk_memory_au",
            }

            now = datetime.now(UTC)
            record = _build_record(session_id="s1", event_id="evt-1", content_text="espresso roast", inserted_at=now)
            store.insert_memory_entries([record])
            with config.provide_session() as driver:
                driver.execute("UPDATE adk_memory SET content_text = ? WHERE event_id = ?", ("latte foam", "evt-1"))
                driver.commit()

            assert [r["event_id"] for r in store.search_entries("latte", "app", "user")] == ["evt-1"]
            assert store.search_entries("espresso", "app", "user") == []
        finally:
            config.close_pool()


def test_sqlite_fts_companions_propagate_missing_fts5() -> None:
    """OperationalError from missing fts5 module propagates before trigger creation."""
    store = SqliteADKMemoryStore(SqliteConfig(extension_config={"adk": {"memory_use_fts": True}}))
    executed: list[str] = []

    def _select(sql: str, *params: Any) -> list[dict[str, object]]:
        del sql, params
        return []

    def _execute(sql: str, *params: Any) -> MagicMock:
        del params
        if "CREATE VIRTUAL TABLE" in sql:
            msg = "no such module: fts5"
            raise sqlite3.OperationalError(msg)
        executed.append(sql)
        return MagicMock()

    driver = MagicMock()
    driver.select.side_effect = _select
    driver.execute.side_effect = _execute

    with pytest.raises(sqlite3.OperationalError, match="fts5"):
        store.reconcile_companion_objects_sync(driver)

    assert not any("CREATE TRIGGER" in stmt for stmt in executed)
    assert driver.commit.call_count == 0


def test_sqlite_companions_restore_session_and_event_indexes() -> None:
    """Session store companion reconciliation restores dropped session and event indexes."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = SqliteConfig(connection_config={"database": tmp.name})
        try:
            store = SqliteADKStore(config)
            store.create_tables()
            store.create_session("s1", "app", "user", {})

            with config.provide_session() as driver:
                driver.execute("DROP INDEX idx_adk_session_update_time")
                driver.execute("DROP INDEX idx_adk_event_timestamp")
                driver.commit()
                store.reconcile_companion_objects_sync(driver)
                driver.commit()
                rows = driver.select(
                    "SELECT name FROM sqlite_master WHERE type = 'index' AND name IN (?, ?)",
                    ("idx_adk_session_update_time", "idx_adk_event_timestamp"),
                )

            assert {str(row["name"]) for row in rows} == {"idx_adk_session_update_time", "idx_adk_event_timestamp"}
            session = store.get_session("app", "user", "s1")
            assert session is not None and session["id"] == "s1"
        finally:
            config.close_pool()


def test_sqlite_companions_do_not_commit() -> None:
    """Session store companion reconciliation participates in caller rollback without committing."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = SqliteConfig(connection_config={"database": tmp.name})
        try:
            store = SqliteADKStore(config)
            store.create_tables()

            with config.provide_session() as driver:
                driver.execute("DROP INDEX idx_adk_session_update_time")
                driver.commit()
                driver.begin()
                store.reconcile_companion_objects_sync(driver)
                in_tx = driver.select(
                    "SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?", ("idx_adk_session_update_time",)
                )
                driver.rollback()
                after_rb = driver.select(
                    "SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?", ("idx_adk_session_update_time",)
                )

            assert len(in_tx) == 1
            assert after_rb == []
        finally:
            config.close_pool()


def test_sqlite_memory_companions_skip_disabled_store() -> None:
    """Disabled SQLite memory store skips companion execution, and non-FTS skips sqlite_master."""
    disabled_store = SqliteADKMemoryStore(SqliteConfig(extension_config={"adk": {"enable_memory": False}}))
    disabled_driver = MagicMock()
    disabled_store.reconcile_companion_objects_sync(disabled_driver)
    assert disabled_driver.execute.call_count == 0
    assert disabled_driver.select.call_count == 0

    plain_store = SqliteADKMemoryStore(SqliteConfig(extension_config={"adk": {"memory_use_fts": False}}))
    plain_driver = MagicMock()
    plain_store.reconcile_companion_objects_sync(plain_driver)
    assert plain_driver.execute.call_count == 3
    assert plain_driver.select.call_count == 0
