"""Integration tests for AioSQLite ADK memory store."""

import tempfile
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.adk import AiosqliteADKMemoryStore
from sqlspec.extensions.adk import StoredMemory

pytestmark = pytest.mark.xdist_group("sqlite")


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


async def test_aiosqlite_memory_store_insert_search_dedup() -> None:
    """Insert memory entries, search by text, and skip duplicates."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = AiosqliteConfig(connection_config={"database": tmp.name})
        store = AiosqliteADKMemoryStore(config)
        await store.create_tables()

        now = datetime.now(timezone.utc)
        record1 = _build_record(session_id="s1", event_id="evt-1", content_text="espresso", inserted_at=now)
        record2 = _build_record(session_id="s1", event_id="evt-2", content_text="latte", inserted_at=now)

        inserted = await store.insert_memory_entries([record1, record2])
        assert inserted == 2

        results = await store.search_entries(query="espresso", app_name="app", user_id="user")
        assert len(results) == 1
        assert results[0]["event_id"] == "evt-1"

        deduped = await store.insert_memory_entries([record1])
        assert deduped == 0

        await config.close_pool()


async def test_aiosqlite_memory_store_fts_search() -> None:
    """FTS-enabled memory stores search through the FTS5 virtual table."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = AiosqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"adk": {"memory_use_fts": True}}
        )
        store = AiosqliteADKMemoryStore(config)
        await store.create_tables()

        now = datetime.now(timezone.utc)
        record1 = _build_record(session_id="s1", event_id="evt-fts-1", content_text="espresso roast", inserted_at=now)
        record2 = _build_record(session_id="s1", event_id="evt-fts-2", content_text="latte foam", inserted_at=now)
        await store.insert_memory_entries([record1, record2])

        results = await store.search_entries(query="espresso", app_name="app", user_id="user")

        assert len(results) == 1
        assert results[0]["event_id"] == "evt-fts-1"

        await config.close_pool()


async def test_aiosqlite_memory_store_disabled_lifecycle() -> None:
    """Disabled memory stores skip table creation and reject memory operations."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = AiosqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"adk": {"enable_memory": False}}
        )
        store = AiosqliteADKMemoryStore(config)
        await store.create_tables()

        async with config.provide_connection() as conn:
            cursor = await conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", ("adk_memory_entries",)
            )
            row = await cursor.fetchone()

        assert row is None

        now = datetime.now(timezone.utc)
        record = _build_record(session_id="s1", event_id="evt-disabled", content_text="espresso", inserted_at=now)
        with pytest.raises(RuntimeError, match="Memory store is disabled"):
            await store.insert_memory_entries([record])
        with pytest.raises(RuntimeError, match="Memory store is disabled"):
            await store.search_entries(query="espresso", app_name="app", user_id="user")

        await config.close_pool()


async def test_aiosqlite_memory_store_delete_by_session() -> None:
    """Delete memory entries by session id."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = AiosqliteConfig(connection_config={"database": tmp.name})
        store = AiosqliteADKMemoryStore(config)
        await store.create_tables()

        now = datetime.now(timezone.utc)
        record1 = _build_record(session_id="s1", event_id="evt-1", content_text="espresso", inserted_at=now)
        record2 = _build_record(session_id="s2", event_id="evt-2", content_text="latte", inserted_at=now)
        await store.insert_memory_entries([record1, record2])

        deleted = await store.delete_entries_by_session("s1")
        assert deleted == 1

        remaining = await store.search_entries(query="latte", app_name="app", user_id="user")
        assert len(remaining) == 1
        assert remaining[0]["session_id"] == "s2"

        await config.close_pool()


async def test_aiosqlite_memory_store_delete_older_than() -> None:
    """Delete memory entries older than a cutoff."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = AiosqliteConfig(connection_config={"database": tmp.name})
        store = AiosqliteADKMemoryStore(config)
        await store.create_tables()

        now = datetime.now(timezone.utc)
        old = now - timedelta(days=40)
        record1 = _build_record(session_id="s1", event_id="evt-1", content_text="old", inserted_at=old)
        record2 = _build_record(session_id="s1", event_id="evt-2", content_text="new", inserted_at=now)
        await store.insert_memory_entries([record1, record2])

        deleted = await store.delete_entries_older_than(30)
        assert deleted == 1

        remaining = await store.search_entries(query="new", app_name="app", user_id="user")
        assert len(remaining) == 1
        assert remaining[0]["event_id"] == "evt-2"


async def test_aiosqlite_memory_store_scoped_search_combined_default() -> None:
    """Default search recall returns both user-scoped and app-scoped memories."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = AiosqliteConfig(connection_config={"database": tmp.name})
        store = AiosqliteADKMemoryStore(config)
        await store.create_tables()

        now = datetime.now(timezone.utc)
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

        await store.insert_memory_entries([record_user, record_app, record_other_user])

        results = await store.search_entries(query="architecture", app_name="app", user_id="user")
        event_ids = {r["event_id"] for r in results}
        assert event_ids == {"evt-u1", "evt-a1"}
        assert "evt-other" not in event_ids


async def test_aiosqlite_memory_store_explicit_scope_filters() -> None:
    """Explicit scope filters restrict results to only user or only app memories."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = AiosqliteConfig(connection_config={"database": tmp.name})
        store = AiosqliteADKMemoryStore(config)
        await store.create_tables()

        now = datetime.now(timezone.utc)
        record_user = _build_record(
            session_id="s1", event_id="evt-u1", content_text="scoped query plan", inserted_at=now, scope="user"
        )
        record_app = _build_record(
            session_id="s2", event_id="evt-a1", content_text="scoped release plan", inserted_at=now, scope="app"
        )
        await store.insert_memory_entries([record_user, record_app])

        user_only = await store.search_entries(query="scoped", app_name="app", user_id="user", scope_filter="user")
        assert len(user_only) == 1
        assert user_only[0]["event_id"] == "evt-u1"

        app_only = await store.search_entries(query="scoped", app_name="app", user_id="user", scope_filter="app")
        assert len(app_only) == 1
        assert app_only[0]["event_id"] == "evt-a1"


async def test_aiosqlite_memory_store_scoped_retention() -> None:
    """Scoped retention deletes entries matching app_name and scope filters."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = AiosqliteConfig(connection_config={"database": tmp.name})
        store = AiosqliteADKMemoryStore(config)
        await store.create_tables()

        now = datetime.now(timezone.utc)
        old = now - timedelta(days=40)
        record_user_old = _build_record(
            session_id="s1", event_id="evt-uo", content_text="old user memo", inserted_at=old, scope="user"
        )
        record_app_old = _build_record(
            session_id="s2", event_id="evt-ao", content_text="old app guideline", inserted_at=old, scope="app"
        )
        await store.insert_memory_entries([record_user_old, record_app_old])

        deleted = await store.delete_entries_older_than(30, app_name="app", scope="user")
        assert deleted == 1

        remaining = await store.search_entries(query="old", app_name="app", user_id="user")
        assert len(remaining) == 1
        assert remaining[0]["event_id"] == "evt-ao"

        await config.close_pool()
