"""Concrete embedded ADK offload keeps mutation reads on the writer."""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from google.adk.errors.already_exists_error import AlreadyExistsError

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.adk import AiosqliteADKMemoryStore, AiosqliteADKStore
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.adk import SqliteADKMemoryStore, SqliteADKStore
from sqlspec.extensions.adk.memory._types import StoredMemory
from sqlspec.extensions.adk.service import SQLSpecSessionService
from sqlspec.utils.routing import RoutingContext


def _entry(label: str) -> StoredMemory:
    now = datetime.now(UTC)
    return {
        "id": label,
        "session_id": "session",
        "app_name": "app",
        "user_id": "user",
        "scope": "user",
        "event_id": label,
        "author": "agent",
        "timestamp": now,
        "content_json": {},
        "content_text": f"entry {label}",
        "metadata_json": None,
        "inserted_at": now,
        "embedding": None,
    }


async def test_sync_adk_lagging_readonly_replica_and_mutation_consistency(tmp_path: Path) -> None:
    writer = SqliteConfig(connection_config={"database": str(tmp_path / "writer.db")}, bind_key="agents")
    seed = SqliteConfig(connection_config={"database": str(tmp_path / "reader.db")})
    for config, label in ((writer, "writer"), (seed, "reader")):
        sessions = SqliteADKStore(config)
        sessions.create_tables()
        sessions.create_session(label, "app", "user", {"source": label})
        memory = SqliteADKMemoryStore(config)
        memory.create_tables()
        memory.insert_memory_entries([_entry(label)])
    seed.close_pool()
    reader = seed.derive_pool(database=f"file:{tmp_path / 'reader.db'}?mode=ro")
    store = SqliteADKStore(writer, read_config=reader)
    assert store.get_session("app", "user", "writer") is None
    reader_record = store.get_session("app", "user", "reader")
    assert reader_record is not None
    assert reader_record["state"] == {"source": "reader"}
    writer_record = store.get_session("app", "user", "writer", renew_for=0)
    assert writer_record is not None
    assert writer_record["state"] == {"source": "writer"}
    assert [row["id"] for row in store.list_sessions("app", "user")] == ["reader"]
    memory = SqliteADKMemoryStore(writer, read_config=reader)
    assert [row["id"] for row in memory.search_entries("entry", "app", "user")] == ["reader"]
    with RoutingContext.primary("agents"):
        assert [row["id"] for row in memory.search_entries("entry", "app", "user")] == ["writer"]
    service = SQLSpecSessionService(store)
    with pytest.raises(AlreadyExistsError):
        await service.create_session(app_name="app", user_id="user", session_id="writer")
    await service.delete_session(app_name="app", user_id="user", session_id="writer")
    assert SqliteADKStore(writer).get_session("app", "user", "writer") is None
    writer.close_pool()
    reader.close_pool()


async def test_async_adk_readonly_replica_routes_reads_and_renewals(tmp_path: Path) -> None:
    writer = AiosqliteConfig(connection_config={"database": str(tmp_path / "writer.db")})
    seed = AiosqliteConfig(connection_config={"database": str(tmp_path / "reader.db")})
    for config, label in ((writer, "writer"), (seed, "reader")):
        sessions = AiosqliteADKStore(config)
        await sessions.create_tables()
        await sessions.create_session(label, "app", "user", {"source": label})
        memory = AiosqliteADKMemoryStore(config)
        await memory.create_tables()
        await memory.insert_memory_entries([_entry(label)])
    await seed.close_pool()
    reader = seed.derive_pool(database=f"file:{tmp_path / 'reader.db'}?mode=ro")
    store = AiosqliteADKStore(writer, read_config=reader)
    assert await store.get_session("app", "user", "writer") is None
    record = await store.get_session("app", "user", "writer", renew_for=0)
    assert record is not None
    assert record["state"] == {"source": "writer"}
    assert [row["id"] for row in await store.list_sessions("app", "user")] == ["reader"]
    memory = AiosqliteADKMemoryStore(writer, read_config=reader)
    assert [row["id"] for row in await memory.search_entries("entry", "app", "user")] == ["reader"]
    service = SQLSpecSessionService(store)
    with pytest.raises(AlreadyExistsError):
        await service.create_session(app_name="app", user_id="user", session_id="writer")
    await service.delete_session(app_name="app", user_id="user", session_id="writer")
    await writer.close_pool()
    await reader.close_pool()
