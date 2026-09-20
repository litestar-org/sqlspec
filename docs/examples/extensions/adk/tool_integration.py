from datetime import datetime, timezone

import anyio
import pytest

__all__ = ("test_adk_tool_integration",)


def test_adk_tool_integration() -> None:
    pytest.importorskip("aiosqlite")

    async def _run() -> int:
        # start-example
        from sqlspec.adapters.aiosqlite import AiosqliteConfig
        from sqlspec.adapters.aiosqlite.adk import AiosqliteADKMemoryStore
        from sqlspec.extensions.adk import StoredMemory

        config = AiosqliteConfig(
            connection_config={"database": ":memory:"}, extension_config={"adk": {"memory_use_fts": True}}
        )
        try:
            store = AiosqliteADKMemoryStore(config)
            await store.ensure_tables()

            record: StoredMemory = {
                "id": "mem_1",
                "session_id": "session_1",
                "app_name": "docs",
                "user_id": "user_1",
                "scope": "user",
                "event_id": "evt_1",
                "author": "tool",
                "timestamp": datetime.now(timezone.utc),
                "content_json": {"tool": "search", "query": "sqlspec"},
                "content_text": "tool:search query=sqlspec",
                "metadata_json": None,
                "inserted_at": datetime.now(timezone.utc),
                "embedding": None,
            }
            await store.insert_memory_entries([record])
            results = await store.search_entries(query="sqlspec", app_name="docs", user_id="user_1")
        finally:
            await config.close_pool()
        # end-example
        return len(results)

    count = anyio.run(_run)

    assert count == 1
