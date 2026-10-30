"""Index and query tool-generated memories with full-text search in AiosqliteADKMemoryStore."""

import asyncio
from datetime import UTC, datetime

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.adk import AiosqliteADKMemoryStore
from sqlspec.extensions.adk import StoredMemory

__all__ = ("main",)


async def main() -> int:
    """Insert a tool invocation memory entry and search it via FTS."""
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"adk": {"memory_use_fts": True}}
    )
    try:
        # --8<-- [start:example]
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
            "timestamp": datetime.now(UTC),
            "content_json": {"tool": "search", "query": "sqlspec"},
            "content_text": "tool:search query=sqlspec",
            "metadata_json": None,
            "inserted_at": datetime.now(UTC),
            "embedding": None,
        }
        await store.insert_memory_entries([record])
        results = await store.search_entries(query="sqlspec", app_name="docs", user_id="user_1")
        # --8<-- [end:example]
        assert len(results) == 1
        return len(results)
    finally:
        await config.close_pool()


if __name__ == "__main__":
    print(asyncio.run(main()))
