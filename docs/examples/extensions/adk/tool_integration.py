"""Index and query tool-generated memories with full-text search in AiosqliteADKMemoryStore."""

import asyncio

# --8<-- [start:example]
from datetime import UTC, datetime

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.adk import AiosqliteADKMemoryStore
from sqlspec.extensions.adk import StoredMemory


async def search_tool_memories() -> list[StoredMemory]:
    """Insert a tool invocation memory entry and search it via FTS."""
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
            "timestamp": datetime.now(UTC),
            "content_json": {"tool": "search", "query": "sqlspec"},
            "content_text": "tool:search query=sqlspec",
            "metadata_json": None,
            "inserted_at": datetime.now(UTC),
            "embedding": None,
        }
        await store.insert_memory_entries([record])
        return await store.search_entries(query="sqlspec", app_name="docs", user_id="user_1")
    finally:
        await config.close_pool()


# --8<-- [end:example]

__all__ = ("main", "search_tool_memories")


async def main() -> int:
    """Execute the ADK memory FTS search example and return the match count."""
    results = await search_tool_memories()
    assert len(results) == 1
    return len(results)


if __name__ == "__main__":
    print(asyncio.run(main()))
