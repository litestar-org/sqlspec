"""Expose SQLSpec-backed ADK sessions and memory through Litestar endpoints."""

import asyncio
from typing import Any

from litestar import Litestar, get

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.adk import AiosqliteADKStore
from sqlspec.adapters.aiosqlite.adk.store import AiosqliteADKMemoryStore
from sqlspec.extensions.adk import SQLSpecSessionService
from sqlspec.extensions.adk.memory import SQLSpecMemoryService

config = AiosqliteConfig(connection_config={"database": ":memory:"})
service: "SQLSpecSessionService | None" = None
memory_service: "SQLSpecMemoryService | None" = None


async def startup() -> None:
    """Initialize the ADK store when the app boots."""
    global service
    global memory_service
    store = AiosqliteADKStore(config)
    memory_store = AiosqliteADKMemoryStore(config)
    await store.create_tables()
    await memory_store.create_tables()
    service = SQLSpecSessionService(store)
    memory_service = SQLSpecMemoryService(memory_store)


@get("/sessions")
async def list_sessions() -> "dict[str, Any]":
    """Return the session count for the demo app."""
    assert service is not None
    sessions = await service.list_sessions(app_name="docs", user_id="demo")
    return {"count": len(sessions.sessions)}


@get("/memories")
async def list_memories(query: str = "demo") -> "dict[str, Any]":
    """Return memory count for a query string."""
    assert memory_service is not None
    response = await memory_service.search_memory(app_name="docs", user_id="demo", query=query)
    return {"count": len(response.memories)}


app = Litestar(route_handlers=[list_sessions, list_memories], on_startup=[startup])


def main() -> None:
    """Prime the database outside of Litestar for smoke tests."""
    asyncio.run(startup())


if __name__ == "__main__":
    main()
