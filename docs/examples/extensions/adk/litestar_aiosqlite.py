"""Expose SQLSpec-backed ADK sessions through a Litestar endpoint."""

import asyncio
from typing import Any

from litestar import Litestar, get

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.adk import AiosqliteADKStore
from sqlspec.extensions.adk import SQLSpecSessionService

config = AiosqliteConfig(pool_config={"database": ":memory:"})
service: "SQLSpecSessionService | None" = None


async def startup() -> None:
    """Initialize the ADK store when the app boots."""
    global service
    store = AiosqliteADKStore(config)
    await store.create_tables()
    service = SQLSpecSessionService(store)


@get("/sessions")
async def list_sessions() -> "dict[str, Any]":
    """Return the session count for the demo app."""
    assert service is not None
    sessions = await service.list_sessions(app_name="docs", user_id="demo")
    return {"count": len(sessions.sessions)}


app = Litestar(route_handlers=[list_sessions], on_startup=[startup])


def main() -> None:
    """Prime the database outside of Litestar for smoke tests."""
    asyncio.run(startup())


if __name__ == "__main__":
    main()
