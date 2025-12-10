"""Persist Google ADK sessions with SQLSpec + AioSQLite."""

import asyncio
from datetime import datetime, timezone

from google.adk.events.event import Event
from google.genai import types

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.adk import AiosqliteADKStore
from sqlspec.extensions.adk import SQLSpecSessionService

__all__ = ("main",)


def _event(author: str, text: str) -> Event:
    return Event(
        id=f"evt_{author}",
        invocation_id="inv_1",
        author=author,
        branch="main",
        actions=[],
        timestamp=datetime.now(timezone.utc).timestamp(),
        content=types.Content(parts=[types.Part(text=text)]),
        partial=False,
        turn_complete=True,
    )


async def main() -> None:
    """Create a session, append two events, and read the transcript."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    store = AiosqliteADKStore(config)
    await store.create_tables()
    service = SQLSpecSessionService(store)
    session = await service.create_session(app_name="docs", user_id="demo", state={"mode": "chat"})
    await service.append_event(session, _event("user", "How does SQLSpec store sessions?"))
    await service.append_event(session, _event("assistant", "Sessions live in SQLite tables via the ADK store."))
    replay = await service.get_session(app_name="docs", user_id="demo", session_id=session.id)
    total = len(replay.events) if replay else 0
    print({"session": session.id, "events": total})


if __name__ == "__main__":
    asyncio.run(main())
