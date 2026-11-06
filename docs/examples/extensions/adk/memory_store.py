"""Persist and reload Google ADK session events using AiosqliteADKStore."""

import asyncio

# --8<-- [start:example]
from datetime import UTC, datetime

from google.adk.events.event import Event
from google.genai import types

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.adk import AiosqliteADKStore
from sqlspec.extensions.adk import StoredEvent
from sqlspec.extensions.adk.converters import event_to_record


async def persist_session_events() -> list[StoredEvent]:
    """Create an ADK session, append a user event, and fetch stored events."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    try:
        store = AiosqliteADKStore(config)
        await store.ensure_tables()

        session = await store.create_session(
            session_id="session_1", app_name="docs", user_id="user_1", state={"mode": "demo"}
        )

        event = Event(
            id="evt_1",
            invocation_id="inv_1",
            author="user",
            content=types.Content(parts=[types.Part(text="Hello")]),
            timestamp=datetime.now(UTC).timestamp(),
        )
        event_record = event_to_record(event, session["app_name"], session["user_id"], session["id"])
        await store.append_event(event_record)
        return await store.get_events(session["app_name"], session["user_id"], session["id"])
    finally:
        await config.close_pool()


# --8<-- [end:example]

__all__ = ("main", "persist_session_events")


async def main() -> list[StoredEvent]:
    """Execute the ADK session persistence example and verify stored events."""
    events = await persist_session_events()
    assert len(events) == 1
    return events


if __name__ == "__main__":
    print(len(asyncio.run(main())))
