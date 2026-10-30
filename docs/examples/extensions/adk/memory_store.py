"""Persist and reload Google ADK session events using AiosqliteADKStore."""

import asyncio
from datetime import UTC, datetime

from google.adk.events.event import Event
from google.genai import types

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.adk import AiosqliteADKStore
from sqlspec.extensions.adk import StoredEvent
from sqlspec.extensions.adk.converters import event_to_record

__all__ = ("main",)


async def main() -> list[StoredEvent]:
    """Create an ADK session, append a user event, and fetch stored events."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    try:
        # --8<-- [start:example]
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
        events = await store.get_events(session["app_name"], session["user_id"], session["id"])
        # --8<-- [end:example]
        assert len(events) == 1
        return events
    finally:
        await config.close_pool()


if __name__ == "__main__":
    print(len(asyncio.run(main())))
