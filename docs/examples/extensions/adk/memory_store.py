from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

import anyio
import pytest

if TYPE_CHECKING:
    from sqlspec.extensions.adk import EventRecord

__all__ = ("test_adk_memory_store",)


def test_adk_memory_store() -> None:
    pytest.importorskip("aiosqlite")
    pytest.importorskip("google.adk")

    async def _run() -> list[EventRecord]:
        # start-example
        from google.adk.events.event import Event
        from google.genai import types

        from sqlspec.adapters.aiosqlite import AiosqliteConfig
        from sqlspec.adapters.aiosqlite.adk import AiosqliteADKStore
        from sqlspec.extensions.adk.converters import event_to_record

        config = AiosqliteConfig(connection_config={"database": ":memory:"})
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
            timestamp=datetime.now(timezone.utc).timestamp(),
        )
        event_record = event_to_record(event, session["app_name"], session["user_id"], session["id"])
        await store.append_event(event_record)
        events = await store.get_events(session["app_name"], session["user_id"], session["id"])
        # end-example
        return events

    events = anyio.run(_run)

    assert len(events) == 1
