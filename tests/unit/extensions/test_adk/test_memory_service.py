"""Unit tests for ADK memory service clean-break behavior."""

import importlib.util
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pytest

if importlib.util.find_spec("google.genai") is None or importlib.util.find_spec("google.adk") is None:
    pytest.skip("google-adk not installed", allow_module_level=True)

from google.adk.events.event import Event
from google.adk.events.event_actions import EventActions
from google.adk.memory.memory_entry import MemoryEntry
from google.genai import types

from sqlspec.extensions.adk.memory import SQLSpecMemoryService

if TYPE_CHECKING:
    from sqlspec.extensions.adk.memory import MemoryRecord


class _MemoryStore:
    def __init__(self) -> None:
        self.entries: list[MemoryRecord] = []

    async def insert_memory_entries(self, entries: list["MemoryRecord"], owner_id: object | None = None) -> int:
        self.entries.extend(entries)
        return len(entries)

    async def search_entries(
        self, query: str, app_name: str, user_id: str, limit: int | None = None
    ) -> list["MemoryRecord"]:
        return [
            entry
            for entry in self.entries
            if entry["app_name"] == app_name and entry["user_id"] == user_id and query in entry["content_text"]
        ]


def _event(event_id: str, text: str, custom_metadata: dict[str, object] | None = None) -> Event:
    return Event(
        id=event_id,
        invocation_id="inv-1",
        author="user",
        content=types.Content(parts=[types.Part(text=text)]),
        actions=EventActions(),
        timestamp=datetime.now(timezone.utc).timestamp(),
        partial=False,
        turn_complete=True,
        custom_metadata=custom_metadata,
    )


async def test_add_events_to_memory_persists_user_scoped_delta_metadata() -> None:
    store = _MemoryStore()
    service = SQLSpecMemoryService(store)  # type: ignore[arg-type]

    await service.add_events_to_memory(
        app_name="app", user_id="user", events=[_event("evt-1", "delta memory")], custom_metadata={"ttl": 3600}
    )

    assert len(store.entries) == 1
    record = store.entries[0]
    assert record["session_id"] == ""
    assert record["event_id"] == "evt-1"
    assert record["metadata_json"] == {"ttl": 3600}


async def test_add_events_to_memory_merges_event_and_call_metadata() -> None:
    store = _MemoryStore()
    service = SQLSpecMemoryService(store)  # type: ignore[arg-type]

    await service.add_events_to_memory(
        app_name="app",
        user_id="user",
        events=[_event("evt-1", "delta memory", custom_metadata={"source": "event", "priority": 2})],
        custom_metadata={"source": "call", "ttl": 3600},
    )

    assert len(store.entries) == 1
    assert store.entries[0]["metadata_json"] == {"source": "event", "ttl": 3600, "priority": 2}


async def test_add_memory_preserves_entry_metadata_and_supports_search() -> None:
    store = _MemoryStore()
    service = SQLSpecMemoryService(store)  # type: ignore[arg-type]
    memory = MemoryEntry(
        id="memory-1",
        author="agent",
        timestamp="2026-05-23T12:00:00+00:00",
        content=types.Content(parts=[types.Part(text="direct memory")]),
        custom_metadata={"source": "entry"},
    )

    await service.add_memory(app_name="app", user_id="user", memories=[memory], custom_metadata={"ttl": 3600})
    response = await service.search_memory(app_name="app", user_id="user", query="direct")

    assert len(response.memories) == 1
    result = response.memories[0]
    assert result.id == "memory-1"
    assert result.author == "agent"
    assert result.custom_metadata == {"ttl": 3600, "source": "entry"}


def test_sync_memory_service_is_not_public_clean_break_surface() -> None:
    import sqlspec.extensions.adk as adk
    import sqlspec.extensions.adk.memory as memory

    assert "SQLSpecSyncMemoryService" not in adk.__all__
    assert "SQLSpecSyncMemoryService" not in memory.__all__
    assert not hasattr(adk, "SQLSpecSyncMemoryService")
    assert not hasattr(memory, "SQLSpecSyncMemoryService")
