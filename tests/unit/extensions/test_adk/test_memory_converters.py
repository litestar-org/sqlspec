"""Unit tests for ADK memory converters."""

import importlib.util
from datetime import datetime, timezone

import pytest

if importlib.util.find_spec("google.genai") is None or importlib.util.find_spec("google.adk") is None:
    pytest.skip("google-adk not installed", allow_module_level=True)

from google.adk.events.event import Event
from google.adk.events.event_actions import EventActions
from google.adk.memory.memory_entry import MemoryEntry
from google.adk.sessions.session import Session
from google.genai import types

from sqlspec.extensions.adk.memory._types import StoredMemory
from sqlspec.extensions.adk.memory.converters import (
    event_to_memory_record,
    extract_content_text,
    memory_entry_to_record,
    record_to_memory_entry,
    session_to_memory_records,
)
from sqlspec.extensions.adk.memory.service import SQLSpecSyncMemoryService


class _MockSyncMemoryStore:
    def __init__(self) -> None:
        self.enabled = True
        self.inserted_batches: list[list[StoredMemory]] = []

    def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        self.inserted_batches.append(entries)
        return len(entries)

    def search_entries(
        self, *, query: str, app_name: str, user_id: str, limit: int | None = None
    ) -> list[StoredMemory]:
        return []


def _event(event_id: str, text: str | None) -> Event:
    content = types.Content(parts=[types.Part(text=text)]) if text is not None else None
    return Event(
        id=event_id,
        invocation_id="inv-1",
        author="user",
        content=content,
        actions=EventActions(),
        timestamp=datetime.now(timezone.utc).timestamp(),
        partial=False,
        turn_complete=True,
    )


def test_extract_content_text_combines_parts() -> None:
    content = types.Content(
        parts=[
            types.Part(text="hello"),
            types.Part(function_call=types.FunctionCall(name="lookup")),
            types.Part(function_response=types.FunctionResponse(name="lookup", response={"output": "ok"})),
        ]
    )
    text = extract_content_text(content)
    assert "hello" in text
    assert "function:lookup" in text
    assert "response:lookup" in text


def test_event_to_memory_record_skips_empty_content() -> None:
    event = _event("evt-empty", " ")
    record = event_to_memory_record(event, session_id="session-1", app_name="app", user_id="user")
    assert record is None


def test_session_to_memory_records_roundtrip() -> None:
    session = Session(
        id="session-1", app_name="app", user_id="user", state={}, events=[_event("evt-1", "Hello memory")]
    )
    records = session_to_memory_records(session)
    assert len(records) == 1

    entry = record_to_memory_entry(records[0])
    assert entry.author == "user"
    assert entry.content is not None
    assert entry.content.parts is not None
    assert entry.content.parts[0].text == "Hello memory"


def test_memory_entry_to_record_generates_unique_event_id_when_missing() -> None:
    entry_a = MemoryEntry(content=types.Content(parts=[types.Part(text="First memory")]))
    entry_b = MemoryEntry(content=types.Content(parts=[types.Part(text="Second memory")]))

    record_a = memory_entry_to_record(entry_a, app_name="app", user_id="user")
    record_b = memory_entry_to_record(entry_b, app_name="app", user_id="user")

    assert record_a is not None
    assert record_b is not None
    assert record_a["event_id"] == record_a["id"]
    assert record_b["event_id"] == record_b["id"]
    assert record_a["event_id"] != ""
    assert record_a["event_id"] != record_b["event_id"]
    assert record_a["session_id"] == "__unknown_session_id__"


def test_sync_memory_service_add_events_and_add_memory() -> None:
    store = _MockSyncMemoryStore()
    service = SQLSpecSyncMemoryService(store)  # type: ignore[arg-type]

    service.add_events_to_memory(
        app_name="app", user_id="user", events=[_event("evt-1", "Incremental memory")], session_id=None
    )
    service.add_memory(
        app_name="app",
        user_id="user",
        memories=[
            MemoryEntry(content=types.Content(parts=[types.Part(text="Direct memory 1")])),
            MemoryEntry(content=types.Content(parts=[types.Part(text="Direct memory 2")])),
        ],
    )

    assert len(store.inserted_batches) == 2
    assert store.inserted_batches[0][0]["session_id"] == "__unknown_session_id__"
    assert store.inserted_batches[0][0]["event_id"] == "evt-1"
    assert len(store.inserted_batches[1]) == 2
    assert store.inserted_batches[1][0]["event_id"] != store.inserted_batches[1][1]["event_id"]
    assert store.inserted_batches[1][0]["session_id"] == "__unknown_session_id__"
