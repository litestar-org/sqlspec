"""Unit tests for ADK memory converters."""

import importlib.util
from datetime import datetime, timezone

import pytest

if importlib.util.find_spec("google.genai") is None or importlib.util.find_spec("google.adk") is None:
    pytest.skip("google-adk not installed", allow_module_level=True)

from google.adk.events.event import Event
from google.adk.events.event_actions import EventActions
from google.adk.sessions.session import Session
from google.genai import types

from sqlspec.extensions.adk.memory.converters import (
    event_to_memory_record,
    extract_content_text,
    record_to_memory_entry,
    session_to_memory_records,
)


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
