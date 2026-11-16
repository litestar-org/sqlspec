"""SQLSpecSessionService flows on the SQLite and aiosqlite ADK stores."""

import importlib.util
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

if importlib.util.find_spec("google.genai") is None or importlib.util.find_spec("google.adk") is None:
    pytest.skip("google-adk not installed", allow_module_level=True)

from google.adk.events.event import Event
from google.adk.events.event_actions import EventActions
from google.genai import types

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.adk import AiosqliteADKStore
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.adk import SqliteADKStore
from sqlspec.extensions.adk.service import SQLSpecSessionService


@pytest.fixture(params=["sqlite", "aiosqlite"])
async def service(request: pytest.FixtureRequest, tmp_path: Path) -> "AsyncIterator[SQLSpecSessionService]":
    database = str(tmp_path / "adk.db")
    config: Any
    if request.param == "sqlite":
        config = SqliteConfig(connection_config={"database": database})
        store: Any = SqliteADKStore(config)
        store.create_tables()
    else:
        config = AiosqliteConfig(connection_config={"database": database})
        store = AiosqliteADKStore(config)
        await store.create_tables()
    try:
        yield SQLSpecSessionService(store)
    finally:
        close = config.close_pool()
        if close is not None:
            await close


def _event(text: str) -> Event:
    return Event(
        author="user",
        invocation_id="inv",
        content=types.Content(role="user", parts=[types.Part(text=text)]),
        actions=EventActions(state_delta={}),
    )


def _event_texts(session: Any) -> "list[str] | None":
    if session is None:
        return None
    return [event.content.parts[0].text for event in session.events if event.content and event.content.parts]


async def _create(service: SQLSpecSessionService) -> Any:
    return await service.create_session(
        app_name="app", user_id="alice", state={"k": "alice-secret", "user:pref": "A"}, session_id="S1"
    )


async def test_append_event_after_create_session(service: SQLSpecSessionService) -> None:
    session = await _create(service)

    await service.append_event(session, _event("hello from alice"))

    stored = await service.get_session(app_name="app", user_id="alice", session_id="S1")
    assert _event_texts(stored) == ["hello from alice"]


async def test_append_two_events_in_sequence(service: SQLSpecSessionService) -> None:
    session = await _create(service)

    await service.append_event(session, _event("first"))
    await service.append_event(session, _event("second"))

    stored = await service.get_session(app_name="app", user_id="alice", session_id="S1")
    assert _event_texts(stored) == ["first", "second"]


async def test_create_session_timestamp_matches_storage(service: SQLSpecSessionService) -> None:
    session = await _create(service)

    stored = await service.get_session(app_name="app", user_id="alice", session_id="S1")

    assert getattr(stored, "last_update_time", None) == session.last_update_time
