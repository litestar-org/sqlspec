"""SQLite and aiosqlite ADK session flows through SQLSpecSessionService."""

import importlib.util
import inspect
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


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


@pytest.fixture(params=["sqlite", "aiosqlite"])
async def service(request: pytest.FixtureRequest, tmp_path: Path) -> "AsyncIterator[SQLSpecSessionService]":
    database = str(tmp_path / "adk.db")
    if request.param == "sqlite":
        config: Any = SqliteConfig(connection_config={"database": database})
        store: Any = SqliteADKStore(config)
    else:
        config = AiosqliteConfig(connection_config={"database": database})
        store = AiosqliteADKStore(config)
    await _maybe_await(store.create_tables())
    try:
        yield SQLSpecSessionService(store)
    finally:
        await _maybe_await(config.close_pool())


def _event(text: str) -> Event:
    return Event(
        author="user",
        invocation_id="inv",
        content=types.Content(role="user", parts=[types.Part(text=text)]),
        actions=EventActions(state_delta={}),
    )


async def test_append_event_after_create_session(service: SQLSpecSessionService) -> None:
    session = await service.create_session(
        app_name="app", user_id="alice", state={"k": "alice-secret", "user:pref": "A"}, session_id="S1"
    )

    await service.append_event(session, _event("hello from alice"))

    stored = await service.get_session(app_name="app", user_id="alice", session_id="S1")
    assert stored is not None
    assert [e.content.parts[0].text for e in stored.events if e.content and e.content.parts] == ["hello from alice"]


async def test_append_two_events_in_sequence(service: SQLSpecSessionService) -> None:
    session = await service.create_session(app_name="app", user_id="alice", state={}, session_id="S2")

    await service.append_event(session, _event("first"))
    await service.append_event(session, _event("second"))

    stored = await service.get_session(app_name="app", user_id="alice", session_id="S2")
    assert stored is not None
    assert len(stored.events) == 2


async def test_create_session_timestamp_matches_storage(service: SQLSpecSessionService) -> None:
    created = await service.create_session(app_name="app", user_id="alice", state={}, session_id="S3")

    stored = await service.get_session(app_name="app", user_id="alice", session_id="S3")

    assert stored is not None
    assert created.last_update_time == stored.last_update_time
