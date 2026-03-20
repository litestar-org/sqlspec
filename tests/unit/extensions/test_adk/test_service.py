"""Unit tests for SQLSpecSessionService — state persistence fix.

Tests the NEW contract specified in Chapter 1 of the ADK Clean-Break Overhaul:
- append_event calls append_event_and_update_state (not the old append_event)
- temp: keys are stripped before persisting session state
- partial events are not persisted
- create_session strips temp: keys from initial state

The store is mocked — no database required.
"""

import importlib.util
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

if importlib.util.find_spec("google.genai") is None or importlib.util.find_spec("google.adk") is None:
    pytest.skip("google-adk not installed", allow_module_level=True)

from google.adk.events.event import Event
from google.adk.events.event_actions import EventActions
from google.adk.sessions.session import Session

from sqlspec.extensions.adk.service import SQLSpecSessionService


# ---------------------------------------------------------------------------
# Mock store
# ---------------------------------------------------------------------------


class MockStore:
    """Simple mock that records calls to store methods.

    Attributes are set to AsyncMock so that await works out of the box,
    and call arguments are captured for assertion.
    """

    def __init__(self) -> None:
        # Track calls to the new combined method
        self.append_event_and_update_state_calls: list[dict[str, Any]] = []
        self.append_event_and_update_state_called = False

        # Track calls to create_session
        self.create_session_calls: list[dict[str, Any]] = []

        # Provide a get_session that returns a minimal session record
        self._session_record = {
            "id": "s1",
            "app_name": "app",
            "user_id": "u1",
            "state": {},
            "create_time": datetime.now(timezone.utc),
            "update_time": datetime.now(timezone.utc),
        }

    async def append_event_and_update_state(
        self, event_record: Any, session_id: str, state: "dict[str, Any]"
    ) -> None:
        self.append_event_and_update_state_called = True
        self.append_event_and_update_state_calls.append(
            {"event_record": event_record, "session_id": session_id, "state": state}
        )

    async def get_session(self, session_id: str) -> "dict[str, Any] | None":
        return self._session_record

    async def create_session(
        self, *, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]"
    ) -> "dict[str, Any]":
        self.create_session_calls.append(
            {"session_id": session_id, "app_name": app_name, "user_id": user_id, "state": state}
        )
        return {
            "id": session_id,
            "app_name": app_name,
            "user_id": user_id,
            "state": state,
            "create_time": datetime.now(timezone.utc),
            "update_time": datetime.now(timezone.utc),
        }

    # Old method — should NOT be called by the new service
    async def append_event(self, event_record: Any) -> None:
        raise AssertionError("append_event (old method) must not be called — use append_event_and_update_state")

    async def get_events(self, *, session_id: str, after_timestamp: Any = None, limit: Any = None) -> list:
        return []

    async def list_sessions(self, *, app_name: str, user_id: "str | None" = None) -> list:
        return []

    async def delete_session(self, session_id: str) -> None:
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_session(
    *,
    session_id: str = "s1",
    app_name: str = "app",
    user_id: str = "u1",
    state: "dict | None" = None,
) -> Session:
    return Session(
        id=session_id,
        app_name=app_name,
        user_id=user_id,
        state=state or {},
        last_update_time=datetime.now(timezone.utc).timestamp(),
    )


def _make_event(
    *,
    invocation_id: str = "inv-1",
    author: str = "model",
    state_delta: "dict | None" = None,
    partial: bool = False,
) -> Event:
    actions = EventActions(state_delta=state_delta or {})
    return Event(
        invocation_id=invocation_id,
        author=author,
        actions=actions,
        timestamp=datetime.now(timezone.utc).timestamp(),
        partial=partial,
    )


# ---------------------------------------------------------------------------
# append_event — calls append_event_and_update_state
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_append_event_calls_append_event_and_update_state() -> None:
    """append_event must call append_event_and_update_state, not the old append_event."""
    store = MockStore()
    service = SQLSpecSessionService(store)  # type: ignore[arg-type]
    session = _make_session(state={"key": "v0"})
    event = _make_event(state_delta={"key": "v1"})

    await service.append_event(session, event)

    assert store.append_event_and_update_state_called, (
        "append_event_and_update_state was never called — state will not be persisted"
    )


@pytest.mark.anyio
async def test_append_event_persists_updated_state() -> None:
    """append_event persists the state AFTER applying event.actions.state_delta."""
    store = MockStore()
    service = SQLSpecSessionService(store)  # type: ignore[arg-type]
    session = _make_session(state={"key": "v0"})
    event = _make_event(state_delta={"key": "v1"})

    await service.append_event(session, event)

    assert store.append_event_and_update_state_called
    last_call = store.append_event_and_update_state_calls[-1]
    # The persisted state must reflect the mutation from state_delta
    assert last_call["state"]["key"] == "v1"


@pytest.mark.anyio
async def test_append_event_strips_temp_from_persisted_state() -> None:
    """temp: keys are removed before state persistence."""
    store = MockStore()
    service = SQLSpecSessionService(store)  # type: ignore[arg-type]
    session = _make_session(state={"key": "v", "temp:transient": "should_not_persist"})
    event = _make_event()

    await service.append_event(session, event)

    assert store.append_event_and_update_state_called
    last_call = store.append_event_and_update_state_calls[-1]
    persisted_state = last_call["state"]
    assert "temp:transient" not in persisted_state
    assert persisted_state["key"] == "v"


@pytest.mark.anyio
async def test_append_event_strips_temp_state_delta_from_persisted_state() -> None:
    """temp: keys added via state_delta are also stripped before persisting."""
    store = MockStore()
    service = SQLSpecSessionService(store)  # type: ignore[arg-type]
    # Session state has temp: key added by an agent via state_delta
    session = _make_session(state={"regular": "v"})
    event = _make_event(state_delta={"temp:output": "transient", "regular": "updated"})

    await service.append_event(session, event)

    last_call = store.append_event_and_update_state_calls[-1]
    persisted_state = last_call["state"]
    assert "temp:output" not in persisted_state
    assert persisted_state["regular"] == "updated"


@pytest.mark.anyio
async def test_append_event_skips_partial_events() -> None:
    """Partial events are not persisted to the store."""
    store = MockStore()
    service = SQLSpecSessionService(store)  # type: ignore[arg-type]
    session = _make_session()
    partial_event = _make_event(partial=True)

    result = await service.append_event(session, partial_event)

    assert not store.append_event_and_update_state_called, (
        "append_event_and_update_state must NOT be called for partial events"
    )
    assert result.partial is True


@pytest.mark.anyio
async def test_append_event_passes_correct_session_id_to_store() -> None:
    """append_event_and_update_state receives the correct session_id."""
    store = MockStore()
    service = SQLSpecSessionService(store)  # type: ignore[arg-type]
    session = _make_session(session_id="my-unique-session-id")
    event = _make_event()

    await service.append_event(session, event)

    last_call = store.append_event_and_update_state_calls[-1]
    assert last_call["session_id"] == "my-unique-session-id"


@pytest.mark.anyio
async def test_append_event_event_record_has_5_keys() -> None:
    """The event_record passed to the store has exactly 5 keys (new schema)."""
    store = MockStore()
    service = SQLSpecSessionService(store)  # type: ignore[arg-type]
    session = _make_session()
    event = _make_event()

    await service.append_event(session, event)

    last_call = store.append_event_and_update_state_calls[-1]
    event_record = last_call["event_record"]
    assert set(event_record.keys()) == {"session_id", "invocation_id", "author", "timestamp", "event_json"}


@pytest.mark.anyio
async def test_append_event_returns_the_event() -> None:
    """append_event returns the event after persisting."""
    store = MockStore()
    service = SQLSpecSessionService(store)  # type: ignore[arg-type]
    session = _make_session()
    event = _make_event(author="model")

    result = await service.append_event(session, event)

    assert result is not None
    assert result.author == "model"


# ---------------------------------------------------------------------------
# create_session — strips temp: keys from initial state
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_create_session_strips_temp_keys_from_initial_state() -> None:
    """create_session filters temp: keys before passing state to the store."""
    store = MockStore()
    service = SQLSpecSessionService(store)  # type: ignore[arg-type]

    await service.create_session(
        app_name="app", user_id="u1", state={"x": 1, "temp:y": 2, "app:z": 3}
    )

    assert len(store.create_session_calls) == 1
    persisted_state = store.create_session_calls[0]["state"]
    assert "temp:y" not in persisted_state
    assert persisted_state["x"] == 1
    assert persisted_state["app:z"] == 3


@pytest.mark.anyio
async def test_create_session_with_only_temp_state_persists_empty() -> None:
    """create_session with only temp: state persists empty state dict."""
    store = MockStore()
    service = SQLSpecSessionService(store)  # type: ignore[arg-type]

    await service.create_session(app_name="app", user_id="u1", state={"temp:only": "gone"})

    assert store.create_session_calls[0]["state"] == {}


@pytest.mark.anyio
async def test_create_session_none_state_persists_empty() -> None:
    """create_session with state=None persists empty state dict."""
    store = MockStore()
    service = SQLSpecSessionService(store)  # type: ignore[arg-type]

    await service.create_session(app_name="app", user_id="u1")

    assert store.create_session_calls[0]["state"] == {}


@pytest.mark.anyio
async def test_create_session_generates_uuid_if_no_session_id() -> None:
    """create_session generates a UUID if no session_id is provided."""
    store = MockStore()
    service = SQLSpecSessionService(store)  # type: ignore[arg-type]

    session = await service.create_session(app_name="app", user_id="u1")

    assert session.id is not None
    assert len(session.id) > 0


@pytest.mark.anyio
async def test_create_session_uses_provided_session_id() -> None:
    """create_session uses the caller-provided session_id."""
    store = MockStore()
    service = SQLSpecSessionService(store)  # type: ignore[arg-type]

    session = await service.create_session(app_name="app", user_id="u1", session_id="my-id")

    assert session.id == "my-id"
