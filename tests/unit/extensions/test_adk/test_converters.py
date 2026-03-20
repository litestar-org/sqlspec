"""Unit tests for ADK session/event converters and scoped state helpers.

Tests the NEW contract specified in Chapter 1 of the ADK Clean-Break Overhaul:
- EventRecord has exactly 5 keys (session_id, invocation_id, author, timestamp, event_json)
- event_to_record takes only (event, session_id), not (event, session_id, app_name, user_id)
- record_to_event uses Event.model_validate for full round-trip fidelity
- filter_temp_state, split_scoped_state, merge_scoped_state for scoped state handling
- session_to_record strips temp: keys from state
"""

import importlib.util
from datetime import datetime, timezone

import pytest

if importlib.util.find_spec("google.genai") is None or importlib.util.find_spec("google.adk") is None:
    pytest.skip("google-adk not installed", allow_module_level=True)

from google.adk.events.event import Event
from google.adk.events.event_actions import EventActions
from google.adk.sessions.session import Session
from google.genai import types

from sqlspec.extensions.adk.converters import (
    event_to_record,
    filter_temp_state,
    merge_scoped_state,
    record_to_event,
    record_to_session,
    session_to_record,
    split_scoped_state,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_event(
    *,
    event_id: str = "evt-1",
    invocation_id: str = "inv-1",
    author: str = "user",
    text: "str | None" = None,
    state_delta: "dict | None" = None,
    branch: "str | None" = None,
    partial: "bool | None" = None,
    turn_complete: "bool | None" = None,
    custom_metadata: "dict | None" = None,
) -> Event:
    content = types.Content(parts=[types.Part(text=text)]) if text is not None else None
    actions = EventActions(state_delta=state_delta or {})
    return Event(
        id=event_id,
        invocation_id=invocation_id,
        author=author,
        content=content,
        actions=actions,
        timestamp=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp(),
        branch=branch,
        partial=partial,
        turn_complete=turn_complete,
        custom_metadata=custom_metadata,
    )


def _make_session(
    *,
    session_id: str = "session-1",
    app_name: str = "test-app",
    user_id: str = "user-1",
    state: "dict | None" = None,
) -> Session:
    return Session(
        id=session_id,
        app_name=app_name,
        user_id=user_id,
        state=state or {},
        last_update_time=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp(),
    )


# ---------------------------------------------------------------------------
# filter_temp_state
# ---------------------------------------------------------------------------


def test_filter_temp_state_removes_temp_keys() -> None:
    """temp:-prefixed keys are removed; all other keys are kept."""
    state = {"x": 1, "temp:y": 2, "app:z": 3, "user:w": 4}
    result = filter_temp_state(state)
    assert result == {"x": 1, "app:z": 3, "user:w": 4}


def test_filter_temp_state_empty_dict() -> None:
    """Empty dict returns empty dict."""
    assert filter_temp_state({}) == {}


def test_filter_temp_state_all_temp_keys() -> None:
    """Dict with only temp: keys returns empty dict."""
    state = {"temp:a": 1, "temp:b": 2, "temp:": 3}
    assert filter_temp_state(state) == {}


def test_filter_temp_state_no_temp_keys() -> None:
    """Dict with no temp: keys is returned unchanged."""
    state = {"x": 1, "app:y": 2, "user:z": 3}
    result = filter_temp_state(state)
    assert result == state


def test_filter_temp_state_does_not_mutate_input() -> None:
    """Input dict is not mutated."""
    state = {"key": "v", "temp:remove": "gone"}
    original = dict(state)
    filter_temp_state(state)
    assert state == original


# ---------------------------------------------------------------------------
# split_scoped_state
# ---------------------------------------------------------------------------


def test_split_scoped_state_separates_buckets() -> None:
    """app:, user:, and plain keys go into the correct buckets."""
    state = {"app:shared": "a", "user:profile": "u", "session_key": "s", "another": "v"}
    app, user, session = split_scoped_state(state)
    assert app == {"app:shared": "a"}
    assert user == {"user:profile": "u"}
    assert session == {"session_key": "s", "another": "v"}


def test_split_scoped_state_empty() -> None:
    """Empty state produces three empty dicts."""
    app, user, session = split_scoped_state({})
    assert app == {}
    assert user == {}
    assert session == {}


def test_split_scoped_state_only_app_keys() -> None:
    """State with only app: keys puts everything in app bucket."""
    state = {"app:x": 1, "app:y": 2}
    app, user, session = split_scoped_state(state)
    assert app == {"app:x": 1, "app:y": 2}
    assert user == {}
    assert session == {}


def test_split_scoped_state_only_user_keys() -> None:
    """State with only user: keys puts everything in user bucket."""
    state = {"user:a": "one", "user:b": "two"}
    app, user, session = split_scoped_state(state)
    assert app == {}
    assert user == {"user:a": "one", "user:b": "two"}
    assert session == {}


def test_split_scoped_state_only_session_keys() -> None:
    """State with no prefix puts everything in session bucket."""
    state = {"key1": 1, "key2": 2}
    app, user, session = split_scoped_state(state)
    assert app == {}
    assert user == {}
    assert session == {"key1": 1, "key2": 2}


def test_split_scoped_state_preserves_full_key_names() -> None:
    """Keys are not stripped of their prefix in the returned buckets."""
    state = {"app:my_key": "val", "user:my_key": "val2"}
    app, user, _ = split_scoped_state(state)
    assert "app:my_key" in app
    assert "user:my_key" in user


# ---------------------------------------------------------------------------
# merge_scoped_state
# ---------------------------------------------------------------------------


def test_merge_scoped_state_combines_all_buckets() -> None:
    """All three buckets appear in the merged result."""
    merged = merge_scoped_state(
        session_state={"key": "s"},
        app_state={"app:x": "a"},
        user_state={"user:y": "u"},
    )
    assert merged == {"key": "s", "app:x": "a", "user:y": "u"}


def test_merge_scoped_state_overlay_priority_app_over_session() -> None:
    """app_state overlays session_state for the same key."""
    merged = merge_scoped_state(
        session_state={"app:x": "old"},
        app_state={"app:x": "new"},
    )
    assert merged["app:x"] == "new"


def test_merge_scoped_state_overlay_priority_user_over_session() -> None:
    """user_state overlays session_state for the same key."""
    merged = merge_scoped_state(
        session_state={"user:y": "session_val"},
        user_state={"user:y": "user_val"},
    )
    assert merged["user:y"] == "user_val"


def test_merge_scoped_state_no_app_no_user() -> None:
    """Merging without app_state or user_state returns session_state copy."""
    session = {"key": "v", "other": 42}
    merged = merge_scoped_state(session_state=session)
    assert merged == session


def test_merge_scoped_state_empty_session_state() -> None:
    """Empty session_state with app/user state returns combined app+user keys."""
    merged = merge_scoped_state(
        session_state={},
        app_state={"app:a": 1},
        user_state={"user:b": 2},
    )
    assert merged == {"app:a": 1, "user:b": 2}


def test_merge_scoped_state_does_not_mutate_session_state() -> None:
    """Input session_state dict is not mutated."""
    session = {"key": "v"}
    original = dict(session)
    merge_scoped_state(session_state=session, app_state={"app:x": 1})
    assert session == original


# ---------------------------------------------------------------------------
# event_to_record — signature and structure
# ---------------------------------------------------------------------------


def test_event_to_record_only_5_keys() -> None:
    """EventRecord has exactly session_id, invocation_id, author, timestamp, event_json."""
    event = _make_event()
    record = event_to_record(event, "session-1")
    assert set(record.keys()) == {"session_id", "invocation_id", "author", "timestamp", "event_json"}


def test_event_to_record_signature_two_args_only() -> None:
    """event_to_record raises TypeError if called with extra positional args (old 4-arg signature)."""
    event = _make_event()
    with pytest.raises(TypeError):
        event_to_record(event, "session-1", "app-name", "user-id")  # type: ignore[call-arg]


def test_event_to_record_session_id_stored_correctly() -> None:
    """session_id in the record matches the argument passed."""
    event = _make_event(invocation_id="inv-abc", author="model")
    record = event_to_record(event, "my-session-id")
    assert record["session_id"] == "my-session-id"


def test_event_to_record_indexed_fields_match_event() -> None:
    """Indexed scalar columns (invocation_id, author, timestamp) match the source event."""
    event = _make_event(invocation_id="inv-xyz", author="tool")
    record = event_to_record(event, "s1")
    assert record["invocation_id"] == "inv-xyz"
    assert record["author"] == "tool"
    assert isinstance(record["timestamp"], datetime)


def test_event_to_record_event_json_matches_model_dump() -> None:
    """event_json in the record equals event.model_dump(exclude_none=True, mode='json')."""
    event = _make_event(text="hello", state_delta={"key": "val"}, custom_metadata={"foo": "bar"})
    record = event_to_record(event, "s1")
    expected_json = event.model_dump(exclude_none=True, mode="json")
    assert record["event_json"] == expected_json


def test_event_to_record_event_json_is_dict() -> None:
    """event_json field is a plain dict (not bytes, not string)."""
    event = _make_event()
    record = event_to_record(event, "s1")
    assert isinstance(record["event_json"], dict)


def test_event_to_record_actions_in_event_json_is_structured() -> None:
    """Actions are stored as structured JSON dict in event_json, not as raw bytes."""
    event = _make_event(state_delta={"x": "y"})
    record = event_to_record(event, "s1")
    event_json = record["event_json"]
    # actions should be a dict in the JSON blob
    if "actions" in event_json:
        assert isinstance(event_json["actions"], dict)


def test_event_to_record_timestamp_is_datetime() -> None:
    """timestamp column is a datetime object with timezone."""
    event = _make_event()
    record = event_to_record(event, "s1")
    assert isinstance(record["timestamp"], datetime)
    assert record["timestamp"].tzinfo is not None


# ---------------------------------------------------------------------------
# record_to_event — full round-trip fidelity
# ---------------------------------------------------------------------------


def test_record_to_event_full_roundtrip_basic() -> None:
    """Event -> record -> Event produces an identical object for basic fields."""
    original = _make_event(event_id="evt-rt", invocation_id="inv-rt", author="model")
    record = event_to_record(original, "s1")
    restored = record_to_event(record)

    assert restored.id == original.id
    assert restored.invocation_id == original.invocation_id
    assert restored.author == original.author


def test_record_to_event_roundtrip_preserves_content() -> None:
    """Content (parts) survives the round-trip."""
    original = _make_event(text="hello world", author="model")
    record = event_to_record(original, "s1")
    restored = record_to_event(record)

    assert restored.content is not None
    assert restored.content.parts is not None
    assert restored.content.parts[0].text == "hello world"


def test_record_to_event_roundtrip_preserves_actions() -> None:
    """EventActions (state_delta) survives the round-trip."""
    original = _make_event(state_delta={"key": "v1", "other": 42})
    record = event_to_record(original, "s1")
    restored = record_to_event(record)

    assert restored.actions is not None
    assert restored.actions.state_delta == {"key": "v1", "other": 42}


def test_record_to_event_roundtrip_preserves_custom_metadata() -> None:
    """custom_metadata survives the round-trip."""
    original = _make_event(custom_metadata={"tag": "v2", "score": 0.9})
    record = event_to_record(original, "s1")
    restored = record_to_event(record)

    assert restored.custom_metadata == {"tag": "v2", "score": 0.9}


def test_record_to_event_roundtrip_preserves_branch() -> None:
    """branch field survives the round-trip."""
    original = _make_event(branch="feature-branch")
    record = event_to_record(original, "s1")
    restored = record_to_event(record)

    assert restored.branch == "feature-branch"


def test_record_to_event_roundtrip_preserves_partial_flag() -> None:
    """partial flag survives the round-trip."""
    original = _make_event(partial=True)
    record = event_to_record(original, "s1")
    restored = record_to_event(record)

    assert restored.partial is True


def test_record_to_event_roundtrip_preserves_turn_complete() -> None:
    """turn_complete flag survives the round-trip."""
    original = _make_event(turn_complete=True)
    record = event_to_record(original, "s1")
    restored = record_to_event(record)

    assert restored.turn_complete is True


def test_record_to_event_roundtrip_preserves_timestamp() -> None:
    """timestamp survives the round-trip within float precision."""
    fixed_ts = datetime(2024, 6, 1, 10, 30, 0, tzinfo=timezone.utc).timestamp()
    event = Event(
        id="ts-evt",
        invocation_id="inv-1",
        author="user",
        actions=EventActions(),
        timestamp=fixed_ts,
    )
    record = event_to_record(event, "s1")
    restored = record_to_event(record)

    assert abs(restored.timestamp - fixed_ts) < 1.0  # within 1 second


@pytest.mark.xfail(
    reason="ADK Event model uses extra='forbid' — unknown fields raise ValidationError. "
    "Future ADK versions that add fields will also update the model, so this is safe.",
    strict=True,
)
def test_record_to_event_with_extra_fields_in_event_json() -> None:
    """Events with extra/unknown fields in event_json are rejected by Event model."""
    event = _make_event(event_id="extra-fields-evt", author="tool")
    record = event_to_record(event, "s1")

    # Inject hypothetical future ADK field into event_json
    record["event_json"]["hypothetical_v3_field"] = "some_value"  # type: ignore[index]

    # This WILL raise because Event has extra='forbid'
    restored = record_to_event(record)
    assert restored.id == "extra-fields-evt"


# ---------------------------------------------------------------------------
# session_to_record — strips temp: keys
# ---------------------------------------------------------------------------


def test_session_to_record_strips_temp_keys_from_state() -> None:
    """session_to_record removes temp:-prefixed keys before persisting."""
    session = _make_session(state={"key": "v", "temp:x": "t", "app:y": "a"})
    record = session_to_record(session)
    assert "temp:x" not in record["state"]
    assert record["state"]["key"] == "v"
    assert record["state"]["app:y"] == "a"


def test_session_to_record_empty_state_stays_empty() -> None:
    """Empty state produces empty state in record."""
    session = _make_session(state={})
    record = session_to_record(session)
    assert record["state"] == {}


def test_session_to_record_all_temp_state_produces_empty() -> None:
    """Session state with only temp: keys produces empty state in record."""
    session = _make_session(state={"temp:a": 1, "temp:b": 2})
    record = session_to_record(session)
    assert record["state"] == {}


def test_session_to_record_no_temp_state_unchanged() -> None:
    """Session state with no temp: keys is stored without modification."""
    state = {"x": 1, "app:y": 2, "user:z": 3}
    session = _make_session(state=state)
    record = session_to_record(session)
    assert record["state"] == state


def test_session_to_record_includes_required_fields() -> None:
    """Session record includes id, app_name, user_id, state, create_time, update_time."""
    session = _make_session()
    record = session_to_record(session)
    assert "id" in record
    assert "app_name" in record
    assert "user_id" in record
    assert "state" in record
    assert "create_time" in record
    assert "update_time" in record


# ---------------------------------------------------------------------------
# record_to_session — integrates with record_to_event
# ---------------------------------------------------------------------------


def test_record_to_session_with_events_round_trip() -> None:
    """Sessions with events reconstruct correctly using record_to_session."""
    from sqlspec.extensions.adk._types import SessionRecord

    session_record = SessionRecord(
        id="s1",
        app_name="app",
        user_id="u1",
        state={"key": "val"},
        create_time=datetime.now(timezone.utc),
        update_time=datetime.now(timezone.utc),
    )
    event = _make_event(text="hello", author="user")
    event_record = event_to_record(event, "s1")

    session = record_to_session(session_record, [event_record])

    assert session.id == "s1"
    assert session.app_name == "app"
    assert session.user_id == "u1"
    assert session.state == {"key": "val"}
    assert len(session.events) == 1
    assert session.events[0].id == event.id


def test_record_to_session_empty_events() -> None:
    """Sessions without events reconstruct with empty events list."""
    from sqlspec.extensions.adk._types import SessionRecord

    session_record = SessionRecord(
        id="s2",
        app_name="app",
        user_id="u2",
        state={},
        create_time=datetime.now(timezone.utc),
        update_time=datetime.now(timezone.utc),
    )
    session = record_to_session(session_record, [])
    assert session.events == []
