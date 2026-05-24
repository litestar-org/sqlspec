"""Shared acceptance helpers for ADK adapter integration tests."""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Protocol
from uuid import uuid4

from sqlspec.extensions.adk import EventRecord, MemoryRecord, SessionRecord
from sqlspec.extensions.adk.service import SQLSpecSessionService

__all__ = (
    "assert_memory_store_contract",
    "assert_session_atomic_scoped_write_contract",
    "assert_session_empty_state_roundtrip",
    "assert_session_event_cleanup_contract",
    "assert_session_event_store_contract",
    "assert_session_get_session_renewal_contract",
    "assert_session_scoped_state_contract",
    "assert_session_sibling_app_isolation",
    "assert_session_sibling_user_isolation",
    "assert_session_table_lifecycle_contract",
    "assert_session_temp_state_not_persisted",
)


class SessionEventStore(Protocol):
    """Minimal ADK session/event store surface used by contract tests."""

    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: dict[str, object], owner_id: object | None = None
    ) -> SessionRecord: ...

    async def get_session(
        self, session_id: str, *, renew_for: int | timedelta | None = None
    ) -> SessionRecord | None: ...

    async def update_session_state(self, session_id: str, state: dict[str, object]) -> None: ...

    async def list_sessions(self, app_name: str, user_id: str | None = None) -> list[SessionRecord]: ...

    async def delete_session(self, session_id: str) -> None: ...

    async def append_event(self, event_record: EventRecord) -> None: ...

    async def append_event_and_update_state(
        self,
        event_record: EventRecord,
        session_id: str,
        state: dict[str, object],
        *,
        app_name: str | None = None,
        user_id: str | None = None,
        app_state: dict[str, object] | None = None,
        user_state: dict[str, object] | None = None,
    ) -> SessionRecord: ...

    async def get_events(
        self, session_id: str, after_timestamp: datetime | None = None, limit: int | None = None
    ) -> list[EventRecord]: ...

    async def delete_expired_events(self, before: datetime) -> int: ...

    async def delete_idle_sessions(self, updated_before: datetime) -> int: ...

    async def get_app_state(self, app_name: str) -> dict[str, object] | None: ...

    async def get_user_state(self, app_name: str, user_id: str) -> dict[str, object] | None: ...

    async def upsert_app_state(self, app_name: str, state: dict[str, object]) -> None: ...

    async def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, object]) -> None: ...

    async def drop_tables(self) -> None: ...

    async def recreate_tables(self) -> None: ...


class MemoryStore(Protocol):
    """Minimal ADK memory store surface used by contract tests."""

    async def insert_memory_entries(self, entries: list[MemoryRecord], owner_id: object | None = None) -> int: ...

    async def search_entries(
        self, query: str, app_name: str, user_id: str, limit: int | None = None
    ) -> list[MemoryRecord]: ...

    async def delete_entries_by_session(self, session_id: str) -> int: ...

    async def delete_entries_older_than(self, days: int) -> int: ...


def _contract_key(marker: str, suffix: str) -> str:
    return f"adk-contract-{marker}-{suffix}-{uuid4().hex}"


def _event_record(
    *,
    session_id: str,
    event_id: str,
    invocation_id: str,
    author: str,
    timestamp: datetime,
    event_data: dict[str, object],
) -> EventRecord:
    data = dict(event_data)
    data.setdefault("id", event_id)
    return {
        "session_id": session_id,
        "invocation_id": invocation_id,
        "author": author,
        "timestamp": timestamp,
        "event_data": data,
    }


def _memory_record(
    *,
    marker: str,
    session_id: str,
    app_name: str,
    user_id: str,
    event_id: str,
    content_text: str,
    inserted_at: datetime,
    metadata_json: dict[str, object] | None = None,
) -> MemoryRecord:
    return {
        "id": _contract_key(marker, "memory"),
        "session_id": session_id,
        "app_name": app_name,
        "user_id": user_id,
        "event_id": event_id,
        "author": "user",
        "timestamp": inserted_at,
        "content_json": {"parts": [{"text": content_text}]},
        "content_text": content_text,
        "metadata_json": metadata_json,
        "inserted_at": inserted_at,
    }


def _event_data(record: EventRecord) -> dict[str, object]:
    value = record["event_data"]
    assert isinstance(value, dict)
    return value


def _as_utc(value: datetime | str) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


async def assert_session_event_store_contract(store: SessionEventStore, *, marker: str) -> None:
    """Assert the shared ADK session/event store acceptance contract.

    Backend-specific integration tests call this helper after creating tables.
    It keeps core session/event expectations identical across async and
    sync-driver-backed stores.
    """
    app_name = _contract_key(marker, "app")
    user_id = _contract_key(marker, "user")
    session_id = _contract_key(marker, "session")
    base_time = datetime(2026, 5, 23, 12, 0, tzinfo=timezone.utc)

    created = await store.create_session(session_id, app_name, user_id, {"created": True})
    assert created["id"] == session_id
    assert created["app_name"] == app_name
    assert created["user_id"] == user_id
    assert created["state"] == {"created": True}

    first_event = _event_record(
        session_id=session_id,
        event_id="contract-event-1",
        invocation_id="contract-inv-1",
        author="user",
        timestamp=base_time,
        event_data={
            "output": {"kind": "text", "value": "captured by full-event JSON"},
            "node_info": {"node_path": ["root", "agent"]},
            "actions": {
                "route": "next",
                "request_task": {"id": "task-1"},
                "finish_task": {"id": "task-1"},
                "state_delta": {"turn": 1},
            },
        },
    )
    updated = await store.append_event_and_update_state(first_event, session_id, {"turn": 1})
    assert updated["id"] == session_id
    assert updated["state"] == {"turn": 1}

    fetched = await store.get_session(session_id)
    assert fetched is not None
    assert fetched["state"] == {"turn": 1}

    stored_events = await store.get_events(session_id)
    assert len(stored_events) == 1
    assert stored_events[0]["invocation_id"] == "contract-inv-1"
    first_data = _event_data(stored_events[0])
    assert first_data["output"] == {"kind": "text", "value": "captured by full-event JSON"}
    assert first_data["node_info"] == {"node_path": ["root", "agent"]}
    assert first_data["actions"] == {
        "route": "next",
        "request_task": {"id": "task-1"},
        "finish_task": {"id": "task-1"},
        "state_delta": {"turn": 1},
    }

    await store.append_event(
        _event_record(
            session_id=session_id,
            event_id="contract-event-2",
            invocation_id="contract-inv-2",
            author="model",
            timestamp=base_time + timedelta(seconds=1),
            event_data={"content": {"parts": [{"text": "second"}]}},
        )
    )
    await store.append_event(
        _event_record(
            session_id=session_id,
            event_id="contract-event-3",
            invocation_id="contract-inv-3",
            author="model",
            timestamp=base_time + timedelta(seconds=2),
            event_data={"content": {"parts": [{"text": "third"}]}},
        )
    )

    filtered = await store.get_events(session_id, after_timestamp=base_time + timedelta(milliseconds=500), limit=1)
    assert [event["invocation_id"] for event in filtered] == ["contract-inv-2"]

    listed = await store.list_sessions(app_name, user_id)
    assert any(record["id"] == session_id for record in listed)

    await store.delete_session(session_id)
    assert await store.get_session(session_id) is None
    assert await store.get_events(session_id) == []


async def assert_session_get_session_renewal_contract(store: SessionEventStore, *, marker: str) -> None:
    """Assert get_session can renew/touch update_time while reading."""
    app_name = _contract_key(marker, "renew-app")
    user_id = _contract_key(marker, "renew-user")
    session_id = _contract_key(marker, "renew-session")

    created = await store.create_session(session_id, app_name, user_id, {"renew": True})
    original_update_time = _as_utc(created["update_time"])
    await asyncio.sleep(0.02)

    before_renewal = datetime.now(timezone.utc) - timedelta(seconds=2)
    renewed = await store.get_session(session_id, renew_for=timedelta(hours=1))
    after_renewal = datetime.now(timezone.utc) + timedelta(seconds=2)

    assert renewed is not None
    renewed_update_time = _as_utc(renewed["update_time"])
    assert renewed_update_time > original_update_time
    assert before_renewal <= renewed_update_time <= after_renewal
    assert renewed["state"] == {"renew": True}


async def assert_session_scoped_state_contract(store: SessionEventStore, *, marker: str) -> None:
    """Assert service-level app:/user:/temp: semantics over a real store."""
    from google.adk.events.event import Event
    from google.adk.events.event_actions import EventActions

    service = SQLSpecSessionService(store)  # type: ignore[arg-type]
    app_name = _contract_key(marker, "scoped-app")
    user_id = _contract_key(marker, "scoped-user")
    other_user_id = _contract_key(marker, "scoped-other-user")

    session_a = await service.create_session(
        app_name=app_name,
        user_id=user_id,
        session_id=_contract_key(marker, "scoped-session-a"),
        state={"session_seed": "a", "temp:create": "drop"},
    )
    session_b = await service.create_session(
        app_name=app_name, user_id=user_id, session_id=_contract_key(marker, "scoped-session-b"), state={}
    )
    other_user_session = await service.create_session(
        app_name=app_name,
        user_id=other_user_id,
        session_id=_contract_key(marker, "scoped-session-other-user"),
        state={},
    )
    session_a = await service.get_session(app_name=app_name, user_id=user_id, session_id=session_a.id)
    assert session_a is not None

    event = Event(
        invocation_id=_contract_key(marker, "scoped-invocation"),
        author="user",
        timestamp=datetime.now(timezone.utc).timestamp(),
        actions=EventActions(state_delta={"app:counter": 1, "user:theme": "dark", "turn": 1, "temp:scratch": "drop"}),
    )
    await service.append_event(session_a, event)

    raw_session = await store.get_session(session_a.id)
    assert raw_session is not None
    assert raw_session["state"] == {"session_seed": "a", "turn": 1}
    assert await store.get_app_state(app_name) == {"app:counter": 1}
    assert await store.get_user_state(app_name, user_id) == {"user:theme": "dark"}

    fetched_a = await service.get_session(app_name=app_name, user_id=user_id, session_id=session_a.id)
    assert fetched_a is not None
    assert fetched_a.state == {"session_seed": "a", "turn": 1, "app:counter": 1, "user:theme": "dark"}

    fetched_b = await service.get_session(app_name=app_name, user_id=user_id, session_id=session_b.id)
    assert fetched_b is not None
    assert fetched_b.state == {"app:counter": 1, "user:theme": "dark"}

    fetched_other_user = await service.get_session(
        app_name=app_name, user_id=other_user_id, session_id=other_user_session.id
    )
    assert fetched_other_user is not None
    assert fetched_other_user.state == {"app:counter": 1}


async def assert_session_atomic_scoped_write_contract(store: SessionEventStore, *, marker: str) -> None:
    """Assert append_event_and_update_state accepts scoped-state kwargs.

    Verifies the store-level atomic write delivers events INSERT + sessions UPDATE
    + app_state UPSERT + user_state UPSERT in a single round-trip when callers
    supply ``app_state`` and ``user_state`` alongside the session state snapshot.
    """
    app_name = _contract_key(marker, "atomic-app")
    user_id = _contract_key(marker, "atomic-user")
    session_id = _contract_key(marker, "atomic-session")
    no_scope_session_id = _contract_key(marker, "atomic-no-scope-session")
    base_time = datetime(2026, 5, 24, 12, 0, tzinfo=timezone.utc)

    await store.create_session(session_id, app_name, user_id, {"initial": 0})

    event = _event_record(
        session_id=session_id,
        event_id="atomic-event-1",
        invocation_id="atomic-inv-1",
        author="user",
        timestamp=base_time,
        event_data={"actions": {"state_delta": {"app:counter": 1, "user:theme": "dark", "turn": 1}}},
    )

    updated = await store.append_event_and_update_state(
        event,
        session_id,
        {"turn": 1},
        app_name=app_name,
        user_id=user_id,
        app_state={"app:counter": 1},
        user_state={"user:theme": "dark"},
    )

    assert updated["state"] == {"turn": 1}
    assert updated["id"] == session_id
    assert await store.get_app_state(app_name) == {"app:counter": 1}
    assert await store.get_user_state(app_name, user_id) == {"user:theme": "dark"}
    stored_events = await store.get_events(session_id)
    assert any(record["invocation_id"] == "atomic-inv-1" for record in stored_events)

    await store.create_session(no_scope_session_id, app_name, user_id, {"phase": 0})
    no_scope_event = _event_record(
        session_id=no_scope_session_id,
        event_id="atomic-event-2",
        invocation_id="atomic-inv-2",
        author="model",
        timestamp=base_time + timedelta(seconds=1),
        event_data={"content": {"parts": [{"text": "no scope delta"}]}},
    )
    no_scope_update = await store.append_event_and_update_state(
        no_scope_event, no_scope_session_id, {"phase": 1}, app_name=app_name, user_id=user_id
    )
    assert no_scope_update["state"] == {"phase": 1}
    # Skipped scoped writes leave existing app/user state untouched.
    assert await store.get_app_state(app_name) == {"app:counter": 1}
    assert await store.get_user_state(app_name, user_id) == {"user:theme": "dark"}


async def assert_session_temp_state_not_persisted(store: SessionEventStore, *, marker: str) -> None:
    """Assert temp:* keys never survive a service-level append_event round-trip."""
    from google.adk.events.event import Event
    from google.adk.events.event_actions import EventActions

    service = SQLSpecSessionService(store)  # type: ignore[arg-type]
    app_name = _contract_key(marker, "temp-app")
    user_id = _contract_key(marker, "temp-user")
    session_id = _contract_key(marker, "temp-session")

    session = await service.create_session(
        app_name=app_name, user_id=user_id, session_id=session_id, state={"temp:create_seed": "drop"}
    )
    session = await service.get_session(app_name=app_name, user_id=user_id, session_id=session.id)
    assert session is not None
    event = Event(
        invocation_id=_contract_key(marker, "temp-invocation"),
        author="user",
        timestamp=datetime.now(timezone.utc).timestamp(),
        actions=EventActions(state_delta={"temp:scratch": "drop", "turn": 1}),
    )
    await service.append_event(session, event)

    raw_session = await store.get_session(session_id)
    assert raw_session is not None
    assert "temp:scratch" not in raw_session["state"]
    assert "temp:create_seed" not in raw_session["state"]
    assert raw_session["state"] == {"turn": 1}

    app_state = await store.get_app_state(app_name)
    assert app_state in (None, {})
    user_state = await store.get_user_state(app_name, user_id)
    assert user_state in (None, {})

    fetched = await service.get_session(app_name=app_name, user_id=user_id, session_id=session_id)
    assert fetched is not None
    assert "temp:scratch" not in fetched.state
    assert "temp:create_seed" not in fetched.state
    assert fetched.state == {"turn": 1}


async def assert_session_empty_state_roundtrip(store: SessionEventStore, *, marker: str) -> None:
    """Assert empty session/app/user state survives the append_event_and_update_state round-trip."""
    app_name = _contract_key(marker, "empty-app")
    user_id = _contract_key(marker, "empty-user")
    session_id = _contract_key(marker, "empty-session")
    base_time = datetime(2026, 5, 24, 14, 0, tzinfo=timezone.utc)

    created = await store.create_session(session_id, app_name, user_id, {})
    assert created["state"] == {}
    fetched = await store.get_session(session_id)
    assert fetched is not None
    assert fetched["state"] == {}

    event = _event_record(
        session_id=session_id,
        event_id="empty-event-1",
        invocation_id="empty-inv-1",
        author="user",
        timestamp=base_time,
        event_data={"content": {"parts": [{"text": "no state delta"}]}},
    )
    updated = await store.append_event_and_update_state(event, session_id, {}, app_name=app_name, user_id=user_id)
    assert updated["state"] == {}

    after = await store.get_session(session_id)
    assert after is not None
    assert after["state"] == {}

    assert (await store.get_app_state(app_name)) in (None, {})
    assert (await store.get_user_state(app_name, user_id)) in (None, {})


async def assert_session_sibling_app_isolation(store: SessionEventStore, *, marker: str) -> None:
    """Assert app:* writes are isolated per app_name across sibling sessions."""
    app_a = _contract_key(marker, "sibling-app-a")
    app_b = _contract_key(marker, "sibling-app-b")
    user_id = _contract_key(marker, "sibling-app-user")
    session_a = _contract_key(marker, "sibling-app-session-a")
    session_b = _contract_key(marker, "sibling-app-session-b")
    base_time = datetime(2026, 5, 24, 15, 0, tzinfo=timezone.utc)

    await store.create_session(session_a, app_a, user_id, {})
    await store.create_session(session_b, app_b, user_id, {})

    event = _event_record(
        session_id=session_a,
        event_id="sibling-app-event-1",
        invocation_id="sibling-app-inv-1",
        author="user",
        timestamp=base_time,
        event_data={"actions": {"state_delta": {"app:counter": 7, "turn": 1}}},
    )
    await store.append_event_and_update_state(
        event, session_a, {"turn": 1}, app_name=app_a, user_id=user_id, app_state={"app:counter": 7}
    )

    assert await store.get_app_state(app_a) == {"app:counter": 7}
    assert (await store.get_app_state(app_b)) in (None, {})


async def assert_session_sibling_user_isolation(store: SessionEventStore, *, marker: str) -> None:
    """Assert user:* writes are isolated per (app_name, user_id) across sibling sessions."""
    app_name = _contract_key(marker, "sibling-user-app")
    user_a = _contract_key(marker, "sibling-user-a")
    user_b = _contract_key(marker, "sibling-user-b")
    session_a = _contract_key(marker, "sibling-user-session-a")
    session_b = _contract_key(marker, "sibling-user-session-b")
    base_time = datetime(2026, 5, 24, 16, 0, tzinfo=timezone.utc)

    await store.create_session(session_a, app_name, user_a, {})
    await store.create_session(session_b, app_name, user_b, {})

    event = _event_record(
        session_id=session_a,
        event_id="sibling-user-event-1",
        invocation_id="sibling-user-inv-1",
        author="user",
        timestamp=base_time,
        event_data={"actions": {"state_delta": {"user:pref": "dark", "turn": 1}}},
    )
    await store.append_event_and_update_state(
        event, session_a, {"turn": 1}, app_name=app_name, user_id=user_a, user_state={"user:pref": "dark"}
    )

    assert await store.get_user_state(app_name, user_a) == {"user:pref": "dark"}
    assert (await store.get_user_state(app_name, user_b)) in (None, {})


async def assert_session_table_lifecycle_contract(store: SessionEventStore, *, marker: str) -> None:
    """Assert ADK stores can drop and recreate their managed session tables."""
    app_name = _contract_key(marker, "lifecycle-app")
    user_id = _contract_key(marker, "lifecycle-user")
    session_id = _contract_key(marker, "lifecycle-session")

    await store.create_session(session_id, app_name, user_id, {"phase": "before"})
    assert await store.get_session(session_id) is not None

    await store.recreate_tables()

    assert await store.get_session(session_id) is None
    recreated = await store.create_session(session_id, app_name, user_id, {"phase": "after"})
    assert recreated["state"] == {"phase": "after"}

    await store.drop_tables()
    assert await store.get_session(session_id) is None
    await store.drop_tables()


async def assert_session_event_cleanup_contract(store: SessionEventStore, *, marker: str) -> None:
    """Assert ADK session/event cleanup hooks remove only matching rows."""
    app_name = _contract_key(marker, "cleanup-app")
    user_id = _contract_key(marker, "cleanup-user")
    session_id = _contract_key(marker, "cleanup-session")
    old_time = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    new_time = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)

    await store.create_session(session_id, app_name, user_id, {"cleanup": True})
    await store.append_event(
        _event_record(
            session_id=session_id,
            event_id="cleanup-old-event",
            invocation_id="cleanup-old",
            author="user",
            timestamp=old_time,
            event_data={"content": {"parts": [{"text": "old"}]}},
        )
    )
    await store.append_event(
        _event_record(
            session_id=session_id,
            event_id="cleanup-new-event",
            invocation_id="cleanup-new",
            author="user",
            timestamp=new_time,
            event_data={"content": {"parts": [{"text": "new"}]}},
        )
    )

    deleted_events = await store.delete_expired_events(datetime(2026, 5, 5, tzinfo=timezone.utc))
    assert deleted_events == 1
    remaining_events = await store.get_events(session_id)
    assert [event["invocation_id"] for event in remaining_events] == ["cleanup-new"]

    deleted_sessions = await store.delete_idle_sessions(datetime(2100, 1, 1, tzinfo=timezone.utc))
    assert deleted_sessions == 1
    assert await store.get_session(session_id) is None
    assert await store.get_events(session_id) == []


async def assert_memory_store_contract(store: MemoryStore, *, marker: str) -> None:
    """Assert the shared ADK memory store acceptance contract."""
    app_name = _contract_key(marker, "app")
    user_id = _contract_key(marker, "user")
    other_user_id = _contract_key(marker, "other-user")
    session_id = _contract_key(marker, "session")
    other_session_id = _contract_key(marker, "other-session")
    now = datetime.now(timezone.utc)

    espresso = _memory_record(
        marker=marker,
        session_id=session_id,
        app_name=app_name,
        user_id=user_id,
        event_id=_contract_key(marker, "event-espresso"),
        content_text="espresso roast contract memory",
        inserted_at=now,
        metadata_json={"source": "contract", "priority": 2},
    )
    latte = _memory_record(
        marker=marker,
        session_id=session_id,
        app_name=app_name,
        user_id=user_id,
        event_id=_contract_key(marker, "event-latte"),
        content_text="latte foam contract memory",
        inserted_at=now,
    )
    other_user = _memory_record(
        marker=marker,
        session_id=other_session_id,
        app_name=app_name,
        user_id=other_user_id,
        event_id=_contract_key(marker, "event-other"),
        content_text="espresso roast contract memory",
        inserted_at=now,
    )

    inserted = await store.insert_memory_entries([espresso, latte, other_user])
    assert inserted == 3

    duplicate_count = await store.insert_memory_entries([espresso])
    assert duplicate_count == 0

    results = await store.search_entries("espresso", app_name, user_id, limit=10)
    assert len(results) == 1
    assert results[0]["event_id"] == espresso["event_id"]
    assert results[0]["metadata_json"] == {"source": "contract", "priority": 2}

    other_results = await store.search_entries("espresso", app_name, other_user_id, limit=10)
    assert len(other_results) == 1
    assert other_results[0]["event_id"] == other_user["event_id"]

    deleted_session = await store.delete_entries_by_session(session_id)
    assert deleted_session == 2
    assert await store.search_entries("latte", app_name, user_id, limit=10) == []

    old_record = _memory_record(
        marker=marker,
        session_id=_contract_key(marker, "old-session"),
        app_name=app_name,
        user_id=user_id,
        event_id=_contract_key(marker, "event-old"),
        content_text="old contract memory",
        inserted_at=now - timedelta(days=40),
    )
    fresh_record = _memory_record(
        marker=marker,
        session_id=_contract_key(marker, "fresh-session"),
        app_name=app_name,
        user_id=user_id,
        event_id=_contract_key(marker, "event-fresh"),
        content_text="fresh contract memory",
        inserted_at=now,
    )
    assert await store.insert_memory_entries([old_record, fresh_record]) == 2

    deleted_old = await store.delete_entries_older_than(30)
    assert deleted_old == 1
    fresh_results = await store.search_entries("fresh", app_name, user_id, limit=10)
    assert len(fresh_results) == 1
    assert fresh_results[0]["event_id"] == fresh_record["event_id"]
