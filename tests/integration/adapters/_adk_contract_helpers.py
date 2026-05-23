"""Shared acceptance helpers for ADK adapter integration tests."""

from datetime import datetime, timedelta, timezone
from typing import Protocol
from uuid import uuid4

from sqlspec.extensions.adk import EventRecord, MemoryRecord, SessionRecord

__all__ = (
    "assert_memory_store_contract",
    "assert_session_event_cleanup_contract",
    "assert_session_event_store_contract",
)


class SessionEventStore(Protocol):
    """Minimal ADK session/event store surface used by contract tests."""

    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: dict[str, object], owner_id: object | None = None
    ) -> SessionRecord: ...

    async def get_session(self, session_id: str) -> SessionRecord | None: ...

    async def update_session_state(self, session_id: str, state: dict[str, object]) -> None: ...

    async def list_sessions(self, app_name: str, user_id: str | None = None) -> list[SessionRecord]: ...

    async def delete_session(self, session_id: str) -> None: ...

    async def append_event(self, event_record: EventRecord) -> None: ...

    async def append_event_and_update_state(
        self, event_record: EventRecord, session_id: str, state: dict[str, object]
    ) -> SessionRecord: ...

    async def get_events(
        self, session_id: str, after_timestamp: datetime | None = None, limit: int | None = None
    ) -> list[EventRecord]: ...

    async def delete_expired_events(self, before: datetime) -> int: ...

    async def delete_idle_sessions(self, updated_before: datetime) -> int: ...


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
