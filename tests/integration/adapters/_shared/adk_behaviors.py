"""Behavior helpers for shared ADK session/event store contract tests."""

from collections.abc import Awaitable
from datetime import datetime, timedelta, timezone
from inspect import isawaitable
from typing import Any, TypeVar

from sqlspec.extensions.adk import StoredEvent

T = TypeVar("T")


async def _resolve(result: T | Awaitable[T]) -> T:
    if isawaitable(result):
        return await result
    return result


async def _aclose(config: Any) -> None:
    await _resolve(config.close_pool())


def _event(app_name: str, user_id: str, session_id: str, index: int, when: datetime) -> StoredEvent:
    return {
        "id": f"event-{session_id}-{index}",
        "app_name": app_name,
        "user_id": user_id,
        "session_id": session_id,
        "invocation_id": f"inv-{index}",
        "timestamp": when,
        "event_data": {
            "id": f"event-{session_id}-{index}",
            "invocation_id": f"inv-{index}",
            "author": "user",
            "timestamp": when.timestamp(),
            "content": {"parts": [{"text": f"hello-{index}"}]},
        },
    }


async def assert_adk_create_tables_idempotent_contract(make_store: Any) -> None:
    """Creating ADK tables twice is a safe no-op."""
    config, store = make_store()
    try:
        await _resolve(store.create_tables())
        await _resolve(store.create_tables())
        assert await _resolve(store.get_session("app", "user", "missing")) is None
    finally:
        await _aclose(config)


async def assert_adk_session_round_trip_contract(make_store: Any) -> None:
    """Sessions persist empty and populated state as JSON through a round-trip."""
    config, store = make_store()
    try:
        await _resolve(store.create_tables())
        empty = await _resolve(store.create_session("session-empty", "app", "user", {}))
        assert empty["state"] == {}

        created = await _resolve(
            store.create_session("session-state", "app", "user", {"turn": 1, "nested": {"a": [1, 2]}})
        )
        fetched = await _resolve(store.get_session("app", "user", "session-state"))
        assert created["state"] == {"turn": 1, "nested": {"a": [1, 2]}}
        assert fetched is not None
        assert fetched["state"] == {"turn": 1, "nested": {"a": [1, 2]}}
    finally:
        await _aclose(config)


async def assert_adk_get_nonexistent_session_contract(make_store: Any) -> None:
    """Reading a missing session returns None."""
    config, store = make_store()
    try:
        await _resolve(store.create_tables())
        assert await _resolve(store.get_session("app", "user", "nope")) is None
    finally:
        await _aclose(config)


async def assert_adk_update_session_state_contract(make_store: Any) -> None:
    """update_session_state replaces the durable session state."""
    config, store = make_store()
    try:
        await _resolve(store.create_tables())
        await _resolve(store.create_session("session-update", "app", "user", {"count": 0}))
        await _resolve(store.update_session_state("app", "user", "session-update", {"count": 5}))
        fetched = await _resolve(store.get_session("app", "user", "session-update"))
        assert fetched is not None
        assert fetched["state"] == {"count": 5}
    finally:
        await _aclose(config)


async def assert_adk_list_sessions_contract(make_store: Any) -> None:
    """list_sessions filters by app and optional user for tenant isolation."""
    config, store = make_store()
    try:
        await _resolve(store.create_tables())
        await _resolve(store.create_session("s1", "app", "user-a", {}))
        await _resolve(store.create_session("s2", "app", "user-a", {}))
        await _resolve(store.create_session("s3", "app", "user-b", {}))
        await _resolve(store.create_session("s4", "other", "user-a", {}))

        app_sessions = await _resolve(store.list_sessions("app"))
        assert {row["id"] for row in app_sessions} == {"s1", "s2", "s3"}

        user_sessions = await _resolve(store.list_sessions("app", "user-a"))
        assert {row["id"] for row in user_sessions} == {"s1", "s2"}

        assert await _resolve(store.list_sessions("empty-app")) == []
    finally:
        await _aclose(config)


async def assert_adk_delete_session_cascade_contract(make_store: Any) -> None:
    """Deleting a session removes the session and its events."""
    config, store = make_store()
    try:
        await _resolve(store.create_tables())
        session_id = "session-delete"
        await _resolve(store.create_session(session_id, "app", "user", {}))
        await _resolve(
            store.append_event(_event("app", "user", session_id, 0, datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)))
        )

        await _resolve(store.delete_session("app", "user", session_id))
        assert await _resolve(store.get_session("app", "user", session_id)) is None
        assert await _resolve(store.get_events("app", "user", session_id)) == []
    finally:
        await _aclose(config)


async def assert_adk_append_and_get_events_contract(make_store: Any) -> None:
    """Appended events round-trip through get_events."""
    config, store = make_store()
    try:
        await _resolve(store.create_tables())
        session_id = "session-events"
        await _resolve(store.create_session(session_id, "app", "user", {}))
        await _resolve(
            store.append_event(_event("app", "user", session_id, 1, datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)))
        )

        events = await _resolve(store.get_events("app", "user", session_id))
        assert len(events) == 1
        assert events[0]["id"] == f"event-{session_id}-1"
        assert events[0]["app_name"] == "app"
        assert events[0]["user_id"] == "user"
        assert events[0]["invocation_id"] == "inv-1"
        assert events[0]["event_data"]["content"] == {"parts": [{"text": "hello-1"}]}
    finally:
        await _aclose(config)


async def assert_adk_append_event_and_update_state_contract(make_store: Any) -> None:
    """Atomic append updates state and stores the event in one round-trip."""
    config, store = make_store()
    try:
        await _resolve(store.create_tables())
        session_id = "session-atomic"
        await _resolve(store.create_session(session_id, "app", "user", {}))
        updated = await _resolve(
            store.append_event_and_update_state(
                _event("app", "user", session_id, 1, datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)),
                "app",
                "user",
                session_id,
                {"turn": 1},
                app_state={"app:theme": "dark"},
                user_state={"user:locale": "en-US"},
            )
        )
        assert updated["state"] == {"turn": 1}

        session = await _resolve(store.get_session("app", "user", session_id))
        events = await _resolve(store.get_events("app", "user", session_id))
        assert session is not None
        assert session["state"] == {"turn": 1}
        assert len(events) == 1
        assert events[0]["invocation_id"] == "inv-1"
        assert await _resolve(store.get_app_state("app")) == {"app:theme": "dark"}
        assert await _resolve(store.get_user_state("app", "user")) == {"user:locale": "en-US"}
    finally:
        await _aclose(config)


async def assert_adk_get_events_filtering_contract(make_store: Any) -> None:
    """get_events honors after_timestamp and limit."""
    config, store = make_store()
    try:
        await _resolve(store.create_tables())
        session_id = "session-filter"
        await _resolve(store.create_session(session_id, "app", "user", {}))
        base = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)
        for index in range(3):
            await _resolve(
                store.append_event(_event("app", "user", session_id, index, base + timedelta(seconds=index)))
            )

        events = await _resolve(
            store.get_events("app", "user", session_id, after_timestamp=base + timedelta(milliseconds=500), limit=1)
        )
        assert len(events) == 1
        assert events[0]["invocation_id"] == "inv-1"

        assert await _resolve(store.get_events("app", "user", session_id, limit=0)) == []
    finally:
        await _aclose(config)


async def assert_adk_reads_empty_when_tables_missing_contract(make_store: Any) -> None:
    """Read paths return None/empty when the ADK tables do not exist."""
    config, store = make_store()
    try:
        assert await _resolve(store.get_session("app", "user", "missing")) is None
        assert await _resolve(store.list_sessions("app")) == []
        assert await _resolve(store.list_sessions("app", "user")) == []
        assert await _resolve(store.get_events("app", "user", "session-x")) == []
        assert await _resolve(store.get_app_state("app")) is None
        assert await _resolve(store.get_user_state("app", "user")) is None
    finally:
        await _aclose(config)


_LIST_ORDER_SESSION_IDS = ("s-c", "s-a", "s-d", "s-b")
"""Creation order deliberately differs from id order so both sort keys are observable."""


async def _seed_list_order_sessions(store: Any) -> None:
    """Create the ordering fixture: four sessions for user-a and one each for the other scopes."""
    await _resolve(store.create_tables())
    for session_id in _LIST_ORDER_SESSION_IDS:
        await _resolve(store.create_session(session_id, "app", "user-a", {}))
    await _resolve(store.create_session("s-other-user", "app", "user-b", {}))
    await _resolve(store.create_session("s-other-app", "other", "user-a", {}))


async def _session_ids(store: Any, **options: Any) -> "list[str]":
    rows = await _resolve(store.list_sessions("app", "user-a", **options))
    return [row["id"] for row in rows]


async def assert_adk_list_sessions_default_order_contract(make_store: Any) -> None:
    """The default listing stays recent-first and unbounded."""
    config, store = make_store()
    try:
        await _seed_list_order_sessions(store)

        rows = await _resolve(store.list_sessions("app", "user-a"))
        update_times = [row["update_time"] for row in rows]

        assert {row["id"] for row in rows} == set(_LIST_ORDER_SESSION_IDS)
        assert update_times == sorted(update_times, reverse=True)
        assert [row["id"] for row in rows] == await _session_ids(store, order_by="update_time", descending=True)
    finally:
        await _aclose(config)


async def assert_adk_list_sessions_ordering_contract(make_store: Any) -> None:
    """Both order columns sort in both directions and stay a stable total order."""
    config, store = make_store()
    try:
        await _seed_list_order_sessions(store)

        for order_by in ("create_time", "update_time"):
            ascending = await _session_ids(store, order_by=order_by, descending=False)
            descending = await _session_ids(store, order_by=order_by, descending=True)

            assert sorted(ascending) == sorted(_LIST_ORDER_SESSION_IDS)
            assert descending == list(reversed(ascending))
            assert ascending == await _session_ids(store, order_by=order_by, descending=False)
    finally:
        await _aclose(config)


async def assert_adk_list_sessions_paging_contract(make_store: Any) -> None:
    """Finite pages partition the ordered listing without overlaps or gaps."""
    config, store = make_store()
    try:
        await _seed_list_order_sessions(store)

        for order_by in ("create_time", "update_time"):
            for descending in (True, False):
                options = {"order_by": order_by, "descending": descending}
                everything = await _session_ids(store, **options)
                first = await _session_ids(store, limit=2, offset=0, **options)
                second = await _session_ids(store, limit=2, offset=2, **options)

                assert first + second == everything
                assert not set(first) & set(second)
                assert await _session_ids(store, limit=2, offset=len(everything), **options) == []
                assert await _session_ids(store, limit=10, offset=1, **options) == everything[1:]
    finally:
        await _aclose(config)


async def assert_adk_list_sessions_filtering_precedes_paging_contract(make_store: Any) -> None:
    """User and app filtering narrow the result set before the page is taken."""
    config, store = make_store()
    try:
        await _seed_list_order_sessions(store)

        app_page = await _resolve(store.list_sessions("app", limit=10, offset=0))
        user_page = await _resolve(store.list_sessions("app", "user-b", limit=10, offset=0))

        assert {row["id"] for row in app_page} == {*_LIST_ORDER_SESSION_IDS, "s-other-user"}
        assert [row["id"] for row in user_page] == ["s-other-user"]
        assert await _resolve(store.list_sessions("app", "user-b", limit=1, offset=1)) == []
    finally:
        await _aclose(config)


async def assert_adk_list_sessions_option_validation_contract(make_store: Any) -> None:
    """A zero limit answers empty and invalid options raise before any query runs."""
    config, store = make_store()
    try:
        await _seed_list_order_sessions(store)

        assert await _resolve(store.list_sessions("app", "user-a", limit=0)) == []
        assert await _resolve(store.list_sessions("app", "user-a", limit=0, offset=2)) == []

        for options in (
            {"order_by": "id"},
            {"order_by": "state"},
            {"limit": -1},
            {"offset": -1, "limit": 5},
            {"limit": True},
            {"offset": False, "limit": 5},
            {"offset": 1},
        ):
            try:
                await _resolve(store.list_sessions("app", "user-a", **options))
            except ValueError:
                continue
            raise AssertionError(f"list_sessions accepted invalid options: {options}")
    finally:
        await _aclose(config)
