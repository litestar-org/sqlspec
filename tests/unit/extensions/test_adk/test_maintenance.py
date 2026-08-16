from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.extensions.adk.maintenance import (
    prune_events,
    prune_events_sync,
    prune_memory,
    prune_memory_sync,
    prune_sessions,
    prune_sessions_sync,
    prune_user_state,
    prune_user_state_sync,
)


class MockSessionStore:
    """Mock ADK session store."""

    def __init__(self) -> None:
        self.session_table = "adk_session"
        self.events_table = "adk_event"
        self.user_state_table = "adk_user_state"
        self.delete_idle_sessions = AsyncMock(return_value=5)
        self.delete_expired_events = AsyncMock(return_value=12)
        self.delete_idle_user_states = AsyncMock(return_value=3)


class MockSyncSessionStore:
    """Mock sync ADK session store."""

    def __init__(self) -> None:
        self.session_table = "adk_session"
        self.events_table = "adk_event"
        self.user_state_table = "adk_user_state"
        self.delete_idle_sessions = MagicMock(return_value=7)
        self.delete_expired_events = MagicMock(return_value=14)
        self.delete_idle_user_states = MagicMock(return_value=4)


class MockMemoryStore:
    """Mock ADK memory store."""

    def __init__(self) -> None:
        self.memory_table = "adk_memory"
        self.delete_entries_older_than = AsyncMock(return_value=8)


class MockSyncMemoryStore:
    """Mock sync ADK memory store."""

    def __init__(self) -> None:
        self.memory_table = "adk_memory"
        self.delete_entries_older_than = MagicMock(return_value=9)


async def test_prune_sessions_async() -> None:
    store = MockSessionStore()
    report = await prune_sessions(store, idle_days=45, app_name="test_app")

    assert report["deleted_count"] == 5
    assert report["table"] == "adk_session"
    assert report["elapsed_ms"] >= 0.0
    store.delete_idle_sessions.assert_awaited_once()
    assert isinstance(store.delete_idle_sessions.call_args[0][0], datetime)
    assert "app_name" in store.delete_idle_sessions.call_args.kwargs


def test_prune_sessions_sync() -> None:
    store = MockSyncSessionStore()
    report = prune_sessions_sync(store, idle_days=30)

    assert report["deleted_count"] == 7
    assert report["table"] == "adk_session"
    store.delete_idle_sessions.assert_called_once()
    assert isinstance(store.delete_idle_sessions.call_args[0][0], datetime)


async def test_prune_events_async() -> None:
    store = MockSessionStore()
    report = await prune_events(store, older_than_days=60, app_name="agent_app")

    assert report["deleted_count"] == 12
    assert report["table"] == "adk_event"
    store.delete_expired_events.assert_awaited_once()
    assert isinstance(store.delete_expired_events.call_args[0][0], datetime)
    assert "app_name" in store.delete_expired_events.call_args.kwargs


def test_prune_events_sync() -> None:
    store = MockSyncSessionStore()
    report = prune_events_sync(store, older_than_days=90)

    assert report["deleted_count"] == 14
    assert report["table"] == "adk_event"
    store.delete_expired_events.assert_called_once()
    assert isinstance(store.delete_expired_events.call_args[0][0], datetime)


async def test_prune_memory_async() -> None:
    store = MockMemoryStore()
    report = await prune_memory(store, older_than_days=30, app_name="agent_app", scope="user")

    assert report["deleted_count"] == 8
    assert report["table"] == "adk_memory"
    store.delete_entries_older_than.assert_awaited_once_with(30, app_name="agent_app", scope="user")


def test_prune_memory_sync() -> None:
    store = MockSyncMemoryStore()
    report = prune_memory_sync(store, older_than_days=45, scope="all")

    assert report["deleted_count"] == 9
    assert report["table"] == "adk_memory"
    store.delete_entries_older_than.assert_called_once_with(45, app_name=None, scope=None)


async def test_prune_user_state_async() -> None:
    store = MockSessionStore()
    report = await prune_user_state(store, idle_days=120)

    assert report["deleted_count"] == 3
    assert report["table"] == "adk_user_state"
    store.delete_idle_user_states.assert_awaited_once()
    assert isinstance(store.delete_idle_user_states.call_args[0][0], datetime)
    assert store.delete_idle_user_states.call_args.kwargs["app_name"] is None


def test_prune_user_state_sync() -> None:
    store = MockSyncSessionStore()
    report = prune_user_state_sync(store, idle_days=180, app_name="app1")

    assert report["deleted_count"] == 4
    assert report["table"] == "adk_user_state"
    store.delete_idle_user_states.assert_called_once()
    assert isinstance(store.delete_idle_user_states.call_args[0][0], datetime)
    assert store.delete_idle_user_states.call_args.kwargs["app_name"] == "app1"




def test_invalid_target_resolution() -> None:
    with pytest.raises(TypeError, match="Cannot resolve ADK session store"):
        prune_sessions_sync("invalid_target_string")

    with pytest.raises(TypeError, match="Cannot resolve ADK memory store"):
        prune_memory_sync("invalid_target_string")
