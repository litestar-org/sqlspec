# pyright: reportPrivateUsage=false, reportAttributeAccessIssue=false
"""Regression tests for event channel nack capability guards."""

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import AsyncEventChannel, SyncEventChannel


class _SyncBackend:
    backend_name = "sync-test"

    def __init__(self, *, supports_sync: bool) -> None:
        self.supports_sync = supports_sync
        self.nacked: list[str] = []

    def nack(self, event_id: str) -> None:
        self.nacked.append(event_id)


class _AsyncBackend:
    backend_name = "async-test"

    def __init__(self, *, supports_async: bool) -> None:
        self.supports_async = supports_async
        self.nacked: list[str] = []

    async def nack(self, event_id: str) -> None:
        self.nacked.append(event_id)


def test_sync_event_channel_nack_rejects_backend_without_sync_support(tmp_path) -> None:
    channel = SyncEventChannel(SqliteConfig(connection_config={"database": str(tmp_path / "events.db")}))
    channel._backend = _SyncBackend(supports_sync=False)  # type: ignore[assignment]

    with pytest.raises(ImproperConfigurationError, match="does not support sync nack"):
        channel.nack("event-1")


def test_sync_event_channel_nack_delegates_when_supported(tmp_path) -> None:
    backend = _SyncBackend(supports_sync=True)
    channel = SyncEventChannel(SqliteConfig(connection_config={"database": str(tmp_path / "events.db")}))
    channel._backend = backend  # type: ignore[assignment]

    channel.nack("event-1")

    assert backend.nacked == ["event-1"]


async def test_async_event_channel_nack_rejects_backend_without_async_support(tmp_path) -> None:
    channel = AsyncEventChannel(AiosqliteConfig(connection_config={"database": str(tmp_path / "events.db")}))
    channel._backend = _AsyncBackend(supports_async=False)  # type: ignore[assignment]

    with pytest.raises(ImproperConfigurationError, match="does not support async nack"):
        await channel.nack("event-1")


async def test_async_event_channel_nack_delegates_when_supported(tmp_path) -> None:
    backend = _AsyncBackend(supports_async=True)
    channel = AsyncEventChannel(AiosqliteConfig(connection_config={"database": str(tmp_path / "events.db")}))
    channel._backend = backend  # type: ignore[assignment]

    await channel.nack("event-1")

    assert backend.nacked == ["event-1"]
