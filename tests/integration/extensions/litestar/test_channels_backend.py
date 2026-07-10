import asyncio
import tempfile
from typing import Any, cast

import msgspec.json
from litestar.channels.plugin import ChannelsPlugin

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.extensions.events import AsyncEventChannel
from sqlspec.extensions.litestar.channels import SQLSpecChannelsBackend
from sqlspec.migrations.commands import AsyncMigrationCommands


async def _next_event(subscriber: "Any") -> bytes:
    async for event in subscriber.iter_events():
        return cast("bytes", event)
    msg = "Subscriber stopped without yielding an event"
    raise RuntimeError(msg)


class _RecordingEventChannel:
    def __init__(self) -> None:
        self.batches: list[list[tuple[str, dict[str, str], None]]] = []

    async def publish_many(self, events: "list[tuple[str, dict[str, str], None]]") -> list[str]:
        self.batches.append(events)
        return [f"event-{index}" for index in range(len(events))]


async def test_litestar_channels_backend_database_roundtrip(tmp_path: "Any") -> None:
    migrations = tmp_path / "migrations"
    migrations.mkdir()

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        config = AiosqliteConfig(
            connection_config={"database": tmp.name},
            migration_config={"script_location": str(migrations), "include_extensions": ["events"]},
            extension_config={"events": {}},
        )

        commands = AsyncMigrationCommands(config)
        await commands.upgrade("head")

        backend = SQLSpecChannelsBackend(AsyncEventChannel(config), channel_prefix="litestar", poll_interval=0.05)
        plugin = ChannelsPlugin(backend=backend, channels=["notifications"])

        async with plugin:
            subscriber = await plugin.subscribe("notifications")
            await plugin.wait_published({"action": "hello"}, "notifications")

            payload = await asyncio.wait_for(_next_event(subscriber), timeout=3.0)
            decoded = msgspec.json.decode(payload)
            assert decoded["action"] == "hello"

            await plugin.unsubscribe(subscriber)

        await config.close_pool()


async def test_litestar_channels_backend_groups_multi_channel_publish() -> None:
    event_channel = _RecordingEventChannel()
    backend = SQLSpecChannelsBackend(cast("Any", event_channel), channel_prefix="litestar")

    await backend.publish(b"payload", (channel for channel in ("alpha", "beta", "gamma")))

    assert len(event_channel.batches) == 1
    assert len(event_channel.batches[0]) == 3
    assert [event[0] for event in event_channel.batches[0]] == [
        backend._db_channel_name("alpha"),
        backend._db_channel_name("beta"),
        backend._db_channel_name("gamma"),
    ]
    assert {event[1]["data_b64"] for event in event_channel.batches[0]} == {"cGF5bG9hZA=="}


async def test_litestar_channels_backend_groups_multiple_payloads_and_channels() -> None:
    event_channel = _RecordingEventChannel()
    backend = SQLSpecChannelsBackend(cast("Any", event_channel), channel_prefix="litestar")

    await backend.publish_many((b"first", b"second"), (channel for channel in ("alpha", "beta")))

    assert len(event_channel.batches) == 1
    assert [(event[0], event[1]["data_b64"]) for event in event_channel.batches[0]] == [
        (backend._db_channel_name("alpha"), "Zmlyc3Q="),
        (backend._db_channel_name("beta"), "Zmlyc3Q="),
        (backend._db_channel_name("alpha"), "c2Vjb25k"),
        (backend._db_channel_name("beta"), "c2Vjb25k"),
    ]
