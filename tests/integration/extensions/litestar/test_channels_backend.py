import asyncio
import tempfile
from types import SimpleNamespace
from typing import Any, cast

import msgspec.json
import pytest
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


class _StreamingEventChannel:
    def __init__(self, payloads: list[dict[str, str]]) -> None:
        self.payloads = payloads
        self.acked: list[str] = []
        self.shutdown_calls = 0

    async def iter_events(self, _channel: str, *, poll_interval: float) -> "Any":
        _ = poll_interval
        for index, payload in enumerate(self.payloads):
            yield SimpleNamespace(event_id=str(index), payload=payload)

    async def ack(self, event_id: str) -> None:
        self.acked.append(event_id)

    async def shutdown(self) -> None:
        self.shutdown_calls += 1


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

            await backend.publish_many((b"first", b"second"), ("notifications",))
            first = await asyncio.wait_for(_next_event(subscriber), timeout=3.0)
            second = await asyncio.wait_for(_next_event(subscriber), timeout=3.0)
            assert (first, second) == (b"first", b"second")

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


@pytest.mark.parametrize("capacity", [True, False, 0, -1, 1.5, "1"])
def test_litestar_channels_backend_rejects_invalid_output_capacity(capacity: object) -> None:
    with pytest.raises(ValueError, match="output_queue_capacity must be a positive integer"):
        SQLSpecChannelsBackend(cast("Any", _RecordingEventChannel()), output_queue_capacity=capacity)  # type: ignore[arg-type]


async def test_litestar_channels_backend_drops_oldest_and_preserves_acknowledgements() -> None:
    event_channel = _StreamingEventChannel([
        {"data_b64": "Zmlyc3Q="},
        {"data_b64": "c2Vjb25k"},
        {"data_b64": "dGhpcmQ="},
    ])
    backend = SQLSpecChannelsBackend(cast("Any", event_channel), output_queue_capacity=2)
    await backend.on_startup()

    await backend._stream_channel("alerts", backend._db_channel_name("alerts"))

    assert backend.output_queue_depth == 2
    assert backend.dropped_message_count == 1
    assert event_channel.acked == ["0", "1", "2"]
    stream = backend.stream_events()
    assert await anext(stream) == ("alerts", b"second")
    assert await anext(stream) == ("alerts", b"third")
    assert backend.output_queue_depth == 0

    await backend.on_shutdown()
    assert backend.output_queue_depth == 0
    assert backend.dropped_message_count == 1


async def test_litestar_channels_backend_malformed_payload_does_not_count_as_overflow() -> None:
    event_channel = _StreamingEventChannel([{"invalid": "payload"}])
    backend = SQLSpecChannelsBackend(cast("Any", event_channel), output_queue_capacity=1)
    await backend.on_startup()

    await backend._stream_channel("alerts", backend._db_channel_name("alerts"))

    assert backend.output_queue_depth == 0
    assert backend.dropped_message_count == 0
    assert event_channel.acked == ["0"]
