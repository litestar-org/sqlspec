import asyncio
import tempfile
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


@pytest.mark.asyncio
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
