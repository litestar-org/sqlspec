"""Litestar channels backend backed by SQLSpec's EventChannel."""

import asyncio
import base64
import hashlib
import re
from typing import TYPE_CHECKING, Any

from litestar.channels.backends.base import ChannelsBackend

from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterable

    from sqlspec.extensions.events import AsyncEventChannel

logger = get_logger("sqlspec.extensions.litestar.channels")

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SQLSpecChannelsBackend(ChannelsBackend):
    """A Litestar Channels backend implemented on top of SQLSpec's EventChannel.

    This backend allows Litestar's ChannelsPlugin to use a SQLSpec database as the
    broker. Under the hood it relies on SQLSpec's events extension, which can be
    configured to use a durable table queue or native adapter backends.

    Notes:
        Litestar channels may use arbitrary string names. SQLSpec event channel
        names must be valid identifiers. This backend maps Litestar channel names
        to deterministic database channel identifiers via hashing.
    """

    def __init__(
        self, event_channel: "AsyncEventChannel", *, channel_prefix: str = "litestar", poll_interval: float = 0.2
    ) -> None:
        if not _IDENTIFIER_PATTERN.match(channel_prefix):
            msg = f"channel_prefix must be a valid identifier, got: {channel_prefix!r}"
            raise ValueError(msg)
        if poll_interval <= 0:
            msg = "poll_interval must be greater than zero"
            raise ValueError(msg)
        self._event_channel = event_channel
        self._channel_prefix = channel_prefix
        self._poll_interval = poll_interval
        self._output_queue: asyncio.Queue[tuple[str, bytes]] | None = None
        self._shutdown = asyncio.Event()
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._to_db_channel: dict[str, str] = {}
        self._to_litestar_channel: dict[str, str] = {}

    async def on_startup(self) -> None:
        self._shutdown.clear()
        if self._output_queue is None:
            self._output_queue = asyncio.Queue()

    async def on_shutdown(self) -> None:
        self._shutdown.set()
        tasks = list(self._tasks.values())
        self._tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._to_db_channel.clear()
        self._to_litestar_channel.clear()
        await self._event_channel.shutdown()

    async def publish(self, data: bytes, channels: "Iterable[str]") -> None:
        payload = {"data_b64": base64.b64encode(data).decode("ascii")}
        for channel in channels:
            db_channel = self._db_channel_name(channel)
            await self._event_channel.publish(db_channel, payload)

    async def subscribe(self, channels: "Iterable[str]") -> None:
        for channel in channels:
            if channel in self._tasks:
                continue
            db_channel = self._db_channel_name(channel)
            task = asyncio.create_task(self._stream_channel(channel, db_channel))
            self._tasks[channel] = task

    async def unsubscribe(self, channels: "Iterable[str]") -> None:
        cancelled: list[asyncio.Task[None]] = []
        for channel in channels:
            task = self._tasks.pop(channel, None)
            if task is None:
                continue
            task.cancel()
            cancelled.append(task)
        if cancelled:
            await asyncio.gather(*cancelled, return_exceptions=True)
        self._cleanup_channel_mappings()

    def stream_events(self) -> "AsyncGenerator[tuple[str, bytes], None]":
        return self._event_generator()

    async def get_history(self, channel: str, limit: int | None = None) -> list[bytes]:
        """Return history entries for a channel.

        SQLSpec's event queue is primarily designed for durable delivery, not
        for history replay. For now, this backend does not expose history.
        """

        return []

    def _cleanup_channel_mappings(self) -> None:
        active = set(self._tasks)
        removed = [name for name in self._to_db_channel if name not in active]
        for name in removed:
            db_name = self._to_db_channel.pop(name, None)
            if db_name:
                self._to_litestar_channel.pop(db_name, None)

    async def _event_generator(self) -> "AsyncGenerator[tuple[str, bytes], None]":
        if self._output_queue is None:
            msg = "SQLSpecChannelsBackend not started - call ChannelsPlugin.on_startup() first"
            raise RuntimeError(msg)
        queue = self._output_queue
        while True:
            item = await queue.get()
            yield item

    def _db_channel_name(self, channel: str) -> str:
        existing = self._to_db_channel.get(channel)
        if existing:
            return existing
        digest = hashlib.sha256(channel.encode("utf-8")).hexdigest()[:24]
        db_channel = f"{self._channel_prefix}_{digest}"
        self._to_db_channel[channel] = db_channel
        self._to_litestar_channel[db_channel] = channel
        return db_channel

    async def _stream_channel(self, channel: str, db_channel: str) -> None:
        try:
            async for message in self._event_channel.iter_events(db_channel, poll_interval=self._poll_interval):
                if self._shutdown.is_set():
                    return
                payload = message.payload
                decoded = self._decode_payload(payload)
                if decoded is None:
                    logger.warning("litestar channel %s dropped malformed payload: %r", channel, payload)
                    await self._event_channel.ack(message.event_id)
                    continue
                assert self._output_queue is not None
                await self._output_queue.put((channel, decoded))
                await self._event_channel.ack(message.event_id)
        except asyncio.CancelledError:
            raise
        except Exception as error:  # pragma: no cover - defensive
            logger.warning("litestar channel %s stream worker error: %s", channel, error)

    @staticmethod
    def _decode_payload(payload: Any) -> bytes | None:
        if not isinstance(payload, dict):
            return None
        encoded = payload.get("data_b64")
        if not isinstance(encoded, str) or not encoded:
            return None
        try:
            return base64.b64decode(encoded.encode("ascii"))
        except (ValueError, UnicodeEncodeError):
            return None
