"""Persistent Oracle AQ queue-handle caches for Oracle AQ event backends.

Eliminates per-iteration session + queue handle acquisition. The hub holds a
single dedicated connection per backend instance and caches one queue handle
per subscribed channel. Each dequeue uses the cached handle and honors the
caller's poll_interval (capped at the configured aq_wait_seconds ceiling).
"""

# pyright: reportPrivateUsage=false

import asyncio
import contextlib
import logging
import threading
from math import ceil
from typing import TYPE_CHECKING, Any

from sqlspec.exceptions import EventChannelError, MissingDependencyError
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig

__all__ = ("OracleAsyncAQHub", "OracleSyncAQHub")

logger = get_logger("sqlspec.adapters.oracledb.events.hub")


_AQ_AVAILABLE = False
OracleDatabaseError: Any

try:  # pragma: no cover
    from sqlspec.adapters.oracledb._typing import AQMSG_INVISIBLE as _AQMSG_INVISIBLE
    from sqlspec.adapters.oracledb._typing import AQMSG_PAYLOAD_TYPE_JSON as _AQMSG_PAYLOAD_TYPE_JSON
    from sqlspec.adapters.oracledb._typing import AQMSG_VISIBLE as _AQMSG_VISIBLE
    from sqlspec.adapters.oracledb._typing import DB_TYPE_JSON as _DB_TYPE_JSON
    from sqlspec.adapters.oracledb._typing import AQDequeueOptions as _AQDequeueOptions
    from sqlspec.adapters.oracledb._typing import DatabaseError as _OracleDatabaseErrorImported
except ImportError:  # pragma: no cover
    _AQDequeueOptions = None
    _AQMSG_INVISIBLE = None
    _AQMSG_PAYLOAD_TYPE_JSON = None
    _AQMSG_VISIBLE = None
    _DB_TYPE_JSON = None
    OracleDatabaseError = None
else:  # pragma: no cover
    _AQ_AVAILABLE = True
    OracleDatabaseError = _OracleDatabaseErrorImported

AQDequeueOptions: Any = _AQDequeueOptions
AQMSG_INVISIBLE: "int | None" = _AQMSG_INVISIBLE
AQMSG_PAYLOAD_TYPE_JSON: Any = _AQMSG_PAYLOAD_TYPE_JSON
AQMSG_VISIBLE: "int | None" = _AQMSG_VISIBLE
DB_TYPE_JSON: Any = _DB_TYPE_JSON


def _resolve_payload_type() -> Any:
    """Pick the right driver constant for JSON payloads."""
    if DB_TYPE_JSON is not None:
        return DB_TYPE_JSON
    return AQMSG_PAYLOAD_TYPE_JSON


def _resolve_options(visibility: "int | None", default_visibility: "int | None", wait_seconds: float) -> Any:
    """Build an AQDequeueOptions instance for the requested wait + visibility."""
    if AQDequeueOptions is None:  # pragma: no cover
        msg = "oracledb AQDequeueOptions"
        raise MissingDependencyError(msg, install_package="oracledb")
    options = AQDequeueOptions()
    options.wait = 0 if wait_seconds <= 0 else ceil(wait_seconds)
    if visibility is not None:
        options.visibility = visibility
    elif default_visibility is not None:
        options.visibility = default_visibility
    return options


def _channel_queue_name(template: str, channel: str) -> str:
    """Apply a queue-name template (supports ``{channel}`` substitution)."""
    if isinstance(template, str) and "{" in template:
        with contextlib.suppress(Exception):
            return template.format(channel=channel.upper())
    return template


def _resolve_wait_seconds(poll_interval: float, ceiling: int) -> float:
    """Cap the caller's poll_interval at the configured aq_wait_seconds ceiling."""
    interval = max(float(poll_interval), 0.0)
    if ceiling <= 0:
        return interval
    return min(interval, float(ceiling))


class OracleSyncAQHub:
    """Per-channel persistent queue-handle cache for sync Oracle AQ."""

    __slots__ = (
        "_config",
        "_default_visibility",
        "_lock",
        "_pool_destroying_registered",
        "_queue_name_template",
        "_queues",
        "_session_cm",
        "_session_driver",
        "_shutting_down",
        "_visibility",
        "_wait_ceiling",
    )

    def __init__(
        self,
        config: "OracleSyncConfig",
        *,
        queue_name_template: str,
        visibility: "int | None",
        default_visibility: "int | None",
        wait_ceiling: int,
    ) -> None:
        self._config = config
        self._queue_name_template = queue_name_template
        self._visibility = visibility
        self._default_visibility = default_visibility
        self._wait_ceiling = wait_ceiling
        self._lock = threading.Lock()
        self._queues: dict[str, Any] = {}
        self._session_cm: Any | None = None
        self._session_driver: Any | None = None
        self._shutting_down = False
        self._pool_destroying_registered = False

    def subscribe(self, channel: str) -> None:
        with self._lock:
            self._ensure_handle_locked(channel)

    def dequeue(self, channel: str, poll_interval: float) -> "Any | None":
        with self._lock:
            if self._shutting_down:
                return None
            queue = self._ensure_handle_locked(channel)
            driver = self._session_driver
            if driver is None:
                return None
            wait_seconds = _resolve_wait_seconds(poll_interval, self._wait_ceiling)
            options = _resolve_options(self._visibility, self._default_visibility, wait_seconds)
            try:
                message = queue.deqone(options=options)
            except Exception as error:  # pragma: no cover
                if OracleDatabaseError is None or not isinstance(error, OracleDatabaseError):
                    raise
                log_with_context(
                    logger,
                    logging.WARNING,
                    "event.receive",
                    adapter_name="oracledb",
                    backend_name="advanced_queue",
                    mode="sync",
                    error_type=type(error).__name__,
                    status="failed",
                )
                with contextlib.suppress(Exception):
                    driver.rollback()
                return None
            if message is None:
                with contextlib.suppress(Exception):
                    driver.rollback()
                return None
            payload = message.payload
            with contextlib.suppress(Exception):
                driver.commit()
        return payload

    def shutdown(self) -> None:
        with self._lock:
            if self._shutting_down:
                return
            self._shutting_down = True
            session_cm = self._session_cm
            self._queues.clear()
            self._session_cm = None
            self._session_driver = None
        if session_cm is not None:
            with contextlib.suppress(Exception):
                session_cm.__exit__(None, None, None)
        self._shutting_down = False

    def _ensure_handle_locked(self, channel: str) -> Any:
        cached = self._queues.get(channel)
        if cached is not None:
            return cached
        if not _AQ_AVAILABLE:  # pragma: no cover
            msg = "oracledb"
            raise MissingDependencyError(msg, install_package="oracledb")
        driver = self._session_driver
        if driver is None:
            session_cm = self._config.provide_session()
            driver = session_cm.__enter__()
            self._session_cm = session_cm
            self._session_driver = driver
            self._register_pool_destroying()
        connection = getattr(driver, "connection", None)
        if connection is None:
            msg = "Oracle driver does not expose a raw connection"
            raise EventChannelError(msg)
        queue_name = _channel_queue_name(self._queue_name_template, channel)
        queue = connection.queue(queue_name, payload_type=_resolve_payload_type())
        self._queues[channel] = queue
        return queue

    def _register_pool_destroying(self) -> None:
        if self._pool_destroying_registered:
            return
        runtime = self._config.get_observability_runtime()
        runtime.register_lifecycle_hook("on_pool_destroying", self._pool_destroying_hook)
        self._pool_destroying_registered = True

    def _pool_destroying_hook(self, _context: "dict[str, Any]") -> None:
        self.shutdown()


class OracleAsyncAQHub:
    """Per-channel persistent queue-handle cache for async Oracle AQ."""

    __slots__ = (
        "_config",
        "_default_visibility",
        "_lock",
        "_pool_destroying_registered",
        "_queue_name_template",
        "_queues",
        "_session_cm",
        "_session_driver",
        "_shutting_down",
        "_visibility",
        "_wait_ceiling",
    )

    def __init__(
        self,
        config: "OracleAsyncConfig",
        *,
        queue_name_template: str,
        visibility: "int | None",
        default_visibility: "int | None",
        wait_ceiling: int,
    ) -> None:
        self._config = config
        self._queue_name_template = queue_name_template
        self._visibility = visibility
        self._default_visibility = default_visibility
        self._wait_ceiling = wait_ceiling
        self._lock = asyncio.Lock()
        self._queues: dict[str, Any] = {}
        self._session_cm: Any | None = None
        self._session_driver: Any | None = None
        self._shutting_down = False
        self._pool_destroying_registered = False

    async def subscribe(self, channel: str) -> None:
        async with self._lock:
            await self._ensure_handle_locked(channel)

    async def dequeue(self, channel: str, poll_interval: float) -> "Any | None":
        async with self._lock:
            if self._shutting_down:
                return None
            queue = await self._ensure_handle_locked(channel)
            driver = self._session_driver
            if driver is None:
                return None
            wait_seconds = _resolve_wait_seconds(poll_interval, self._wait_ceiling)
            options = _resolve_options(self._visibility, self._default_visibility, wait_seconds)
            try:
                message = await queue.deqone(options=options)
            except Exception as error:  # pragma: no cover
                if OracleDatabaseError is None or not isinstance(error, OracleDatabaseError):
                    raise
                log_with_context(
                    logger,
                    logging.WARNING,
                    "event.receive",
                    adapter_name="oracledb",
                    backend_name="advanced_queue",
                    mode="async",
                    error_type=type(error).__name__,
                    status="failed",
                )
                with contextlib.suppress(Exception):
                    await driver.rollback()
                return None
            if message is None:
                with contextlib.suppress(Exception):
                    await driver.rollback()
                return None
            payload = message.payload
            with contextlib.suppress(Exception):
                await driver.commit()
        return payload

    async def shutdown(self) -> None:
        async with self._lock:
            if self._shutting_down:
                return
            self._shutting_down = True
            session_cm = self._session_cm
            self._queues.clear()
            self._session_cm = None
            self._session_driver = None
        if session_cm is not None:
            with contextlib.suppress(Exception):
                await session_cm.__aexit__(None, None, None)
        self._shutting_down = False

    async def _ensure_handle_locked(self, channel: str) -> Any:
        cached = self._queues.get(channel)
        if cached is not None:
            return cached
        if not _AQ_AVAILABLE:  # pragma: no cover
            msg = "oracledb"
            raise MissingDependencyError(msg, install_package="oracledb")
        driver = self._session_driver
        if driver is None:
            session_cm = self._config.provide_session()
            driver = await session_cm.__aenter__()
            self._session_cm = session_cm
            self._session_driver = driver
            self._register_pool_destroying()
        connection = getattr(driver, "connection", None)
        if connection is None:
            msg = "Oracle driver does not expose a raw connection"
            raise EventChannelError(msg)
        queue_name = _channel_queue_name(self._queue_name_template, channel)
        queue = connection.queue(queue_name, payload_type=_resolve_payload_type())
        self._queues[channel] = queue
        return queue

    def _register_pool_destroying(self) -> None:
        if self._pool_destroying_registered:
            return
        runtime = self._config.get_observability_runtime()
        runtime.register_lifecycle_hook("on_pool_destroying", self._pool_destroying_hook)
        self._pool_destroying_registered = True

    def _pool_destroying_hook(self, _context: "dict[str, Any]") -> "Any":
        return self.shutdown()
