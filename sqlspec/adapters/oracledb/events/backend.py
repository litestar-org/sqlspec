"""Oracle Advanced Queuing backend for EventChannel."""

import contextlib
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlspec.exceptions import EventChannelError, ImproperConfigurationError, MissingDependencyError
from sqlspec.extensions.events import EventMessage, parse_event_timestamp
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig

try:  # pragma: no cover - optional dependency path
    import oracledb
except ImportError:  # pragma: no cover - optional dependency path
    oracledb = None  # type: ignore[assignment]

logger = get_logger("sqlspec.events.oracle")

__all__ = ("OracleAsyncAQEventBackend", "OracleSyncAQEventBackend", "create_event_backend")

_DEFAULT_QUEUE_NAME = "SQLSPEC_EVENTS_QUEUE"
_DEFAULT_VISIBILITY: int | None
_VISIBILITY_LOOKUP: "dict[str, int]"

if oracledb is None:
    _DEFAULT_VISIBILITY = None
    _VISIBILITY_LOOKUP = {}
else:
    try:
        _DEFAULT_VISIBILITY = oracledb.AQMSG_VISIBLE  # type: ignore[attr-defined]
    except AttributeError:
        _DEFAULT_VISIBILITY = None
    _VISIBILITY_LOOKUP = {}
    if _DEFAULT_VISIBILITY is not None:
        _VISIBILITY_LOOKUP["AQMSG_VISIBLE"] = _DEFAULT_VISIBILITY
    with contextlib.suppress(AttributeError):
        _VISIBILITY_LOOKUP["AQMSG_INVISIBLE"] = oracledb.AQMSG_INVISIBLE  # type: ignore[attr-defined]


def _resolve_visibility_setting(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if not isinstance(value, str):
        msg = f"Invalid aq_visibility value: {value!r}. Expected int or AQMSG_* string."
        raise ImproperConfigurationError(msg)
    visibility = _VISIBILITY_LOOKUP.get(value)
    if visibility is None:
        msg = f"Invalid aq_visibility value: {value!r}. Expected one of: {sorted(_VISIBILITY_LOOKUP)}"
        raise ImproperConfigurationError(msg)
    return visibility


class OracleSyncAQEventBackend:
    """Oracle AQ backend for sync Oracle adapters."""

    __slots__ = ("_config", "_queue_name", "_runtime", "_visibility", "_wait_seconds")

    supports_sync = True
    supports_async = False
    backend_name = "advanced_queue"

    def __init__(self, config: "OracleSyncConfig", settings: "dict[str, Any] | None" = None) -> None:
        if "oracledb" not in type(config).__module__:
            msg = "Oracle AQ backend requires an Oracle adapter"
            raise ImproperConfigurationError(msg)
        if config.is_async:
            msg = "OracleSyncAQEventBackend requires a sync adapter"
            raise ImproperConfigurationError(msg)
        if oracledb is None:
            msg = "oracledb"
            raise MissingDependencyError(msg, install_package="oracledb")
        self._config = config
        self._runtime = config.get_observability_runtime()
        settings = settings or {}
        self._queue_name = settings.get("aq_queue", _DEFAULT_QUEUE_NAME)
        self._visibility: int | None = _resolve_visibility_setting(settings.get("aq_visibility"))
        self._wait_seconds: int = int(settings.get("aq_wait_seconds", 5))
        log_with_context(
            logger,
            logging.DEBUG,
            "event.listen",
            adapter_name="oracledb",
            backend_name=self.backend_name,
            mode="async",
            status="backend_ready",
        )
        log_with_context(
            logger,
            logging.DEBUG,
            "event.listen",
            adapter_name="oracledb",
            backend_name=self.backend_name,
            mode="sync",
            status="backend_ready",
        )

    def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        event_id = uuid4().hex
        envelope = _build_envelope(channel, event_id, payload, metadata)
        session_cm = self._config.provide_session()
        with session_cm as driver:
            connection = driver.connection
            if connection is None:
                msg = "Oracle driver does not expose a raw connection"
                raise EventChannelError(msg)
            queue = _get_queue(connection, channel, self._queue_name)
            queue.enqone(payload=envelope)
            driver.commit()
        self._runtime.increment_metric("events.publish.native")
        return event_id

    def dequeue(self, channel: str, poll_interval: float) -> EventMessage | None:
        session_cm = self._config.provide_session()
        with session_cm as driver:
            connection = driver.connection
            if connection is None:
                msg = "Oracle driver does not expose a raw connection"
                raise EventChannelError(msg)
            queue = _get_queue(connection, channel, self._queue_name)
            options = oracledb.AQDequeueOptions()  # type: ignore[attr-defined]
            options.wait = max(int(self._wait_seconds), 0)
            if self._visibility is not None:
                options.visibility = self._visibility
            elif _DEFAULT_VISIBILITY is not None:
                options.visibility = _DEFAULT_VISIBILITY
            try:
                message = queue.deqone(options=options)
            except Exception as error:  # pragma: no cover - driver surfaced runtime
                if oracledb is None or not isinstance(error, oracledb.DatabaseError):
                    raise
                log_with_context(
                    logger,
                    logging.WARNING,
                    "event.receive",
                    adapter_name="oracledb",
                    backend_name=self.backend_name,
                    mode="sync",
                    error_type=type(error).__name__,
                    status="failed",
                )
                driver.rollback()
                return None
            if message is None:
                driver.rollback()
                return None
            payload = message.payload
            driver.commit()
        return _parse_message(channel, payload)

    def ack(self, _event_id: str) -> None:
        """Acknowledge an event (no-op for Oracle AQ).

        Oracle AQ messages are removed upon commit, so acknowledgment
        is handled automatically by the database transaction.
        """
        self._runtime.increment_metric("events.ack")

    def nack(self, _event_id: str) -> None:
        """Return an event to the queue (no-op for Oracle AQ).

        Oracle AQ does not support returning messages after commit.
        """

    def shutdown(self) -> None:
        """Shutdown the backend (no-op for Oracle AQ)."""


class OracleAsyncAQEventBackend:
    """Oracle AQ backend for async Oracle adapters."""

    __slots__ = ("_config", "_queue_name", "_runtime", "_visibility", "_wait_seconds")

    supports_sync = False
    supports_async = True
    backend_name = "advanced_queue"

    def __init__(self, config: "OracleAsyncConfig", settings: "dict[str, Any] | None" = None) -> None:
        if "oracledb" not in type(config).__module__:
            msg = "Oracle AQ backend requires an Oracle adapter"
            raise ImproperConfigurationError(msg)
        if not config.is_async:
            msg = "OracleAsyncAQEventBackend requires an async adapter"
            raise ImproperConfigurationError(msg)
        if oracledb is None:
            msg = "oracledb"
            raise MissingDependencyError(msg, install_package="oracledb")
        self._config = config
        self._runtime = config.get_observability_runtime()
        settings = settings or {}
        self._queue_name = settings.get("aq_queue", _DEFAULT_QUEUE_NAME)
        self._visibility: int | None = _resolve_visibility_setting(settings.get("aq_visibility"))
        self._wait_seconds: int = int(settings.get("aq_wait_seconds", 5))

    async def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        event_id = uuid4().hex
        envelope = _build_envelope(channel, event_id, payload, metadata)
        session_cm = self._config.provide_session()
        async with session_cm as driver:
            connection = driver.connection
            if connection is None:
                msg = "Oracle driver does not expose a raw connection"
                raise EventChannelError(msg)
            queue = _get_queue(connection, channel, self._queue_name)
            await queue.enqone(payload=envelope)
            await driver.commit()
        self._runtime.increment_metric("events.publish.native")
        return event_id

    async def dequeue(self, channel: str, poll_interval: float) -> EventMessage | None:
        session_cm = self._config.provide_session()
        async with session_cm as driver:
            connection = driver.connection
            if connection is None:
                msg = "Oracle driver does not expose a raw connection"
                raise EventChannelError(msg)
            queue = _get_queue(connection, channel, self._queue_name)
            options = oracledb.AQDequeueOptions()  # type: ignore[attr-defined]
            options.wait = max(int(self._wait_seconds), 0)
            if self._visibility is not None:
                options.visibility = self._visibility
            elif _DEFAULT_VISIBILITY is not None:
                options.visibility = _DEFAULT_VISIBILITY
            try:
                message = await queue.deqone(options=options)
            except Exception as error:  # pragma: no cover - driver surfaced runtime
                if oracledb is None or not isinstance(error, oracledb.DatabaseError):
                    raise
                log_with_context(
                    logger,
                    logging.WARNING,
                    "event.receive",
                    adapter_name="oracledb",
                    backend_name=self.backend_name,
                    mode="async",
                    error_type=type(error).__name__,
                    status="failed",
                )
                await driver.rollback()
                return None
            if message is None:
                await driver.rollback()
                return None
            payload = message.payload
            await driver.commit()
        return _parse_message(channel, payload)

    async def ack(self, _event_id: str) -> None:
        """Acknowledge an event (no-op for Oracle AQ).

        Oracle AQ messages are removed upon commit, so acknowledgment
        is handled automatically by the database transaction.
        """
        self._runtime.increment_metric("events.ack")

    async def nack(self, _event_id: str) -> None:
        """Return an event to the queue (no-op for Oracle AQ).

        Oracle AQ does not support returning messages after commit.
        """

    async def shutdown(self) -> None:
        """Shutdown the backend (no-op for Oracle AQ)."""


def _get_queue(connection: Any, channel: str, queue_name: str) -> Any:
    """Get Oracle AQ queue handle."""
    if oracledb is None:
        msg = "oracledb"
        raise MissingDependencyError(msg, install_package="oracledb")
    if isinstance(queue_name, str) and "{" in queue_name:
        with contextlib.suppress(Exception):
            queue_name = queue_name.format(channel=channel.upper())
    try:
        payload_type = oracledb.DB_TYPE_JSON
    except AttributeError:
        payload_type = None
    if payload_type is None:
        try:
            payload_type = oracledb.AQMSG_PAYLOAD_TYPE_JSON  # type: ignore[attr-defined]
        except AttributeError:
            payload_type = None
    return connection.queue(queue_name, payload_type=payload_type)


def _build_envelope(
    channel: str, event_id: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None"
) -> "dict[str, Any]":
    """Build event envelope for Oracle AQ."""
    return {
        "channel": channel,
        "event_id": event_id,
        "payload": payload,
        "metadata": metadata,
        "published_at": datetime.now(timezone.utc).isoformat(),
    }


def _parse_message(channel: str, payload: Any) -> EventMessage:
    """Parse Oracle AQ message payload into EventMessage."""
    if not isinstance(payload, dict):
        payload = {"payload": payload}
    payload_channel = payload.get("channel")
    message_channel = payload_channel if isinstance(payload_channel, str) else channel
    event_id = payload.get("event_id", uuid4().hex)
    body = payload.get("payload")
    if not isinstance(body, dict):
        body = {"value": body}
    metadata = payload.get("metadata")
    if not (metadata is None or isinstance(metadata, dict)):
        metadata = {"value": metadata}
    timestamp = parse_event_timestamp(payload.get("published_at"))
    return EventMessage(
        event_id=event_id,
        channel=message_channel,
        payload=body,
        metadata=metadata,
        attempts=0,
        available_at=timestamp,
        lease_expires_at=None,
        created_at=timestamp,
    )


def create_event_backend(
    config: "OracleAsyncConfig | OracleSyncConfig", backend_name: str, extension_settings: "dict[str, Any]"
) -> OracleSyncAQEventBackend | OracleAsyncAQEventBackend | None:
    """Factory used by EventChannel to create the Oracle AQ backend."""
    is_async = config.is_async
    match (backend_name, is_async):
        case ("advanced_queue", False):
            try:
                return OracleSyncAQEventBackend(config, extension_settings)  # type: ignore[arg-type]
            except (ImproperConfigurationError, MissingDependencyError):
                return None
        case ("advanced_queue", True):
            try:
                return OracleAsyncAQEventBackend(config, extension_settings)  # type: ignore[arg-type]
            except (ImproperConfigurationError, MissingDependencyError):
                return None
        case _:
            return None
