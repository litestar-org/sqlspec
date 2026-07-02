"""Oracle Advanced Queuing backend for EventChannel."""

import contextlib
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlspec.adapters.oracledb.events._hub import OracleAsyncAQHub, OracleSyncAQHub
from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError
from sqlspec.extensions.events import EventMessage, parse_event_timestamp
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig

__all__ = (
    "OracleAsyncAQEventBackend",
    "OracleAsyncTxEventQEventBackend",
    "OracleSyncAQEventBackend",
    "OracleSyncTxEventQEventBackend",
    "create_event_backend",
)

_ORACLEDB_AVAILABLE = False

try:  # pragma: no cover
    from sqlspec.adapters.oracledb._typing import AQMSG_INVISIBLE as _AQMSG_INVISIBLE
    from sqlspec.adapters.oracledb._typing import AQMSG_PAYLOAD_TYPE_JSON as _AQMSG_PAYLOAD_TYPE_JSON
    from sqlspec.adapters.oracledb._typing import AQMSG_VISIBLE as _AQMSG_VISIBLE
    from sqlspec.adapters.oracledb._typing import DB_TYPE_JSON as _DB_TYPE_JSON
    from sqlspec.adapters.oracledb._typing import AQDequeueOptions as _AQDequeueOptions
except ImportError:  # pragma: no cover
    _AQDequeueOptions = None
    _AQMSG_INVISIBLE = None
    _AQMSG_PAYLOAD_TYPE_JSON = None
    _AQMSG_VISIBLE = None
    _DB_TYPE_JSON = None
else:  # pragma: no cover
    _ORACLEDB_AVAILABLE = True

AQDequeueOptions: Any = _AQDequeueOptions
AQMSG_INVISIBLE: "int | None" = _AQMSG_INVISIBLE
AQMSG_PAYLOAD_TYPE_JSON: Any = _AQMSG_PAYLOAD_TYPE_JSON
AQMSG_VISIBLE: "int | None" = _AQMSG_VISIBLE
DB_TYPE_JSON: Any = _DB_TYPE_JSON

logger = get_logger("sqlspec.events.oracle")


_DEFAULT_QUEUE_NAME = "SQLSPEC_EVENTS_QUEUE"
_DEFAULT_VISIBILITY: "int | None"
_VISIBILITY_LOOKUP: "dict[str, int]"

if AQDequeueOptions is None:
    _DEFAULT_VISIBILITY = None
    _VISIBILITY_LOOKUP = {}
else:
    _DEFAULT_VISIBILITY = AQMSG_VISIBLE
    _VISIBILITY_LOOKUP = {}
    if _DEFAULT_VISIBILITY is not None:
        _VISIBILITY_LOOKUP["AQMSG_VISIBLE"] = _DEFAULT_VISIBILITY
    if AQMSG_INVISIBLE is not None:
        _VISIBILITY_LOOKUP["AQMSG_INVISIBLE"] = AQMSG_INVISIBLE


def _resolve_visibility_setting(value: Any) -> "int | None":
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

    __slots__ = ("_config", "_hub", "_queue_name", "_runtime", "_visibility", "_wait_seconds")

    supports_sync = True
    supports_async = False
    backend_name = "aq"

    def __init__(self, config: "OracleSyncConfig", settings: "dict[str, Any] | None" = None) -> None:
        if "oracledb" not in type(config).__module__:
            msg = "Oracle AQ backend requires an Oracle adapter"
            raise ImproperConfigurationError(msg)
        if config.is_async:
            msg = f"{type(self).__name__} requires a sync adapter"
            raise ImproperConfigurationError(msg)
        if not _ORACLEDB_AVAILABLE:
            msg = "oracledb"
            raise MissingDependencyError(msg, install_package="oracledb")
        self._config = config
        self._runtime = config.get_observability_runtime()
        settings = settings or {}
        self._queue_name = settings.get("aq_queue", _DEFAULT_QUEUE_NAME)
        self._visibility: int | None = _resolve_visibility_setting(settings.get("aq_visibility"))
        self._wait_seconds: int = int(settings.get("aq_wait_seconds", 5))
        self._hub: OracleSyncAQHub | None = None
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
                raise ImproperConfigurationError(msg)
            queue = _get_publish_queue(connection, channel, self._queue_name)
            queue.enqone(connection.msgproperties(payload=envelope))
            driver.commit()
        self._runtime.increment_metric("events.publish.native")
        return event_id

    def dequeue(self, channel: str, poll_interval: float) -> "EventMessage | None":
        hub = self._ensure_hub()
        payload = hub.dequeue(channel, poll_interval)
        if payload is None:
            return None
        return _parse_message(channel, payload)

    def ack(self, _event_id: str) -> None:
        """Acknowledge an event (no-op for Oracle AQ - committed at dequeue time)."""
        self._runtime.increment_metric("events.ack")

    def nack(self, _event_id: str) -> None:
        """Return an event to the queue (no-op for Oracle AQ)."""

    def shutdown(self) -> None:
        hub = self._hub
        if hub is not None:
            self._hub = None
            hub.shutdown()

    def _ensure_hub(self) -> OracleSyncAQHub:
        if self._hub is None:
            self._hub = OracleSyncAQHub(
                self._config,
                queue_name_template=self._queue_name,
                visibility=self._visibility,
                default_visibility=_DEFAULT_VISIBILITY,
                wait_ceiling=self._wait_seconds,
                backend_name=self.backend_name,
            )
        return self._hub


class OracleAsyncAQEventBackend:
    """Oracle AQ backend for async Oracle adapters."""

    __slots__ = ("_config", "_hub", "_queue_name", "_runtime", "_visibility", "_wait_seconds")

    supports_sync = False
    supports_async = True
    backend_name = "aq"

    def __init__(self, config: "OracleAsyncConfig", settings: "dict[str, Any] | None" = None) -> None:
        if "oracledb" not in type(config).__module__:
            msg = "Oracle AQ backend requires an Oracle adapter"
            raise ImproperConfigurationError(msg)
        if not config.is_async:
            msg = f"{type(self).__name__} requires an async adapter"
            raise ImproperConfigurationError(msg)
        if not _ORACLEDB_AVAILABLE:
            msg = "oracledb"
            raise MissingDependencyError(msg, install_package="oracledb")
        self._config = config
        self._runtime = config.get_observability_runtime()
        settings = settings or {}
        self._queue_name = settings.get("aq_queue", _DEFAULT_QUEUE_NAME)
        self._visibility: int | None = _resolve_visibility_setting(settings.get("aq_visibility"))
        self._wait_seconds: int = int(settings.get("aq_wait_seconds", 5))
        self._hub: OracleAsyncAQHub | None = None

    async def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        event_id = uuid4().hex
        envelope = _build_envelope(channel, event_id, payload, metadata)
        session_cm = self._config.provide_session()
        async with session_cm as driver:
            connection = driver.connection
            if connection is None:
                msg = "Oracle driver does not expose a raw connection"
                raise ImproperConfigurationError(msg)
            queue = _get_publish_queue(connection, channel, self._queue_name)
            await queue.enqone(connection.msgproperties(payload=envelope))
            await driver.commit()
        self._runtime.increment_metric("events.publish.native")
        return event_id

    async def dequeue(self, channel: str, poll_interval: float) -> "EventMessage | None":
        hub = self._ensure_hub()
        payload = await hub.dequeue(channel, poll_interval)
        if payload is None:
            return None
        return _parse_message(channel, payload)

    async def ack(self, _event_id: str) -> None:
        """Acknowledge an event (no-op for Oracle AQ - committed at dequeue time)."""
        self._runtime.increment_metric("events.ack")

    async def nack(self, _event_id: str) -> None:
        """Return an event to the queue (no-op for Oracle AQ)."""

    async def shutdown(self) -> None:
        hub = self._hub
        if hub is not None:
            self._hub = None
            await hub.shutdown()

    def _ensure_hub(self) -> OracleAsyncAQHub:
        if self._hub is None:
            self._hub = OracleAsyncAQHub(
                self._config,
                queue_name_template=self._queue_name,
                visibility=self._visibility,
                default_visibility=_DEFAULT_VISIBILITY,
                wait_ceiling=self._wait_seconds,
                backend_name=self.backend_name,
            )
        return self._hub


class OracleSyncTxEventQEventBackend(OracleSyncAQEventBackend):
    """Oracle Transactional Event Queues backend for sync Oracle adapters.

    Shares the classic AQ client path (queue/enqueue/dequeue); only provisioning
    (``DBMS_AQADM.CREATE_TRANSACTIONAL_EVENT_QUEUE``) and the backend label differ.
    """

    __slots__ = ()

    backend_name = "txeventq"


class OracleAsyncTxEventQEventBackend(OracleAsyncAQEventBackend):
    """Oracle Transactional Event Queues backend for async Oracle adapters."""

    __slots__ = ()

    backend_name = "txeventq"


def _get_publish_queue(connection: Any, channel: str, queue_name: str) -> Any:
    """Acquire a queue handle for a one-shot publish."""
    if not _ORACLEDB_AVAILABLE:
        msg = "oracledb"
        raise MissingDependencyError(msg, install_package="oracledb")
    if isinstance(queue_name, str) and "{" in queue_name:
        with contextlib.suppress(Exception):
            queue_name = queue_name.format(channel=channel.upper())
    payload_type = "JSON" if DB_TYPE_JSON is not None else AQMSG_PAYLOAD_TYPE_JSON
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
) -> "OracleSyncAQEventBackend | OracleAsyncAQEventBackend | None":
    """EventChannel factory for the Oracle AQ backend."""
    is_async = config.is_async
    match (backend_name, is_async):
        case ("aq", False):
            try:
                return OracleSyncAQEventBackend(config, extension_settings)  # type: ignore[arg-type]
            except (ImproperConfigurationError, MissingDependencyError):
                return None
        case ("aq", True):
            try:
                return OracleAsyncAQEventBackend(config, extension_settings)  # type: ignore[arg-type]
            except (ImproperConfigurationError, MissingDependencyError):
                return None
        case ("txeventq", False):
            try:
                return OracleSyncTxEventQEventBackend(config, extension_settings)  # type: ignore[arg-type]
            except (ImproperConfigurationError, MissingDependencyError):
                return None
        case ("txeventq", True):
            try:
                return OracleAsyncTxEventQEventBackend(config, extension_settings)  # type: ignore[arg-type]
            except (ImproperConfigurationError, MissingDependencyError):
                return None
        case _:
            return None
