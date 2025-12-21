"""Oracle Advanced Queuing backend for EventChannel."""

import contextlib
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlspec.exceptions import EventChannelError, ImproperConfigurationError, MissingDependencyError
from sqlspec.extensions.events._models import EventMessage
from sqlspec.utils.logging import get_logger
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from sqlspec.config import DatabaseConfigProtocol

try:  # pragma: no cover - optional dependency path
    import oracledb
except ImportError:  # pragma: no cover - optional dependency path
    oracledb = None  # type: ignore[assignment]

logger = get_logger("events.oracle")

__all__ = ("OracleAQEventBackend", "create_event_backend")

_DEFAULT_QUEUE_NAME = "SQLSPEC_EVENTS_QUEUE"


class OracleAQEventBackend:
    """Oracle AQ backend used by sync Oracle adapters."""

    supports_sync = True
    supports_async = False
    backend_name = "advanced_queue"

    def __init__(self, config: "DatabaseConfigProtocol[Any, Any, Any]", settings: dict[str, Any] | None = None) -> None:
        if "oracledb" not in type(config).__module__:
            msg = "Oracle AQ backend requires an Oracle adapter"
            raise ImproperConfigurationError(msg)
        if config.is_async:
            msg = "Oracle AQ backend requires a synchronous Oracle configuration"
            raise ImproperConfigurationError(msg)
        if oracledb is None:
            msg = "oracledb"
            raise MissingDependencyError(msg, install_package="oracledb")
        self._config = config
        self._runtime = config.get_observability_runtime()
        settings = settings or {}
        self._queue_name = settings.get("aq_queue", _DEFAULT_QUEUE_NAME)
        self._visibility: str | None = settings.get("aq_visibility")
        self._wait_seconds: int = int(settings.get("aq_wait_seconds", 5))

    def publish_sync(self, channel: str, payload: dict[str, Any], metadata: dict[str, Any] | None = None) -> str:
        event_id = uuid4().hex
        envelope = self._build_envelope(channel, event_id, payload, metadata)
        session_cm = self._config.provide_session()
        with session_cm as driver:  # type: ignore[union-attr]
            connection = getattr(driver, "connection", None)
            if connection is None:
                msg = "Oracle driver does not expose a raw connection"
                raise EventChannelError(msg)
            queue = self._get_queue(connection, channel)
            queue.enqone(payload=envelope)
            driver.commit()
        self._runtime.increment_metric("events.publish.native")
        return event_id

    async def publish_async(self, *_: Any, **__: Any) -> str:  # pragma: no cover - guarded by supports_async
        msg = "Oracle AQ backend does not support async adapters"
        raise ImproperConfigurationError(msg)

    def dequeue_sync(self, channel: str, poll_interval: float) -> EventMessage | None:
        session_cm = self._config.provide_session()
        with session_cm as driver:  # type: ignore[union-attr]
            connection = getattr(driver, "connection", None)
            if connection is None:
                msg = "Oracle driver does not expose a raw connection"
                raise EventChannelError(msg)
            queue = self._get_queue(connection, channel)
            options = oracledb.AQDequeueOptions()  # type: ignore[attr-defined]
            options.wait = max(int(self._wait_seconds), 0)
            if self._visibility:
                default_visibility = getattr(oracledb, "AQMSG_VISIBLE", None)
                options.visibility = getattr(oracledb, self._visibility, None) or default_visibility
            try:
                message = queue.deqone(options=options)
            except Exception as error:  # pragma: no cover - driver surfaced runtime
                if oracledb is None or not isinstance(error, oracledb.DatabaseError):
                    raise
                logger.warning("Oracle AQ dequeue failed: %s", error)
                driver.rollback()
                return None
            if message is None:
                driver.rollback()
                return None
            payload = message.payload
            driver.commit()
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
        published_at = payload.get("published_at")
        timestamp = self._parse_timestamp(published_at)
        self._runtime.increment_metric("events.deliver")
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

    async def dequeue_async(self, *_: Any, **__: Any) -> EventMessage | None:  # pragma: no cover - guarded
        msg = "Oracle AQ backend does not support async adapters"
        raise ImproperConfigurationError(msg)

    def ack_sync(self, _event_id: str) -> None:
        """Acknowledge an event (no-op for Oracle AQ).

        Oracle AQ messages are removed upon commit, so acknowledgment
        is handled automatically by the database transaction.
        """
        self._runtime.increment_metric("events.ack")

    async def ack_async(self, *_: Any, **__: Any) -> None:  # pragma: no cover - guarded
        msg = "Oracle AQ backend does not support async adapters"
        raise ImproperConfigurationError(msg)

    def _get_queue(self, connection: Any, channel: str) -> Any:
        queue_name = self._queue_name
        if isinstance(queue_name, str) and "{" in queue_name:
            with contextlib.suppress(Exception):
                queue_name = queue_name.format(channel=channel.upper())
        payload_type = getattr(oracledb, "DB_TYPE_JSON", None)
        if payload_type is None:
            payload_type = getattr(oracledb, "AQMSG_PAYLOAD_TYPE_JSON", None)
        return connection.queue(queue_name, payload_type=payload_type)

    @staticmethod
    def _build_envelope(
        channel: str, event_id: str, payload: dict[str, Any], metadata: dict[str, Any] | None
    ) -> dict[str, Any]:
        timestamp = datetime.now(timezone.utc).isoformat()
        return {
            "channel": channel,
            "event_id": event_id,
            "payload": payload,
            "metadata": metadata,
            "published_at": timestamp,
        }

    @staticmethod
    def _parse_timestamp(value: Any) -> datetime:
        """Parse a timestamp value into a timezone-aware datetime.

        Handles ISO format strings, datetime objects, and falls back to
        current UTC time for invalid or missing values.
        """
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            with contextlib.suppress(ValueError):
                parsed = datetime.fromisoformat(value)
                if parsed.tzinfo is None:
                    return parsed.replace(tzinfo=timezone.utc)
                return parsed
        return datetime.now(timezone.utc)


def create_event_backend(
    config: "DatabaseConfigProtocol[Any, Any, Any]", backend_name: str, extension_settings: dict[str, Any]
) -> OracleAQEventBackend | None:
    """Factory used by EventChannel to create the Oracle AQ backend."""
    if backend_name != "advanced_queue":
        return None
    try:
        return OracleAQEventBackend(config, extension_settings)
    except (ImproperConfigurationError, MissingDependencyError):
        return None
