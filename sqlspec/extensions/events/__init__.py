"""Event channel package exports."""

from sqlspec.extensions.events._channel import (
    AsyncEventChannel,
    AsyncEventListener,
    SyncEventChannel,
    SyncEventListener,
    load_native_backend,
    resolve_poll_interval,
)
from sqlspec.extensions.events._hints import EventRuntimeHints, get_runtime_hints, resolve_adapter_name
from sqlspec.extensions.events._models import EventMessage
from sqlspec.extensions.events._payload import decode_notify_payload, encode_notify_payload, parse_event_timestamp
from sqlspec.extensions.events._protocols import (
    AsyncEventBackendProtocol,
    AsyncEventHandler,
    SyncEventBackendProtocol,
    SyncEventHandler,
)
from sqlspec.extensions.events._queue import AsyncTableEventQueue, SyncTableEventQueue, build_queue_backend
from sqlspec.extensions.events._store import (
    BaseEventQueueStore,
    normalize_event_channel_name,
    normalize_queue_table_name,
)

__all__ = (
    "AsyncEventBackendProtocol",
    "AsyncEventChannel",
    "AsyncEventHandler",
    "AsyncEventListener",
    "AsyncTableEventQueue",
    "BaseEventQueueStore",
    "EventMessage",
    "EventRuntimeHints",
    "SyncEventBackendProtocol",
    "SyncEventChannel",
    "SyncEventHandler",
    "SyncEventListener",
    "SyncTableEventQueue",
    "build_queue_backend",
    "decode_notify_payload",
    "encode_notify_payload",
    "get_runtime_hints",
    "load_native_backend",
    "normalize_event_channel_name",
    "normalize_queue_table_name",
    "parse_event_timestamp",
    "resolve_adapter_name",
    "resolve_poll_interval",
)
