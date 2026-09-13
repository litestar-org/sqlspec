"""Event channel package exports."""

from sqlspec.extensions.events._channel import (
    AsyncEventChannel,
    AsyncEventListener,
    SyncEventChannel,
    SyncEventListener,
    load_native_backend,
    resolve_event_poll_interval,
    resolve_poll_interval,
)
from sqlspec.extensions.events._hints import EventRuntimeHints, get_runtime_hints, resolve_adapter_name
from sqlspec.extensions.events._models import EventMessage
from sqlspec.extensions.events._payload import (
    MAX_NOTIFY_BYTES,
    decode_notify_payload,
    encode_notify_payload,
    fits_notify_payload,
    measure_notify_payload,
    parse_event_timestamp,
)
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
from sqlspec.extensions.events.primitives import claim_verified, lock_clause, row_limit_clause, select_limit_prefix

__all__ = (
    "MAX_NOTIFY_BYTES",
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
    "claim_verified",
    "decode_notify_payload",
    "encode_notify_payload",
    "fits_notify_payload",
    "get_runtime_hints",
    "load_native_backend",
    "lock_clause",
    "measure_notify_payload",
    "normalize_event_channel_name",
    "normalize_queue_table_name",
    "parse_event_timestamp",
    "resolve_adapter_name",
    "resolve_event_poll_interval",
    "resolve_poll_interval",
    "row_limit_clause",
    "select_limit_prefix",
)
