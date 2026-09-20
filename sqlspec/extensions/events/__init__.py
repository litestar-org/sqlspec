"""Event channel package exports."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec import _COMPILED

if TYPE_CHECKING:
    from sqlspec.extensions.events import primitives as primitives
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

_EXPORTS: dict[str, tuple[str, str | None]] = {
    "primitives": ("sqlspec.extensions.events.primitives", None),
    "AsyncEventBackendProtocol": ("sqlspec.extensions.events._protocols", "AsyncEventBackendProtocol"),
    "AsyncEventChannel": ("sqlspec.extensions.events._channel", "AsyncEventChannel"),
    "AsyncEventHandler": ("sqlspec.extensions.events._protocols", "AsyncEventHandler"),
    "AsyncEventListener": ("sqlspec.extensions.events._channel", "AsyncEventListener"),
    "AsyncTableEventQueue": ("sqlspec.extensions.events._queue", "AsyncTableEventQueue"),
    "BaseEventQueueStore": ("sqlspec.extensions.events._store", "BaseEventQueueStore"),
    "EventMessage": ("sqlspec.extensions.events._models", "EventMessage"),
    "EventRuntimeHints": ("sqlspec.extensions.events._hints", "EventRuntimeHints"),
    "MAX_NOTIFY_BYTES": ("sqlspec.extensions.events._payload", "MAX_NOTIFY_BYTES"),
    "SyncEventBackendProtocol": ("sqlspec.extensions.events._protocols", "SyncEventBackendProtocol"),
    "SyncEventChannel": ("sqlspec.extensions.events._channel", "SyncEventChannel"),
    "SyncEventHandler": ("sqlspec.extensions.events._protocols", "SyncEventHandler"),
    "SyncEventListener": ("sqlspec.extensions.events._channel", "SyncEventListener"),
    "SyncTableEventQueue": ("sqlspec.extensions.events._queue", "SyncTableEventQueue"),
    "build_queue_backend": ("sqlspec.extensions.events._queue", "build_queue_backend"),
    "claim_verified": ("sqlspec.extensions.events.primitives", "claim_verified"),
    "decode_notify_payload": ("sqlspec.extensions.events._payload", "decode_notify_payload"),
    "encode_notify_payload": ("sqlspec.extensions.events._payload", "encode_notify_payload"),
    "fits_notify_payload": ("sqlspec.extensions.events._payload", "fits_notify_payload"),
    "get_runtime_hints": ("sqlspec.extensions.events._hints", "get_runtime_hints"),
    "load_native_backend": ("sqlspec.extensions.events._channel", "load_native_backend"),
    "lock_clause": ("sqlspec.extensions.events.primitives", "lock_clause"),
    "measure_notify_payload": ("sqlspec.extensions.events._payload", "measure_notify_payload"),
    "normalize_event_channel_name": ("sqlspec.extensions.events._store", "normalize_event_channel_name"),
    "normalize_queue_table_name": ("sqlspec.extensions.events._store", "normalize_queue_table_name"),
    "parse_event_timestamp": ("sqlspec.extensions.events._payload", "parse_event_timestamp"),
    "resolve_adapter_name": ("sqlspec.extensions.events._hints", "resolve_adapter_name"),
    "resolve_event_poll_interval": ("sqlspec.extensions.events._channel", "resolve_event_poll_interval"),
    "resolve_poll_interval": ("sqlspec.extensions.events._channel", "resolve_poll_interval"),
    "row_limit_clause": ("sqlspec.extensions.events.primitives", "row_limit_clause"),
    "select_limit_prefix": ("sqlspec.extensions.events.primitives", "select_limit_prefix"),
}


def __getattr__(name: str) -> Any:
    try:
        module_name, attribute = _EXPORTS[name]
    except KeyError:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg) from None
    module = import_module(module_name)
    value = module if attribute is None else getattr(module, attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_EXPORTS))


if _COMPILED:
    for _name in __all__:
        if _name not in globals():
            __getattr__(_name)
