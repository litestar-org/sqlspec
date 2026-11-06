# Events & Channels Extension

```python
from sqlspec.extensions.events import (
    AsyncEventBackendProtocol,
    AsyncEventChannel,
    AsyncEventListener,
    AsyncTableEventQueue,
    BaseEventQueueStore,
    EventMessage,
    EventRuntimeHints,
    SyncEventBackendProtocol,
    SyncEventChannel,
    SyncEventListener,
    SyncTableEventQueue,
    build_queue_backend,
    decode_notify_payload,
    encode_notify_payload,
    fits_notify_payload,
    get_runtime_hints,
    measure_notify_payload,
)
```

Database-native pub/sub (`LISTEN/NOTIFY`, Oracle Advanced Queuing) and durable table-backed event queues across all supported adapters. Adapter-specific `EventQueueStore` and native `EventsBackend` classes are documented on each [Database Adapter](../adapters/index.md) page. See the [Event Channels Guide](../../integrations/events.md) for usage examples.

## Event Channels & Listeners

::: sqlspec.extensions.events._channel.SyncEventChannel
    options:
      heading_level: 3
      members:
        - publish
        - iter_events
        - listen
        - ack
        - nack
        - shutdown

::: sqlspec.extensions.events._channel.AsyncEventChannel
    options:
      heading_level: 3
      members:
        - publish
        - iter_events
        - listen
        - ack
        - nack
        - shutdown

::: sqlspec.extensions.events._channel.SyncEventListener
    options:
      heading_level: 3
      members: false

::: sqlspec.extensions.events._channel.AsyncEventListener
    options:
      heading_level: 3
      members: false

::: sqlspec.extensions.events._models.EventMessage
    options:
      heading_level: 3
      members: false

## Durable Table Queues & Stores

::: sqlspec.extensions.events._queue.SyncTableEventQueue
    options:
      heading_level: 3
      members:
        - publish
        - publish_many
        - dequeue
        - dequeue_by_event_id
        - ack
        - nack
        - shutdown

::: sqlspec.extensions.events._queue.AsyncTableEventQueue
    options:
      heading_level: 3
      members:
        - publish
        - publish_many
        - dequeue
        - dequeue_by_event_id
        - ack
        - nack
        - shutdown

::: sqlspec.extensions.events._store.BaseEventQueueStore
    options:
      heading_level: 3
      members: false

::: sqlspec.extensions.events._queue.build_queue_backend
    options:
      heading_level: 4

## Runtime Hints, Protocols & Payload Helpers

::: sqlspec.extensions.events._hints.EventRuntimeHints
    options:
      heading_level: 4
      members: false

::: sqlspec.extensions.events._hints.get_runtime_hints
    options:
      heading_level: 4

::: sqlspec.extensions.events._protocols.SyncEventBackendProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.extensions.events._protocols.AsyncEventBackendProtocol
    options:
      heading_level: 4
      members: false

::: sqlspec.extensions.events._payload.encode_notify_payload
    options:
      heading_level: 4

::: sqlspec.extensions.events._payload.decode_notify_payload
    options:
      heading_level: 4

::: sqlspec.extensions.events._payload.fits_notify_payload
    options:
      heading_level: 4

::: sqlspec.extensions.events._payload.measure_notify_payload
    options:
      heading_level: 4
