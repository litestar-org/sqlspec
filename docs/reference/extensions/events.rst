======
Events
======

Pub/sub event channel system with database-backed queue support. Provides
both sync and async channels with listener management and native backend
integration for databases that support LISTEN/NOTIFY.

Native LISTEN/NOTIFY model
==========================

Native PG event backends (``asyncpg``, ``psycopg`` async/sync, ``psqlpy``)
hold a **single persistent LISTEN connection per backend instance**. Each
backend owns its own listener hub that:

* Acquires the dedicated LISTEN connection lazily on first subscribe.
* Emits ``LISTEN <channel>`` exactly once per channel and ``UNLISTEN`` on
  unsubscribe / shutdown.
* Dispatches incoming notifications into per-channel ``asyncio.Queue``
  instances (or ``queue.Queue`` for the sync psycopg variant).
* Serializes subscribe / unsubscribe under a lock so concurrent callers
  cannot race on driver-level statements that share the connection.

The Oracle Advanced Queuing backend uses an analogous pattern: a
per-channel queue-handle cache backed by a single dedicated session per
backend instance. ``dequeue`` honors ``min(poll_interval, aq_wait_seconds)``
as its wait bound so the caller's polling cadence is respected.

``ack`` / ``nack`` semantics are unchanged. Native backends remain
fire-and-forget; hybrid (``listen_notify_durable``) backends acknowledge
through the durable table queue.

Channels
========

.. autoclass:: sqlspec.extensions.events.AsyncEventChannel
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.extensions.events.SyncEventChannel
   :members:
   :show-inheritance:

Listeners
=========

.. autoclass:: sqlspec.extensions.events.AsyncEventListener
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.extensions.events.SyncEventListener
   :members:
   :show-inheritance:

Event Queue
===========

.. autoclass:: sqlspec.extensions.events.AsyncTableEventQueue
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.extensions.events.SyncTableEventQueue
   :members:
   :show-inheritance:

.. autofunction:: sqlspec.extensions.events.build_queue_backend

Store
=====

.. autoclass:: sqlspec.extensions.events.BaseEventQueueStore
   :members:
   :show-inheritance:

Models
======

.. autoclass:: sqlspec.extensions.events.EventMessage
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.extensions.events.EventRuntimeHints
   :members:
   :show-inheritance:

Protocols
=========

.. autoclass:: sqlspec.extensions.events.AsyncEventBackendProtocol
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.extensions.events.SyncEventBackendProtocol
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.extensions.events.AsyncEventHandler
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.extensions.events.SyncEventHandler
   :members:
   :show-inheritance:

Payload Helpers
===============

.. autofunction:: sqlspec.extensions.events.encode_notify_payload

.. autofunction:: sqlspec.extensions.events.decode_notify_payload

.. autofunction:: sqlspec.extensions.events.parse_event_timestamp

Utility Functions
=================

.. autofunction:: sqlspec.extensions.events.load_native_backend

.. autofunction:: sqlspec.extensions.events.resolve_poll_interval

.. autofunction:: sqlspec.extensions.events.normalize_event_channel_name

.. autofunction:: sqlspec.extensions.events.normalize_queue_table_name
