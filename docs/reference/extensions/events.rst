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

The Oracle native backends (``aq`` and
``txeventq``) use an analogous pattern: a per-channel
queue-handle cache backed by a single dedicated session per backend instance.
``dequeue`` honors ``min(poll_interval, aq_wait_seconds)`` as its wait bound so
the caller's polling cadence is respected.

``ack`` / ``nack`` semantics are unchanged. Native backends remain
fire-and-forget; hybrid (``listen_notify_durable``) backends acknowledge
through the durable table queue.

Oracle native event backends
============================

Oracle provides two **native** messaging backends in addition to the default
``table_queue``:

* ``aq`` — classic Oracle Advanced Queuing (AQ).
* ``txeventq`` — Oracle Transactional Event Queues (TxEventQ).

Both share the same client path and JSON payloads; they differ only in how the
underlying queue is provisioned. Select one via ``events.backend``:

.. code-block:: python

    from sqlspec.adapters.oracledb import OracleAsyncConfig

    config = OracleAsyncConfig(
        connection_config={"dsn": "..."},
        extension_config={"events": {"backend": "txeventq"}},
    )

The default remains ``table_queue``, which works on every Oracle edition
without extra privileges; both native backends are opt-in.

Requirements
------------

* **Thin mode** — both backends run in python-oracledb's default Thin mode; no
  Instant Client / Thick mode is required.
* **JSON payloads** require **Oracle Database 21c or newer** (23ai satisfies this).
* **Privileges** — the connecting user needs ``DBMS_AQADM`` access. Grant
  ``aq_administrator_role, aq_user_role`` and ``EXECUTE ON dbms_aq``.

Provisioning
------------

The backend attaches to an existing queue; it does not create one. Provision the
queue with ``DBMS_AQADM`` first:

* ``aq`` — ``create_queue_table(queue_payload_type => 'JSON')`` +
  ``create_queue`` + ``start_queue``.
* ``txeventq`` —
  ``create_transactional_event_queue(queue_payload_type => 'JSON', multiple_consumers => FALSE)``
  + ``start_queue``.

By default all channels route through a single physical queue
(``SQLSPEC_EVENTS_QUEUE``) with the channel carried in the event envelope. To
isolate channels onto per-channel physical queues, template the queue name with
``{channel}`` via the ``aq_queue`` setting (for example
``"aq_queue": "SQLSPEC_EVT_{channel}"``) and provision one queue per channel.

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
