======
Events
======

Pub/sub event channel system with database-backed queue support. Provides
both sync and async channels with listener management and native backend
integration for databases that support LISTEN/NOTIFY.

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
