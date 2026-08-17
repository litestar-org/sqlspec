==========
Quickstart
==========

Wire SQLSpec stores into your ADK agent to persist sessions, events, and memory
across restarts.

How It Works
============

1. Create a SQLSpec database config with ADK extension settings.
2. Initialize the appropriate stores (session and memory).
3. Pass the service wrappers to your ADK agent.

Session Service
===============

The session service persists agent state and events between conversations.
When a user returns, the agent can resume from where it left off.

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore
   from sqlspec.extensions.adk import SQLSpecSessionService

   config = AsyncpgConfig(
       connection_config={"dsn": "postgresql://localhost/mydb"},
       extension_config={
           "adk": {
               "session_table": "adk_session",
               "events_table": "adk_event",
           }
       },
   )

   store = AsyncpgADKStore(config)
   await store.ensure_tables()

   session_service = SQLSpecSessionService(store)

   # Create a session with scoped state
   session = await session_service.create_session(
       app_name="my_agent",
       user_id="user_123",
       state={
           "app:model": "gemini-2.0",      # shared across all sessions
           "user:name": "Alice",            # shared across user's sessions
           "conversation_turn": 0,          # session-local
           "temp:scratch": "...",           # runtime-only, never persisted
       },
   )

Events are persisted automatically when you use the session service with an
ADK runner. Each call to ``append_event()`` atomically stores the event and
updates the session's durable state via ``append_event_and_update_state()``.

Listing Sessions
----------------

``SQLSpecSessionService.list_sessions()`` accepts optional ordering and paging
arguments in addition to Google ADK's ``app_name`` and ``user_id``. They are a
SQLSpec extension to ADK's narrower base signature, and the ordering and page
bounds are applied in SQL rather than by trimming a fully materialized result.

.. code-block:: python

   # Most recently updated sessions for one user, 20 at a time
   page = await session_service.list_sessions(
       app_name="my_agent",
       user_id="user_123",
       limit=20,
       offset=0,
   )

   # Oldest conversations first
   oldest = await session_service.list_sessions(
       app_name="my_agent",
       user_id="user_123",
       order_by="create_time",
       descending=False,
       limit=20,
   )

The contract is deliberately narrow:

- The default is unchanged: ``update_time`` descending, with no page bounds.
- ``order_by`` accepts only ``create_time`` or ``update_time``. No other value
  and no raw SQL expression reaches a query.
- ``id`` is applied as a secondary sort key in the same direction, so pages stay
  deterministic when two sessions share a timestamp.
- ``limit`` and ``offset`` accept non-negative integers. ``limit=0`` returns an
  empty response without touching the database, and a positive ``offset``
  requires a finite ``limit``.
- The response is Google ADK's ``ListSessionsResponse``. It carries no total
  count and no cursor token, so paginate by advancing ``offset`` yourself.

Scoped State
------------

State keys use prefixes to control their scope and persistence:

- ``app:`` -- shared across all sessions for the same application.
- ``user:`` -- shared across all sessions for the same user.
- ``temp:`` -- runtime-only, stripped before every write to storage.
- *(no prefix)* -- private to the current session.

See :ref:`scoped-state` for full details.

Memory Service
==============

The memory service retains context that the agent can reference later. This
enables long-term memory across sessions with full-text search.

.. code-block:: python

   from sqlspec.adapters.asyncpg.adk import AsyncpgADKMemoryStore
   from sqlspec.extensions.adk import SQLSpecMemoryService

   memory_store = AsyncpgADKMemoryStore(config)
   await memory_store.ensure_tables()

   memory_service = SQLSpecMemoryService(memory_store)

Enable full-text search by setting ``memory_use_fts: True`` in the ADK config.
This creates database-native FTS indexes (tsvector, FTS5, InnoDB FT) for
efficient memory retrieval.

Artifact Service
================

The artifact service contracts live in ``sqlspec.extensions.adk.artifact`` and
separate SQL metadata from object-storage content. Use them when your
deployment provides a concrete artifact metadata store; adapter ``adk``
packages currently export session/event and memory stores only.

Schema Setup
============

You can programmatically create ADK tables ahead of first use with
``ensure_tables()``:

.. code-block:: python

   await session_store.ensure_tables()
   await memory_store.ensure_tables()

Alternatively, configure SQLSpec migrations for your database and run the
migration CLI as part of deployment:

.. code-block:: console

   sqlspec upgrade

Next Steps
==========

- :doc:`backends` for the full support matrix and backend-specific details.
- :doc:`schema` for table layouts, StoredEvent format, and scoped state semantics.
- :doc:`api` for the complete API reference.
