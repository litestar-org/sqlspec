==========
Quickstart
==========

Wire SQLSpec stores into your ADK agent to persist sessions, events, memory,
and artifacts across restarts.

How It Works
============

1. Create a SQLSpec database config with ADK extension settings.
2. Initialize the appropriate stores (session, memory, artifact).
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
               "session_table": "adk_sessions",
               "events_table": "adk_events",
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

The artifact service stores binary artifacts (files, images, reports) with
automatic versioning. Metadata lives in SQL; content lives in object storage.

.. code-block:: python

   from sqlspec.adapters.asyncpg.adk import AsyncpgADKArtifactStore
   from sqlspec.extensions.adk import SQLSpecArtifactService

   artifact_store = AsyncpgADKArtifactStore(config)
   await artifact_store.ensure_table()

   artifact_service = SQLSpecArtifactService(
       store=artifact_store,
       artifact_storage_uri="s3://my-bucket/adk-artifacts/",
   )

   # Save an artifact (returns version number starting from 0)
   version = await artifact_service.save_artifact(
       app_name="my_agent",
       user_id="user_123",
       filename="report.pdf",
       artifact=part,
   )

   # Load the latest version
   loaded = await artifact_service.load_artifact(
       app_name="my_agent",
       user_id="user_123",
       filename="report.pdf",
   )

Schema Setup
============

You can programmatically create ADK tables ahead of first use with
``ensure_tables()`` / ``ensure_table()``:

.. code-block:: python

   await session_store.ensure_tables()
   await memory_store.ensure_tables()
   await artifact_store.ensure_table()

Alternatively, configure SQLSpec migrations for your database and run the
migration CLI as part of deployment:

.. code-block:: console

   sqlspec upgrade

Next Steps
==========

- :doc:`backends` for the full support matrix and backend-specific details.
- :doc:`schema` for table layouts, EventRecord format, and scoped state semantics.
- :doc:`api` for the complete API reference.
