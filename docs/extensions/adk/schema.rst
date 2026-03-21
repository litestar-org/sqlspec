======
Schema
======

ADK stores create tables for sessions, events, memory entries, and artifact
metadata. Table names are configurable via ``extension_config["adk"]``.

Use ``create_tables()`` or ``ensure_tables()`` on a store to apply the schema.

.. contents:: On this page
   :local:
   :depth: 2

Sessions Table
==============

The sessions table stores agent session metadata and durable state.

Default name: ``adk_sessions``

.. list-table::
   :header-rows: 1

   * - Column
     - Type
     - Description
   * - ``id``
     - ``VARCHAR`` / ``TEXT``
     - Primary key. UUID assigned by the service layer.
   * - ``app_name``
     - ``VARCHAR`` / ``TEXT``
     - Application identifier.
   * - ``user_id``
     - ``VARCHAR`` / ``TEXT``
     - User identifier.
   * - ``state``
     - ``JSONB`` / ``JSON`` / ``TEXT``
     - Durable session state (see :ref:`scoped-state`).
   * - ``create_time``
     - ``TIMESTAMP``
     - When the session was created (UTC).
   * - ``update_time``
     - ``TIMESTAMP``
     - Last state update time (UTC).

An optional ``owner_id`` column can be added via ``owner_id_column`` in the ADK
config for multi-tenant deployments.

.. _event-record:

Events Table (EventRecord)
==========================

The events table uses **full-event JSON storage**: the entire ADK ``Event`` is
serialized into a single ``event_json`` column alongside a small set of indexed
scalar columns used for query filtering.

This design eliminates column drift with upstream ADK releases. New ``Event``
fields are automatically captured in ``event_json`` without schema changes.

Default name: ``adk_events``

.. list-table::
   :header-rows: 1

   * - Column
     - Type
     - Description
   * - ``session_id``
     - ``VARCHAR`` / ``TEXT``
     - Foreign key to the sessions table.
   * - ``invocation_id``
     - ``VARCHAR`` / ``TEXT``
     - ADK invocation identifier (indexed for filtering).
   * - ``author``
     - ``VARCHAR`` / ``TEXT``
     - Event author: ``"user"``, ``"agent"``, or ``"system"``.
   * - ``timestamp``
     - ``TIMESTAMP``
     - Event timestamp (UTC, indexed for range queries).
   * - ``event_json``
     - ``JSONB`` / ``JSON`` / ``TEXT``
     - Full ADK Event serialized via ``Event.model_dump()``.

**Serialization and reconstruction:**

Events are converted to records via ``event_to_record()``, which calls
``event.model_dump(exclude_none=True, mode="json")`` to produce the JSON blob.
Reconstruction is lossless: ``record_to_event()`` restores the full ``Event``
via ``Event.model_validate()``.

.. code-block:: python

   from sqlspec.extensions.adk.converters import event_to_record, record_to_event

   # Serialize: Event -> EventRecord
   record = event_to_record(event=adk_event, session_id="sess_123")

   # Reconstruct: EventRecord -> Event
   restored_event = record_to_event(record)

.. _scoped-state:

Scoped State Semantics
======================

ADK uses key prefixes to scope state visibility across sessions. SQLSpec
respects these prefixes when persisting and loading state.

.. list-table::
   :header-rows: 1

   * - Prefix
     - Scope
     - Persisted
     - Description
   * - ``app:``
     - Application
     - Yes
     - Shared across all sessions for the same ``app_name``.
   * - ``user:``
     - User
     - Yes
     - Shared across all sessions for the same ``app_name`` + ``user_id``.
   * - ``temp:``
     - Runtime
     - **No**
     - Process-local state. Stripped before every write to storage.
   * - *(no prefix)*
     - Session
     - Yes
     - Private to a single session.

**How scoped state is handled:**

1. On ``create_session()``, the service strips ``temp:`` keys before the
   initial ``INSERT``.

2. On ``append_event()``, the service calls ``filter_temp_state()`` to produce
   a durable state snapshot, then calls ``append_event_and_update_state()`` to
   atomically persist the event and the state update.

3. On ``get_session()``, state is loaded from the database. Since ``temp:``
   keys were never written, they are absent from the loaded state.

.. code-block:: python

   from sqlspec.extensions.adk.converters import filter_temp_state, split_scoped_state

   state = {
       "app:model_version": "v2",
       "user:preferences": {"theme": "dark"},
       "temp:scratch_pad": "...",
       "conversation_turn": 5,
   }

   # Strip temp keys before persisting
   durable = filter_temp_state(state)
   # {"app:model_version": "v2", "user:preferences": {...}, "conversation_turn": 5}

   # Split into scoped buckets
   app_state, user_state, session_state = split_scoped_state(durable)
   # app_state: {"app:model_version": "v2"}
   # user_state: {"user:preferences": {"theme": "dark"}}
   # session_state: {"conversation_turn": 5}

.. _append-event-contract:

The ``append_event_and_update_state()`` Contract
=================================================

This method is the **authoritative durable write boundary** for post-creation
session mutations. It atomically:

1. Inserts the event record into the events table.
2. Updates the session's durable state in the sessions table.

Both operations succeed together or fail together within a single database
transaction.

.. code-block:: python

   # Called by SQLSpecSessionService.append_event() internally:
   await store.append_event_and_update_state(
       event_record=event_record,
       session_id=session.id,
       state=durable_state,  # temp: keys already stripped
   )

**Why this matters:**

- Prevents state from advancing without the corresponding event being recorded.
- Prevents orphaned events that reference a stale session state.
- Ensures that on session reload, the state always reflects all persisted events.

Every backend store implements this as a single transaction (or equivalent
atomic operation for the backend's concurrency model).

Memory Table
============

The memory table stores long-term context entries that agents can search and
reference across sessions.

Default name: ``adk_memory_entries``

.. list-table::
   :header-rows: 1

   * - Column
     - Type
     - Description
   * - ``id``
     - ``VARCHAR`` / ``TEXT``
     - Primary key.
   * - ``session_id``
     - ``VARCHAR`` / ``TEXT``
     - Session that produced this memory.
   * - ``app_name``
     - ``VARCHAR`` / ``TEXT``
     - Application identifier.
   * - ``user_id``
     - ``VARCHAR`` / ``TEXT``
     - User identifier.
   * - ``content_text``
     - ``TEXT``
     - Searchable text content (used by FTS).
   * - ``content_json``
     - ``JSONB`` / ``JSON`` / ``TEXT``
     - Structured content.
   * - ``inserted_at``
     - ``TIMESTAMP``
     - When the entry was created.

When ``memory_use_fts`` is enabled in the ADK config, backends create
full-text search indexes on ``content_text`` using the database's native
FTS engine (tsvector, FTS5, InnoDB FT, etc.).

.. _artifact-schema:

Artifact Metadata Table
=======================

The artifact table stores versioning metadata for binary artifacts. Content
bytes are stored separately in object storage; this table tracks ownership,
versioning, and canonical URIs.

Default name: ``adk_artifact_versions``

.. list-table::
   :header-rows: 1

   * - Column
     - Type
     - Description
   * - ``app_name``
     - ``VARCHAR`` / ``TEXT``
     - Application identifier.
   * - ``user_id``
     - ``VARCHAR`` / ``TEXT``
     - User identifier.
   * - ``session_id``
     - ``VARCHAR`` / ``TEXT`` (nullable)
     - Session identifier. NULL for user-scoped artifacts.
   * - ``filename``
     - ``VARCHAR`` / ``TEXT``
     - Artifact filename.
   * - ``version``
     - ``INTEGER``
     - Monotonically increasing version (starts at 0).
   * - ``mime_type``
     - ``VARCHAR`` / ``TEXT`` (nullable)
     - MIME type of the artifact content.
   * - ``canonical_uri``
     - ``VARCHAR`` / ``TEXT``
     - URI pointing to content in object storage.
   * - ``custom_metadata``
     - ``JSONB`` / ``JSON`` / ``TEXT`` (nullable)
     - User-defined metadata.
   * - ``created_at``
     - ``TIMESTAMP``
     - When this version was created.

The composite key is ``(app_name, user_id, session_id, filename, version)``.

Table Name Configuration
========================

All table names are configurable:

.. code-block:: python

   config = AsyncpgConfig(
       connection_config={"dsn": "postgresql://..."},
       extension_config={
           "adk": {
               "session_table": "my_sessions",        # default: "adk_sessions"
               "events_table": "my_events",            # default: "adk_events"
               "memory_table": "my_memory",            # default: "adk_memory_entries"
               "artifact_table": "my_artifacts",       # default: "adk_artifact_versions"
           }
       },
   )

Table names are validated on store initialization: they must start with a
letter or underscore, contain only alphanumeric characters and underscores,
and be at most 63 characters long.
