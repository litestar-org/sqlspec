======
Schema
======

ADK stores create tables for sessions, events, app-scoped state, user-scoped
state, internal metadata, and memory entries. The artifact metadata table
contract is documented for deployments that provide a concrete artifact
metadata store. Table names are configurable via ``extension_config["adk"]``.

You can programmatically create the schema with ``create_tables()`` or
``ensure_tables()`` on a store. For managed deployments, configure SQLSpec
migrations for the target database and run ``sqlspec upgrade`` instead.

When ADK migrations are enabled from ``extension_config["adk"]``, SQLSpec
checks that the selected adapter has the session and memory store classes used
to generate this schema before the migration starts.

.. contents:: On this page
   :local:
   :depth: 2

Sessions Table
==============

The sessions table stores agent session metadata and durable state.

Default name: ``adk_session``

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

Events Table (StoredEvent)
==========================

The events table uses **full-event JSON storage**: the entire ADK ``Event`` is
serialized into a single ``event_data`` column alongside a small set of indexed
scalar columns used for query filtering.

This design eliminates column drift with upstream ADK releases. New ``Event``
fields are automatically captured in ``event_data`` without schema changes.

Default name: ``adk_event``

.. list-table::
   :header-rows: 1

   * - Column
     - Type
     - Description
   * - ``id``
     - ``VARCHAR`` / ``TEXT``
     - Event identifier.
   * - ``app_name``
     - ``VARCHAR`` / ``TEXT``
     - Application identifier.
   * - ``user_id``
     - ``VARCHAR`` / ``TEXT``
     - User identifier.
   * - ``session_id``
     - ``VARCHAR`` / ``TEXT``
     - Foreign key to the sessions table.
   * - ``invocation_id``
     - ``VARCHAR`` / ``TEXT``
     - ADK invocation identifier (indexed for filtering).
   * - ``timestamp``
     - ``TIMESTAMP``
     - Event timestamp (UTC, indexed for range queries).
   * - ``event_data``
     - ``JSONB`` / ``JSON`` / ``TEXT``
     - Full ADK Event serialized via ``Event.model_dump()``.

**Serialization and reconstruction:**

Events are converted to records via ``event_to_record()``, which calls
``event.model_dump(exclude_none=True, mode="json")`` to produce the JSON blob.
Reconstruction is lossless: ``record_to_event()`` restores the full ``Event``
via ``Event.model_validate()``.

.. code-block:: python

   from sqlspec.extensions.adk.converters import event_to_record, record_to_event

   # Serialize: Event -> StoredEvent
   record = event_to_record(
       event=adk_event,
       app_name="my_agent",
       user_id="user_123",
       session_id="sess_123",
   )

   # Reconstruct: StoredEvent -> Event
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
   initial write, splits ``app:`` and ``user:`` keys into scoped buckets, and
   stores only session-local keys in the session row.

2. On ``append_event()``, the service calls ``filter_temp_state()`` to produce
   a durable state snapshot, splits scoped state, then calls
   ``append_event_and_update_state()`` to atomically persist the event,
   session state, app state, and user state update.

3. On ``get_session()``, the service loads session-local state plus matching
   app/user state rows, then merges them into the ADK state view. Since
   ``temp:`` keys were never written, they are absent from the loaded state.

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
3. Upserts app-scoped state when ``app:`` keys changed.
4. Upserts user-scoped state when ``user:`` keys changed.

All operations succeed together or fail together within a single database
transaction.

.. code-block:: python

   # Called by SQLSpecSessionService.append_event() internally:
   await store.append_event_and_update_state(
       event_record=event_record,
       app_name=session.app_name,
       user_id=session.user_id,
       session_id=session.id,
       state=session_state,  # temp: keys already stripped
       app_state=app_state,
       user_state=user_state,
   )

**Why this matters:**

- Prevents state from advancing without the corresponding event being recorded.
- Prevents orphaned events that reference a stale session state.
- Ensures that on session reload, the state always reflects all persisted events.

Every backend store implements this as a single transaction (or equivalent
atomic operation for the backend's concurrency model).

Scoped State Tables
===================

App and user state are stored separately from session-local state so the
service can merge shared state across sessions.

Default app-state table name: ``adk_app_state``

Default user-state table name: ``adk_user_state``

Both tables store a JSON state document and an ``update_time`` timestamp. The
app-state table is keyed by ``app_name``. The user-state table is keyed by
``app_name`` and ``user_id``.

Internal Metadata Table
=======================

The internal metadata table stores SQLSpec ADK schema bookkeeping such as the
current schema version.

Default name: ``adk_internal_metadata``

Memory Table
============

The memory table stores long-term context entries that agents can search and
reference across sessions.

Default name: ``adk_memory``

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
   * - ``scope``
     - ``VARCHAR`` / ``TEXT``
     - Scope visibility: ``'user'`` (default) or ``'app'``.
   * - ``event_id``
     - ``VARCHAR`` / ``TEXT``
     - Unique event identifier for deduplication on ingest.
   * - ``author``
     - ``VARCHAR`` / ``TEXT``
     - Author or role of the originating event (e.g. ``user``, ``model``).
   * - ``timestamp``
     - ``TIMESTAMP`` / ``TIMESTAMPTZ``
     - Event timestamp.
   * - ``embedding``
     - ``VECTOR`` (or dialect array)
     - Vector embedding of size ``vector_dimensions`` for semantic recall.
   * - ``content_json``
     - ``JSONB`` / ``JSON`` / ``TEXT``
     - Full structured ADK Content object.
   * - ``content_text``
     - ``TEXT``
     - Plain text extracted from content parts (used by FTS/BM25).
   * - ``metadata_json``
     - ``JSONB`` / ``JSON`` / ``TEXT`` (nullable)
     - Custom or entry-level metadata dictionary.
   * - ``inserted_at``
     - ``TIMESTAMP`` / ``TIMESTAMPTZ``
     - When the entry was inserted (UTC).

An optional ``owner_id`` column can be added via ``owner_id_column`` in the ADK
config for multi-tenant isolation.

.. _pgvector-recall:

pgvector Recall and Search Strategies
=====================================

When querying memories via :meth:`~sqlspec.extensions.adk.SQLSpecMemoryService.search_memory`,
SQLSpec supports multiple retrieval modes depending on adapter capabilities and configuration:

1. **Semantic Vector Search (pgvector)**
   When an ``embedding`` vector is passed, PostgreSQL uses the pgvector cosine distance
   operator (``embedding <=> :query_vector::vector``) to rank entries by semantic similarity:

   .. code-block:: sql

      SELECT * FROM adk_memory
      WHERE app_name = $1 AND ((scope = 'user' AND user_id = $2) OR scope = 'app')
        AND embedding IS NOT NULL
      ORDER BY embedding <=> $3::float8[]::vector ASC, timestamp DESC
      LIMIT $4

   Vector indexes are configured via ``vector_index_type``:
   - ``"hnsw"`` (default): Hierarchical Navigable Small World graphs with ``vector_cosine_ops``.
   - ``"ivfflat"``: Inverted file index with vector cosine distance.
   - ``"scann"``: AlloyDB/ScaNN tree quantization index (tuned via ``scann_num_leaves`` and ``scann_quantizer``).

2. **Full-Text Search (FTS)**
   When ``memory_use_fts`` is ``True``, backends create native full-text indexes (GIN on
   ``to_tsvector('english', content_text)`` on PostgreSQL, FTS5 on SQLite, InnoDB FT on MySQL,
   Oracle Text on Oracle, or ``TOKENIZE_FULLTEXT`` on Spanner). Queries execute using native
   stemmed text matching.

3. **Hybrid Search with Reciprocal Rank Fusion (RRF)**
   When both a text ``query`` and a vector ``embedding`` are provided and ``enable_bm25=True``
   (using PostgreSQL 17/18 with ``pg_textsearch`` or AlloyDB), SQLSpec executes a hybrid query
   fusing dense vector search and sparse BM25 text rank using Reciprocal Rank Fusion:

   .. code-block:: sql

      WITH vector_matches AS (
          SELECT id, RANK() OVER (ORDER BY embedding <=> $3::float8[]::vector) AS rank_vec
          FROM adk_memory
          WHERE ... AND embedding IS NOT NULL
          LIMIT $5
      ),
      text_matches AS (
          SELECT id, RANK() OVER (ORDER BY content_text <@> $4) AS rank_txt
          FROM adk_memory
          WHERE ...
          LIMIT $5
      )
      SELECT m.*, (COALESCE(1.0 / (60 + v.rank_vec), 0.0) + COALESCE(1.0 / (60 + t.rank_txt), 0.0)) AS rrf_score
      FROM adk_memory m
      LEFT JOIN vector_matches v ON m.id = v.id
      LEFT JOIN text_matches t ON m.id = t.id
      WHERE v.id IS NOT NULL OR t.id IS NOT NULL
      ORDER BY rrf_score DESC, m.timestamp DESC
      LIMIT $6

4. **Simple Fallback**
   When FTS and vector search are not configured, queries fall back to case-insensitive
   substring matching (``content_text ILIKE %query%``).

Indexes Created on Memory Table
-------------------------------

- ``idx_{table}_app_scope_user_time``: composite index on ``(app_name, scope, user_id, timestamp DESC)``
- ``idx_{table}_scope``: composite index on ``(app_name, scope)``
- ``idx_{table}_session``: index on ``(session_id)``
- ``idx_{table}_fts``: GIN index on ``to_tsvector('english', content_text)`` (when ``memory_use_fts=True``)
- ``idx_{table}_bm25``: BM25 index on ``content_text`` (when ``enable_bm25=True``)
- ``idx_{table}_{index_type}``: HNSW, IVFFlat, or ScaNN vector index on ``embedding``

.. _artifact-schema:

Artifact Metadata Table Contract
================================

Concrete artifact metadata stores use this table shape to store versioning
metadata for binary artifacts. Content bytes are stored separately in object
storage; this table tracks ownership, versioning, and canonical URIs.

Default name: ``adk_artifact``

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

Table and Extension Configuration
=================================

All table names and extension settings are configured through ``extension_config["adk"]``:

.. code-block:: python

   config = AsyncpgConfig(
       connection_config={"dsn": "postgresql://..."},
       extension_config={
           "adk": {
               # Table name overrides
               "session_table": "adk_session",
               "events_table": "adk_event",
               "app_state_table": "adk_app_state",
               "user_state_table": "adk_user_state",
               "metadata_table": "adk_internal_metadata",
               "memory_table": "adk_memory",
               "artifact_table": "adk_artifact",
               # Multi-tenant isolation
               "owner_id_column": "tenant_id INTEGER NOT NULL",
               # Memory search & pgvector configuration
               "memory_use_fts": True,
               "memory_max_results": 20,
               "vector_index_type": "hnsw",  # 'hnsw', 'ivfflat', or 'scann'
               "vector_dimensions": 768,      # embedding dimension
               "enable_bm25": False,          # requires pg_textsearch
               "scann_num_leaves": 100,
               "scann_quantizer": "SQ8",
               # Artifact storage
               "artifact_storage_uri": "s3://my-bucket/adk-artifacts",
               # Schema lifecycle
               "manage_schema": True,
               "create_schema": True,
               "enable_sessions": True,
               "enable_memory": True,
           }
       },
   )

Table names are validated on store initialization: they must start with a
letter or underscore, contain only alphanumeric characters and underscores,
and be at most 63 characters long.
