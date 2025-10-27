================
Schema Reference
================

This document provides a complete reference for the ADK extension database schema.

Overview
========

The ADK extension uses a two-table schema:

1. **Sessions Table** (``adk_sessions``) - Stores session metadata and state
2. **Events Table** (``adk_events``) - Stores conversation events with foreign key to sessions

Both tables are designed for:

- Efficient querying by app and user
- ACID transaction support
- Concurrent read/write access
- JSON storage for flexible metadata

Sessions Table
==============

The sessions table stores session metadata and state for each AI agent conversation.

Table Name
----------

**Default:** ``adk_sessions``

**Customizable:** Yes, via store constructor

Field Definitions
-----------------

.. list-table::
   :header-rows: 1
   :widths: 15 15 10 60

   * - Field
     - Type
     - Nullable
     - Description
   * - ``id``
     - VARCHAR(128)
     - No
     - Unique session identifier (typically UUID). Primary key.
   * - ``app_name``
     - VARCHAR(128)
     - No
     - Application name identifying the AI agent.
   * - ``user_id``
     - VARCHAR(128)
     - No
     - User identifier owning the session.
   * - ``<owner_id_column>``
     - (Configurable)
     - Depends
     - **Optional**: Custom FK column to link sessions to your user table. See :ref:`user-fk-column-feature`.
   * - ``state``
     - JSON/JSONB
     - No
     - Session state dictionary (default: ``{}``)
   * - ``create_time``
     - TIMESTAMP
     - No
     - Session creation timestamp (UTC, microsecond precision)
   * - ``update_time``
     - TIMESTAMP
     - No
     - Last update timestamp (UTC, auto-updated)

.. _user-fk-column-feature:

User Foreign Key Column (Optional)
-----------------------------------

The sessions table can include an **optional owner ID column** to link sessions to your
application's user table. This enables:

- **Referential integrity**: Database enforces valid user references
- **Cascade deletes**: Automatically remove sessions when users are deleted
- **Multi-tenancy**: Isolate sessions by tenant/organization/workspace
- **Join queries**: Efficiently query sessions with user metadata

Configuration:
  The ``owner_id_column`` parameter accepts a complete column DDL definition:

  .. code-block:: python

     store = AsyncpgADKStore(
         config,
         owner_id_column="account_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE"
     )

Column Name Parsing:
  The first word of the DDL is extracted as the column name for INSERT/SELECT operations.
  The entire DDL is used verbatim in CREATE TABLE statements.

Format:
  ``"column_name TYPE [NOT NULL] REFERENCES table(column) [ON DELETE ...]"``

Examples by Database:

- **PostgreSQL**: ``"account_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE"``
- **MySQL**: ``"user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE"``
- **SQLite**: ``"tenant_id INTEGER NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"``
- **Oracle**: ``"user_id NUMBER(10) REFERENCES users(id) ON DELETE CASCADE"``
- **Nullable**: ``"workspace_id UUID REFERENCES workspaces(id) ON DELETE SET NULL"``

See :doc:`quickstart` for complete usage examples.

Indexes
-------

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Index Name
     - Type
     - Purpose
   * - ``PRIMARY KEY (id)``
     - B-tree
     - Fast lookups by session ID
   * - ``idx_adk_sessions_app_user``
     - Composite
     - Efficient listing by (app_name, user_id)
   * - ``idx_adk_sessions_update_time``
     - B-tree DESC
     - Recent sessions queries
   * - ``idx_adk_sessions_state``
     - GIN (PostgreSQL)
     - JSONB queries on state (partial index)

Database-Specific Schema
------------------------

PostgreSQL
^^^^^^^^^^

**Base Schema (without owner ID column):**

.. code-block:: sql

   CREATE TABLE adk_sessions (
       id VARCHAR(128) PRIMARY KEY,
       app_name VARCHAR(128) NOT NULL,
       user_id VARCHAR(128) NOT NULL,
       state JSONB NOT NULL DEFAULT '{}'::jsonb,
       create_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
       update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
   ) WITH (fillfactor = 80);

   CREATE INDEX idx_adk_sessions_app_user
       ON adk_sessions(app_name, user_id);

   CREATE INDEX idx_adk_sessions_update_time
       ON adk_sessions(update_time DESC);

   CREATE INDEX idx_adk_sessions_state
       ON adk_sessions USING GIN (state)
       WHERE state != '{}'::jsonb;

**With Owner ID Column:**

.. code-block:: sql

   CREATE TABLE adk_sessions (
       id VARCHAR(128) PRIMARY KEY,
       app_name VARCHAR(128) NOT NULL,
       user_id VARCHAR(128) NOT NULL,
       account_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
       state JSONB NOT NULL DEFAULT '{}'::jsonb,
       create_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
       update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
   ) WITH (fillfactor = 80);

   -- Indexes...

**Notes:**

- ``JSONB`` type for efficient JSON operations
- ``TIMESTAMPTZ`` for timezone-aware timestamps
- ``FILLFACTOR 80`` leaves space for HOT updates
- Partial GIN index excludes empty states
- User FK column is inserted after ``user_id`` when configured

MySQL
^^^^^

.. code-block:: sql

   CREATE TABLE adk_sessions (
       id VARCHAR(128) PRIMARY KEY,
       app_name VARCHAR(128) NOT NULL,
       user_id VARCHAR(128) NOT NULL,
       state JSON NOT NULL,
       create_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
       update_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
           ON UPDATE CURRENT_TIMESTAMP(6),
       INDEX idx_adk_sessions_app_user (app_name, user_id),
       INDEX idx_adk_sessions_update_time (update_time DESC)
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

**Notes:**

- ``JSON`` type (MySQL 5.7.8+)
- ``TIMESTAMP(6)`` for microsecond precision
- ``ON UPDATE`` auto-updates ``update_time``
- InnoDB engine required for foreign keys

SQLite
^^^^^^

.. code-block:: sql

   CREATE TABLE adk_sessions (
       id TEXT PRIMARY KEY,
       app_name TEXT NOT NULL,
       user_id TEXT NOT NULL,
       state TEXT NOT NULL DEFAULT '{}',
       create_time REAL NOT NULL DEFAULT (julianday('now')),
       update_time REAL NOT NULL DEFAULT (julianday('now'))
   );

   CREATE INDEX idx_adk_sessions_app_user
       ON adk_sessions(app_name, user_id);

   CREATE INDEX idx_adk_sessions_update_time
       ON adk_sessions(update_time DESC);

**Notes:**

- ``TEXT`` for all string fields
- ``REAL`` for Julian Day timestamps
- JSON stored as TEXT, use ``json_extract()`` for queries

Oracle
^^^^^^

.. code-block:: sql

   CREATE TABLE adk_sessions (
       id VARCHAR2(128) PRIMARY KEY,
       app_name VARCHAR2(128) NOT NULL,
       user_id VARCHAR2(128) NOT NULL,
       state CLOB NOT NULL,
       create_time TIMESTAMP(6) DEFAULT SYSTIMESTAMP NOT NULL,
       update_time TIMESTAMP(6) DEFAULT SYSTIMESTAMP NOT NULL
   );

   CREATE INDEX idx_adk_sessions_app_user
       ON adk_sessions(app_name, user_id);

   CREATE INDEX idx_adk_sessions_update_time
       ON adk_sessions(update_time DESC);

**Notes:**

- ``CLOB`` for JSON storage (use ``JSON_VALUE()`` for queries)
- ``TIMESTAMP(6)`` for microsecond precision
- ``SYSTIMESTAMP`` for current time

Events Table
============

The events table stores individual conversation turns with full event data.

Table Name
----------

**Default:** ``adk_events``

**Customizable:** Yes, via store constructor

Field Definitions
-----------------

.. list-table::
   :header-rows: 1
   :widths: 20 15 10 55

   * - Field
     - Type
     - Nullable
     - Description
   * - ``id``
     - VARCHAR(128)
     - No
     - Unique event identifier. Primary key.
   * - ``session_id``
     - VARCHAR(128)
     - No
     - Foreign key to sessions table. Cascade delete.
   * - ``app_name``
     - VARCHAR(128)
     - No
     - Application name (denormalized from session)
   * - ``user_id``
     - VARCHAR(128)
     - No
     - User identifier (denormalized from session)
   * - ``invocation_id``
     - VARCHAR(256)
     - Yes
     - ADK invocation identifier
   * - ``author``
     - VARCHAR(256)
     - Yes
     - Event author (user/assistant/system)
   * - ``branch``
     - VARCHAR(256)
     - Yes
     - Conversation branch identifier
   * - ``actions``
     - BLOB/BYTEA
     - Yes
     - Pickled actions object
   * - ``long_running_tool_ids_json``
     - TEXT
     - Yes
     - JSON-encoded list of long-running tool IDs
   * - ``timestamp``
     - TIMESTAMP
     - No
     - Event timestamp (UTC, microsecond precision)
   * - ``content``
     - JSON/JSONB
     - Yes
     - Event content (parts, text, data)
   * - ``grounding_metadata``
     - JSON/JSONB
     - Yes
     - Grounding metadata from LLM
   * - ``custom_metadata``
     - JSON/JSONB
     - Yes
     - Custom application metadata
   * - ``partial``
     - BOOLEAN
     - Yes
     - Whether event is partial (streaming)
   * - ``turn_complete``
     - BOOLEAN
     - Yes
     - Whether turn is complete
   * - ``interrupted``
     - BOOLEAN
     - Yes
     - Whether event was interrupted
   * - ``error_code``
     - VARCHAR(256)
     - Yes
     - Error code if event failed
   * - ``error_message``
     - VARCHAR(1024)
     - Yes
     - Error message if event failed

Indexes
-------

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Index Name
     - Type
     - Purpose
   * - ``PRIMARY KEY (id)``
     - B-tree
     - Fast lookups by event ID
   * - ``idx_adk_events_session``
     - Composite
     - Efficient queries by (session_id, timestamp ASC)
   * - ``FOREIGN KEY (session_id)``
     - Constraint
     - References adk_sessions(id) ON DELETE CASCADE

Foreign Key Constraint
----------------------

.. code-block:: sql

   FOREIGN KEY (session_id)
       REFERENCES adk_sessions(id)
       ON DELETE CASCADE

**Behavior:**

- Deleting a session automatically deletes all its events
- Ensures referential integrity
- Prevents orphaned events

Database-Specific Schema
------------------------

PostgreSQL
^^^^^^^^^^

.. code-block:: sql

   CREATE TABLE adk_events (
       id VARCHAR(128) PRIMARY KEY,
       session_id VARCHAR(128) NOT NULL,
       app_name VARCHAR(128) NOT NULL,
       user_id VARCHAR(128) NOT NULL,
       invocation_id VARCHAR(256),
       author VARCHAR(256),
       actions BYTEA,
       long_running_tool_ids_json TEXT,
       branch VARCHAR(256),
       timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
       content JSONB,
       grounding_metadata JSONB,
       custom_metadata JSONB,
       partial BOOLEAN,
       turn_complete BOOLEAN,
       interrupted BOOLEAN,
       error_code VARCHAR(256),
       error_message VARCHAR(1024),
       FOREIGN KEY (session_id)
           REFERENCES adk_sessions(id)
           ON DELETE CASCADE
   );

   CREATE INDEX idx_adk_events_session
       ON adk_events(session_id, timestamp ASC);

**Notes:**

- ``BYTEA`` for binary data (no size limit)
- ``BOOLEAN`` native type
- Multiple ``JSONB`` columns for structured data

MySQL
^^^^^

.. code-block:: sql

   CREATE TABLE adk_events (
       id VARCHAR(128) PRIMARY KEY,
       session_id VARCHAR(128) NOT NULL,
       app_name VARCHAR(128) NOT NULL,
       user_id VARCHAR(128) NOT NULL,
       invocation_id VARCHAR(256),
       author VARCHAR(256),
       actions BLOB,
       long_running_tool_ids_json TEXT,
       branch VARCHAR(256),
       timestamp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
       content JSON,
       grounding_metadata JSON,
       custom_metadata JSON,
       partial TINYINT(1),
       turn_complete TINYINT(1),
       interrupted TINYINT(1),
       error_code VARCHAR(256),
       error_message VARCHAR(1024),
       INDEX idx_adk_events_session (session_id, timestamp ASC),
       FOREIGN KEY (session_id)
           REFERENCES adk_sessions(id)
           ON DELETE CASCADE
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

**Notes:**

- ``BLOB`` for binary data
- ``TINYINT(1)`` for boolean values (0/1)
- ``TEXT`` for long strings

SQLite
^^^^^^

.. code-block:: sql

   CREATE TABLE adk_events (
       id TEXT PRIMARY KEY,
       session_id TEXT NOT NULL,
       app_name TEXT NOT NULL,
       user_id TEXT NOT NULL,
       invocation_id TEXT,
       author TEXT,
       actions BLOB,
       long_running_tool_ids_json TEXT,
       branch TEXT,
       timestamp REAL NOT NULL DEFAULT (julianday('now')),
       content TEXT,
       grounding_metadata TEXT,
       custom_metadata TEXT,
       partial INTEGER,
       turn_complete INTEGER,
       interrupted INTEGER,
       error_code TEXT,
       error_message TEXT,
       FOREIGN KEY (session_id)
           REFERENCES adk_sessions(id)
           ON DELETE CASCADE
   );

   CREATE INDEX idx_adk_events_session
       ON adk_events(session_id, timestamp ASC);

**Notes:**

- ``INTEGER`` for boolean values (0/1)
- ``REAL`` for Julian Day timestamps
- JSON stored as ``TEXT``

Oracle
^^^^^^

.. code-block:: sql

   CREATE TABLE adk_events (
       id VARCHAR2(128) PRIMARY KEY,
       session_id VARCHAR2(128) NOT NULL,
       app_name VARCHAR2(128) NOT NULL,
       user_id VARCHAR2(128) NOT NULL,
       invocation_id VARCHAR2(256),
       author VARCHAR2(256),
       actions BLOB,
       long_running_tool_ids_json CLOB,
       branch VARCHAR2(256),
       timestamp TIMESTAMP(6) DEFAULT SYSTIMESTAMP NOT NULL,
       content CLOB,
       grounding_metadata CLOB,
       custom_metadata CLOB,
       partial NUMBER(1),
       turn_complete NUMBER(1),
       interrupted NUMBER(1),
       error_code VARCHAR2(256),
       error_message VARCHAR2(1024),
       CONSTRAINT fk_adk_events_session
           FOREIGN KEY (session_id)
           REFERENCES adk_sessions(id)
           ON DELETE CASCADE
   );

   CREATE INDEX idx_adk_events_session
       ON adk_events(session_id, timestamp ASC);

**Notes:**

- ``NUMBER(1)`` for boolean values (0/1)
- ``CLOB`` for JSON and long text
- ``BLOB`` for binary data

Type Mapping Reference
======================

Python to Database Type Mapping
--------------------------------

.. list-table::
   :header-rows: 1
   :widths: 20 20 20 20 20

   * - Python Type
     - PostgreSQL
     - MySQL
     - SQLite
     - Oracle
   * - ``str`` (ID)
     - VARCHAR(128)
     - VARCHAR(128)
     - TEXT
     - VARCHAR2(128)
   * - ``dict[str, Any]``
     - JSONB
     - JSON
     - TEXT
     - CLOB
   * - ``datetime``
     - TIMESTAMPTZ
     - TIMESTAMP(6)
     - REAL
     - TIMESTAMP(6)
   * - ``bytes``
     - BYTEA
     - BLOB
     - BLOB
     - BLOB
   * - ``bool``
     - BOOLEAN
     - TINYINT(1)
     - INTEGER
     - NUMBER(1)
   * - ``str`` (long)
     - TEXT
     - TEXT
     - TEXT
     - CLOB

Query Patterns
==============

Common Queries
--------------

**Get Session by ID:**

.. code-block:: sql

   SELECT id, app_name, user_id, state, create_time, update_time
   FROM adk_sessions
   WHERE id = ?

**List User's Sessions:**

.. code-block:: sql

   SELECT id, app_name, user_id, state, create_time, update_time
   FROM adk_sessions
   WHERE app_name = ? AND user_id = ?
   ORDER BY update_time DESC

**Get Session Events:**

.. code-block:: sql

   SELECT *
   FROM adk_events
   WHERE session_id = ?
   ORDER BY timestamp ASC

**Recent Events After Timestamp:**

.. code-block:: sql

   SELECT *
   FROM adk_events
   WHERE session_id = ? AND timestamp > ?
   ORDER BY timestamp ASC
   LIMIT 10

JSON Queries (PostgreSQL)
--------------------------

**Find Sessions with Specific State:**

.. code-block:: sql

   SELECT *
   FROM adk_sessions
   WHERE state @> '{"location": "SF"}'::jsonb

**Extract State Value:**

.. code-block:: sql

   SELECT id, state->>'location' as location
   FROM adk_sessions
   WHERE app_name = 'weather_agent'

**Update Nested State:**

.. code-block:: sql

   UPDATE adk_sessions
   SET state = jsonb_set(state, '{settings,theme}', '"dark"')
   WHERE id = ?

Analytics Queries
-----------------

**Session Count by User:**

.. code-block:: sql

   SELECT user_id, COUNT(*) as session_count
   FROM adk_sessions
   WHERE app_name = ?
   GROUP BY user_id
   ORDER BY session_count DESC

**Average Session Duration:**

.. code-block:: sql

   SELECT
       app_name,
       AVG(update_time - create_time) as avg_duration
   FROM adk_sessions
   GROUP BY app_name

**Event Count by Session:**

.. code-block:: sql

   SELECT
       s.id,
       s.user_id,
       COUNT(e.id) as event_count
   FROM adk_sessions s
   LEFT JOIN adk_events e ON s.id = e.session_id
   GROUP BY s.id, s.user_id
   ORDER BY event_count DESC

Storage Considerations
======================

Data Size Estimates
-------------------

**Typical Session:**

- Session record: ~500 bytes (base) + state size
- Average state: 1-5 KB
- Total per session: ~2-10 KB

**Typical Event:**

- Event record: ~1 KB (base)
- Content: 0.5-5 KB
- Actions: 0.1-1 KB
- Total per event: ~2-10 KB

**Example: 1000 users, 10 sessions each, 50 events per session:**

- Sessions: 1000 × 10 × 5 KB = 50 MB
- Events: 1000 × 10 × 50 × 5 KB = 2.5 GB
- Total: ~2.55 GB

Retention Policies
------------------

Implement automatic cleanup for old sessions:

.. code-block:: sql

   -- Delete sessions older than 90 days
   DELETE FROM adk_sessions
   WHERE update_time < CURRENT_TIMESTAMP - INTERVAL '90 days'

   -- Archive old sessions to separate table
   INSERT INTO adk_sessions_archive
   SELECT * FROM adk_sessions
   WHERE update_time < CURRENT_TIMESTAMP - INTERVAL '90 days'

   DELETE FROM adk_sessions
   WHERE update_time < CURRENT_TIMESTAMP - INTERVAL '90 days'

See Also
========

- :doc:`adapters` - Database-specific implementations
- :doc:`migrations` - Schema migration guide
- :doc:`api` - API reference
- :doc:`/examples/adapters/asyncpg/connect_pool` - PostgreSQL connection example
- :doc:`/examples/extensions/adk/basic_aiosqlite` - SQLite usage example
- :doc:`/examples/patterns/multi_tenant/router` - Multi-tenant schema example
