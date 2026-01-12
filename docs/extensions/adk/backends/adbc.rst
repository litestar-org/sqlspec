=====
ADBC
=====

Arrow Database Connectivity (ADBC) provides a vendor-neutral API for database access using Apache Arrow's
columnar format. ADBC enables zero-copy data transfer and high-performance analytics across multiple
database backends.

Overview
========

The ADBC ADK store supports multiple database backends through ADBC drivers:

- **PostgreSQL** - Production-ready with full feature support
- **SQLite** - Development and testing
- **DuckDB** - Embedded analytics
- **Flight SQL** - Distributed query execution
- **Snowflake** - Cloud data warehouse

**Key Benefits:**

- **Zero-Copy Data Transfer** - Arrow-native data exchange eliminates serialization overhead
- **Columnar Format** - Efficient for analytical workloads
- **Vendor Neutral** - Single API across multiple databases
- **High Performance** - Optimized for large-scale data operations

Installation
============

Install ADBC with your chosen driver:

**PostgreSQL:**

.. code-block:: bash

   pip install sqlspec[adbc-postgresql]
   # or
   uv add 'sqlspec[adbc-postgresql]'

**SQLite:**

.. code-block:: bash

   pip install sqlspec[adbc-sqlite]
   # or
   uv add 'sqlspec[adbc-sqlite]'

**DuckDB:**

.. code-block:: bash

   pip install sqlspec[adbc-duckdb]
   # or
   uv add 'sqlspec[adbc-duckdb]'

Quick Start
===========

Basic SQLite Example
--------------------

.. code-block:: python

   from sqlspec.adapters.adbc import AdbcConfig
   from sqlspec.adapters.adbc.adk import AdbcADKStore

   # Configure ADBC with SQLite backend
   config = AdbcConfig(connection_config={
       "driver_name": "sqlite",
       "uri": "file:agent.db"
   })

   # Initialize store and create tables
   store = AdbcADKStore(config)
   store.create_tables()

   # Create session
   session = store.create_session(
       session_id="session-1",
       app_name="my-agent",
       user_id="user-123",
       state={"step": 1, "context": "initialized"}
   )

   # Create event
   event = store.create_event(
       event_id="event-1",
       session_id="session-1",
       app_name="my-agent",
       user_id="user-123",
       author="assistant",
       content={"message": "Processing request..."}
   )

PostgreSQL Production Example
------------------------------

.. code-block:: python

   from sqlspec.adapters.adbc import AdbcConfig
   from sqlspec.adapters.adbc.adk import AdbcADKStore

   # Configure ADBC with PostgreSQL backend
   config = AdbcConfig(connection_config={
       "driver_name": "postgresql",
       "uri": "postgresql://user:pass@localhost:5432/agentdb",
       "username": "agent_user",
       "password": "secure_password"
   })

   store = AdbcADKStore(config)
   store.create_tables()

   # Sessions and events work identically across backends
   session = store.create_session(
       session_id="prod-session-1",
       app_name="production-agent",
       user_id="user-456",
       state={"environment": "production", "version": "1.0"}
   )

Configuration
=============

ADBC Connection Parameters
---------------------------

.. code-block:: python

   from sqlspec.adapters.adbc import AdbcConfig

   config = AdbcConfig(
       connection_config={
           "driver_name": "postgresql",  # or "sqlite", "duckdb", etc.
           "uri": "postgresql://host:port/database",
           "username": "user",
           "password": "pass",
           # Driver-specific options
           "adbc.connection.autocommit": "true"
       }
   )

Custom Table Names
------------------

.. code-block:: python

   store = AdbcADKStore(
       config,
       session_table="agent_sessions",
       events_table="agent_events"
   )

Schema
======

Sessions Table
--------------

.. code-block:: sql

   CREATE TABLE IF NOT EXISTS adk_sessions (
       id VARCHAR(128) PRIMARY KEY,
       app_name VARCHAR(128) NOT NULL,
       user_id VARCHAR(128) NOT NULL,
       state TEXT NOT NULL DEFAULT '{}',
       create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
   )

   CREATE INDEX IF NOT EXISTS idx_adk_sessions_app_user
       ON adk_sessions(app_name, user_id);

   CREATE INDEX IF NOT EXISTS idx_adk_sessions_update_time
       ON adk_sessions(update_time DESC);

Events Table
------------

.. code-block:: sql

   CREATE TABLE IF NOT EXISTS adk_events (
       id VARCHAR(128) PRIMARY KEY,
       session_id VARCHAR(128) NOT NULL,
       app_name VARCHAR(128) NOT NULL,
       user_id VARCHAR(128) NOT NULL,
       invocation_id VARCHAR(256),
       author VARCHAR(256),
       actions BLOB,
       long_running_tool_ids_json TEXT,
       branch VARCHAR(256),
       timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       content TEXT,
       grounding_metadata TEXT,
       custom_metadata TEXT,
       partial INTEGER,
       turn_complete INTEGER,
       interrupted INTEGER,
       error_code VARCHAR(256),
       error_message VARCHAR(1024),
       FOREIGN KEY (session_id) REFERENCES adk_sessions(id) ON DELETE CASCADE
   )

   CREATE INDEX IF NOT EXISTS idx_adk_events_session
       ON adk_events(session_id, timestamp ASC);

**Field Types:**

- ``TEXT`` - JSON-serialized data (state, content, metadata)
- ``BLOB`` - Pickled actions from Google ADK
- ``INTEGER`` - Boolean fields (0/1/NULL)
- ``TIMESTAMP`` - Created/updated timestamps
- ``VARCHAR`` - String identifiers with length limits

Usage Patterns
==============

Session Management
------------------

.. code-block:: python

   # Create session
   session = store.create_session(
       session_id="unique-id",
       app_name="my-agent",
       user_id="user-123",
       state={"conversation": [], "context": {}}
   )

   # Get session
   session = store.get_session("unique-id")
   if session:
       print(session["state"])

   # Update session state
   store.update_session_state("unique-id", {
       "conversation": [...],
       "context": {...},
       "updated": True
   })

   # List user's sessions
   sessions = store.list_sessions("my-agent", "user-123")
   for session in sessions:
       print(f"{session['id']}: {session['state']}")

   # Delete session (cascades to events)
   store.delete_session("unique-id")

Event Management
----------------

.. code-block:: python

   # Create event with all fields
   event = store.create_event(
       event_id="event-123",
       session_id="session-id",
       app_name="my-agent",
       user_id="user-123",
       author="assistant",
       actions=b"pickled_actions",
       content={"message": "Response text"},
       grounding_metadata={"sources": ["doc1", "doc2"]},
       custom_metadata={"confidence": 0.95},
       partial=False,
       turn_complete=True,
       interrupted=False
   )

   # List session events (ordered by timestamp)
   events = store.list_events("session-id")
   for event in events:
       print(f"{event['timestamp']}: {event['content']}")

Database-Specific Notes
=======================

SQLite
------

**Advantages:**

- Simple setup for development
- Serverless (embedded database)
- Good for single-user agents

**Limitations:**

- Foreign key enforcement requires ``PRAGMA foreign_keys = ON`` per connection
- ADBC creates new connections per operation, so cascade deletes may not work reliably
- No concurrent writes (single writer)


PostgreSQL
----------

**Advantages:**

- Full ACID compliance
- Excellent concurrency
- JSON/JSONB support for efficient queries
- Production-grade reliability


DuckDB
------

**Advantages:**

- Embedded analytical database
- Excellent for processing large datasets
- Arrow-native with zero-copy integration
- SQL analytics capabilities


Performance Considerations
==========================

Connection Management
---------------------

ADBC creates a new connection for each operation by default. For high-throughput applications:

- Use connection pooling at the application level
- Consider batch operations where possible
- Monitor connection creation overhead

Data Types
----------

- **JSON serialization**: Uses ``to_json/from_json`` for cross-database compatibility
- **Arrow format**: Data returned as Arrow Tables/RecordBatches for zero-copy access
- **BLOB storage**: Actions are stored as binary data (pickled by Google ADK)

Indexing
--------

The ADK store creates indexes on:

- ``(app_name, user_id)`` for session listing
- ``update_time DESC`` for recent session queries
- ``(session_id, timestamp ASC)`` for event chronology

Migration from Other Adapters
==============================

ADBC uses standard SQL compatible with most databases. To migrate:

1. **Export data** from existing store
2. **Configure ADBC** with your target database
3. **Create tables** using ``store.create_tables()``
4. **Import data** using standard SQL or bulk insert operations

.. code-block:: python

   # Example: Migrate from AsyncPG to ADBC
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore
   from sqlspec.adapters.adbc import AdbcConfig
   from sqlspec.adapters.adbc.adk import AdbcADKStore

   # Source (AsyncPG)
   source_config = AsyncpgConfig(connection_config={"dsn": "..."})
   source_store = AsyncpgADKStore(source_config)

   # Destination (ADBC)
   dest_config = AdbcConfig(connection_config={
       "driver_name": "postgresql",
       "uri": "..."
   })
   dest_store = AdbcADKStore(dest_config)
   dest_store.create_tables()

   # Migrate sessions
   async for session in source_store.list_sessions("app", "user"):
       dest_store.create_session(
           session_id=session["id"],
           app_name=session["app_name"],
           user_id=session["user_id"],
           state=session["state"]
       )

Troubleshooting
===============

Foreign Key Constraints
-----------------------

If cascade deletes don't work with SQLite:

.. code-block:: python

   # Manually enable foreign keys for SQLite
   with config.provide_connection() as conn:
       cursor = conn.cursor()
       try:
           cursor.execute("PRAGMA foreign_keys = ON")
           conn.commit()
       finally:
           cursor.close()

Driver Not Found
----------------

Ensure you've installed the correct ADBC driver:

.. code-block:: bash

   # PostgreSQL
   pip install adbc-driver-postgresql

   # SQLite
   pip install adbc-driver-sqlite

   # DuckDB
   pip install adbc-driver-duckdb

Connection Errors
-----------------

Verify connection string format for your driver:

- **SQLite**: ``"sqlite:///path/to/db.sqlite"`` or ``"file:/path/to/db.sqlite"``
- **PostgreSQL**: ``"postgresql://user:pass@host:port/database"``
- **DuckDB**: ``"duckdb:///path/to/db.duckdb"``

API Reference
=============

.. autoclass:: sqlspec.adapters.adbc.adk.AdbcADKStore
   :members:
   :inherited-members:
   :show-inheritance:
   :no-index:

See Also
========

- :doc:`/extensions/adk/index` - ADK extension overview
- :doc:`/extensions/adk/quickstart` - Quick start guide
- :doc:`/adapters/adbc` - ADBC adapter documentation
- `ADBC Documentation <https://arrow.apache.org/adbc/>`_ - Official Apache Arrow ADBC docs
