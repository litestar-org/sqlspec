===========
Quick Start
===========

This guide will get you up and running with the SQLSpec ADK extension in 5 minutes.

Overview
========

In this quickstart, you'll:

1. Configure a database connection
2. Create the ADK tables
3. Initialize a session service
4. Create and manage AI agent sessions
5. Store and retrieve conversation events

Prerequisites
=============

Ensure you have installed:

- SQLSpec with a database adapter (see :doc:`installation`)
- Google ADK (``google-genai``)

.. code-block:: bash

   pip install sqlspec[asyncpg] google-genai

Step 1: Import Required Modules
================================

.. code-block:: python

   import asyncio
   from google.adk.events.event import Event
   from google.genai.types import Content, Part

   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore
   from sqlspec.extensions.adk import SQLSpecSessionService

Step 2: Configure Database Connection
======================================

Create a database configuration. This example uses PostgreSQL with AsyncPG:

.. code-block:: python

   config = AsyncpgConfig(connection_config={
       "dsn": "postgresql://user:password@localhost:5432/mydb",
       "min_size": 5,
       "max_size": 20
   })

.. note::

   Connection strings vary by database. See :doc:`adapters` for examples for each database.

For local development with SQLite:

.. code-block:: python

   from sqlspec.adapters.aiosqlite import AiosqliteConfig
   from sqlspec.adapters.aiosqlite.adk import AiosqliteADKStore

   config = AiosqliteConfig(connection_config={
       "database": "./my_agent.db"
   })

Step 3: Create the Store
=========================

Initialize the database store and create tables:

.. code-block:: python

   async def setup_database():
       # Create store instance
       store = AsyncpgADKStore(config)

       # Create sessions and events tables
       await store.create_tables()

       return store

.. tip::

   Run ``create_tables()`` once during application initialization. It's idempotent and safe to call multiple times.

Step 4: Initialize Session Service
===================================

Create the session service that implements ADK's ``BaseSessionService`` protocol:

.. code-block:: python

   async def create_service():
       store = await setup_database()
       service = SQLSpecSessionService(store)
       return service

Optional: Initialize Memory Service
===================================

If you want long-term memory search, create a memory store and service alongside the session service:

.. code-block:: python

   from sqlspec.adapters.asyncpg.adk.memory_store import AsyncpgADKMemoryStore
   from sqlspec.extensions.adk.memory import SQLSpecMemoryService

   async def create_memory_service(config):
       memory_store = AsyncpgADKMemoryStore(config)
       await memory_store.create_tables()
       return SQLSpecMemoryService(memory_store)

Step 5: Create a Session
=========================

Sessions represent individual conversations with unique state per user and application:

.. code-block:: python

   async def main():
       service = await create_service()

       # Create a new session
       session = await service.create_session(
           app_name="weather_agent",
           user_id="user_12345",
           state={"location": "San Francisco", "units": "metric"}
       )

       print(f"Session created: {session.id}")
       print(f"State: {session.state}")

.. note::

   - ``app_name``: Identifies your AI agent application
   - ``user_id``: Identifies the user (allows multiple sessions per user)
   - ``state``: Arbitrary JSON-serializable dictionary for session context
   - ``session_id``: Auto-generated UUID (or provide your own)

Step 6: Append Events
=====================

Events represent individual turns in the conversation:

.. code-block:: python

   async def conversation_example(service, session):
       # User message event
       user_event = Event(
           id="evt_001",
           invocation_id="inv_001",
           author="user",
           content=Content(parts=[Part(text="What's the weather today?")]),
           actions=[]
       )
       await service.append_event(session, user_event)

       # Assistant response event
       assistant_event = Event(
           id="evt_002",
           invocation_id="inv_001",
           author="assistant",
           content=Content(parts=[
               Part(text="The weather in San Francisco is sunny, 72°F.")
           ]),
           actions=[]
       )
       await service.append_event(session, assistant_event)

       print(f"Appended {len(session.events)} events to session")

Step 7: Retrieve a Session
===========================

Retrieve an existing session with its events:

.. code-block:: python

   async def retrieve_session(service):
       # Get session with all events
       session = await service.get_session(
           app_name="weather_agent",
           user_id="user_12345",
           session_id="<session-id-from-step-5>"
       )

       if session:
           print(f"Session {session.id}")
           print(f"State: {session.state}")
           print(f"Events: {len(session.events)}")

           for event in session.events:
               print(f"  {event.author}: {event.content}")

Step 8: List User Sessions
===========================

List all sessions for a user within an application:

.. code-block:: python

   async def list_user_sessions(service):
       response = await service.list_sessions(
           app_name="weather_agent",
           user_id="user_12345"
       )

       print(f"Found {len(response.sessions)} sessions")

       for session in response.sessions:
           print(f"  Session {session.id}")
           print(f"    Created: {session.create_time}")
           print(f"    Last updated: {session.last_update_time}")
           print(f"    State: {session.state}")

Step 9: Delete a Session
=========================

Delete a session and all its events:

.. code-block:: python

   async def cleanup(service, session_id):
       await service.delete_session(
           app_name="weather_agent",
           user_id="user_12345",
           session_id=session_id
       )

       print(f"Deleted session {session_id}")

Complete Example
================

Here's a complete working example that demonstrates all key operations. The full runnable
code is available at :doc:`/examples/extensions/adk/basic_aiosqlite`.

.. literalinclude:: ../../examples/extensions/adk/basic_aiosqlite.py
   :language: python
   :caption: Complete ADK session management example (basic_aiosqlite.py)

Running the Example
===================

Run the example directly:

.. code-block:: bash

   python docs/examples/extensions/adk/basic_aiosqlite.py

You should see output similar to:

.. code-block:: text

   === Google ADK with AsyncPG Example ===
   ✅ Created ADK tables in PostgreSQL

   === Creating Session ===
   Created session: 550e8400-e29b-41d4-a716-446655440000
   App: chatbot, User: user_123
   Initial state: {'conversation_count': 0}

   === Adding User Message Event ===
   Added user event: event_1
   User message: What is the weather like today?

   === Adding Assistant Response Event ===
   Added assistant event: event_2
   Assistant response: The weather is sunny with a high of 75°F.

   ✅ Example completed successfully!

Custom Table Names
==================

For multi-tenant deployments, use custom table names per tenant:

.. code-block:: python

   # Tenant A
   store_a = AsyncpgADKStore(
       config,
       session_table="tenant_a_sessions",
       events_table="tenant_a_events"
   )
   await store_a.create_tables()
   service_a = SQLSpecSessionService(store_a)

   # Tenant B
   store_b = AsyncpgADKStore(
       config,
       session_table="tenant_b_sessions",
       events_table="tenant_b_events"
   )
   await store_b.create_tables()
   service_b = SQLSpecSessionService(store_b)

User Foreign Key Column
========================

Link ADK sessions to your application's user table with referential integrity using the ``owner_id_column`` parameter.
This feature enables database-enforced relationships between sessions and users, automatic cascade deletes, and
multi-tenant isolation.

Why Use Owner ID Columns?
-------------------------

**Benefits:**

- **Referential Integrity**: Database enforces valid user references
- **Cascade Deletes**: Automatically remove sessions when users are deleted
- **Multi-Tenancy**: Isolate sessions by tenant/organization
- **Query Efficiency**: Join sessions with user data in a single query
- **Data Consistency**: Prevent orphaned sessions

Basic Usage
-----------

The ``owner_id_column`` parameter accepts a full column DDL definition:

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore

   config = AsyncpgConfig(connection_config={
       "dsn": "postgresql://user:password@localhost:5432/mydb"
   })

   # Create store with owner ID column
   store = AsyncpgADKStore(
       config,
       owner_id_column="account_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE"
   )
   await store.create_tables()

   # Create session with user FK value
   session = await store.create_session(
       session_id="session-123",
       app_name="my_agent",
       user_id="alice@example.com",
       state={"theme": "dark"},
       owner_id="550e8400-e29b-41d4-a716-446655440000"  # UUID of owner
   )

Database-Specific Examples
---------------------------

PostgreSQL with UUID
^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore

   store = AsyncpgADKStore(
       config,
       owner_id_column="account_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE"
   )
   await store.create_tables()

   # Use UUID type for owner_id
   import uuid
   user_uuid = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")

   session = await store.create_session(
       session_id="session-1",
       app_name="app",
       user_id="alice",
       state={},
       owner_id=user_uuid
   )

MySQL with BIGINT
^^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.adapters.asyncmy import AsyncmyConfig
   from sqlspec.adapters.asyncmy.adk import AsyncmyADKStore

   store = AsyncmyADKStore(
       config,
       owner_id_column="user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE"
   )
   await store.create_tables()

   session = await store.create_session(
       session_id="session-1",
       app_name="app",
       user_id="alice",
       state={},
       owner_id=12345  # Integer user ID
   )

SQLite with INTEGER
^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.adapters.sqlite import SqliteConfig
   from sqlspec.adapters.sqlite.adk import SqliteADKStore

   store = SqliteADKStore(
       config,
       owner_id_column="tenant_id INTEGER NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"
   )
   store.create_tables()

   session = store.create_session(
       session_id="session-1",
       app_name="app",
       user_id="alice",
       state={},
       owner_id=1
   )

Oracle with NUMBER
^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.adapters.oracledb import OracleConfig
   from sqlspec.adapters.oracledb.adk import OracleADKStore

   store = OracleADKStore(
       config,
       owner_id_column="user_id NUMBER(10) REFERENCES users(id) ON DELETE CASCADE"
   )
   await store.create_tables()

   session = await store.create_session(
       session_id="session-1",
       app_name="app",
       user_id="alice",
       state={},
       owner_id=12345
   )

Multi-Tenant Example
---------------------

Complete example linking sessions to tenants:

.. code-block:: python

   from sqlspec.adapters.duckdb import DuckDBConfig
   from sqlspec.adapters.duckdb.adk import DuckdbADKStore

   config = DuckDBConfig(connection_config={"database": "multi_tenant.ddb"})

   # Create tenants table
   with config.provide_connection() as conn:
       conn.execute("""
           CREATE TABLE tenants (
               id INTEGER PRIMARY KEY,
               name VARCHAR NOT NULL
           )
       """)
       conn.execute("INSERT INTO tenants (id, name) VALUES (1, 'Acme Corp')")
       conn.execute("INSERT INTO tenants (id, name) VALUES (2, 'Initech')")

   # Create store with tenant FK
   store = DuckdbADKStore(
       config,
       owner_id_column="tenant_id INTEGER NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"
   )
   store.create_tables()

   # Create sessions for different tenants
   session_acme = store.create_session(
       session_id="session-acme-1",
       app_name="analytics",
       user_id="alice",
       state={"workspace": "dashboard"},
       owner_id=1  # Acme Corp
   )

   session_initech = store.create_session(
       session_id="session-initech-1",
       app_name="analytics",
       user_id="bob",
       state={"workspace": "reports"},
       owner_id=2  # Initech
   )

   # Query sessions with tenant info
   with config.provide_connection() as conn:
       cursor = conn.execute("""
           SELECT s.id, s.user_id, t.name as tenant_name
           FROM adk_sessions s
           JOIN tenants t ON s.tenant_id = t.id
       """)
       for row in cursor.fetchall():
           print(f"Session {row[0]} - User: {row[1]}, Tenant: {row[2]}")

.. seealso::

   :doc:`/examples/patterns/multi_tenant/router`
      Complete runnable multi-tenant example with owner ID column

Cascade Delete Behavior
------------------------

When configured with ``ON DELETE CASCADE``, deleting a user automatically removes all their sessions:

.. code-block:: python

   # Create session linked to user
   await store.create_session(
       session_id="session-1",
       app_name="app",
       user_id="alice",
       state={},
       owner_id=user_uuid
   )

   # Verify session exists
   session = await store.get_session("session-1")
   assert session is not None

   # Delete user from your application
   async with config.provide_connection() as conn:
       await conn.execute("DELETE FROM users WHERE id = $1", user_uuid)

   # Session automatically deleted by CASCADE
   session = await store.get_session("session-1")
   assert session is None  # Automatically removed

Nullable Foreign Keys
---------------------

Use nullable FK columns for optional user relationships:

.. code-block:: python

   store = AsyncpgADKStore(
       config,
       owner_id_column="workspace_id UUID REFERENCES workspaces(id) ON DELETE SET NULL"
   )
   await store.create_tables()

   # Create session without FK (NULL value)
   session = await store.create_session(
       session_id="session-1",
       app_name="app",
       user_id="alice",
       state={}
       # owner_id not provided - will be NULL
   )

   # Create session with FK
   session = await store.create_session(
       session_id="session-2",
       app_name="app",
       user_id="bob",
       state={},
       owner_id=workspace_uuid
   )

Configuration via Extension Config
-----------------------------------

For migrations and programmatic configuration, use ``extension_config``:

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig

   config = AsyncpgConfig(
       connection_config={"dsn": "postgresql://..."},
       extension_config={
           "adk": {
               "session_table": "adk_sessions",
               "events_table": "adk_events",
               "owner_id_column": "account_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE"
           }
       }
   )

This is especially useful with the migration system (see :doc:`migrations`).

Column Name Extraction
----------------------

The store automatically extracts the column name from your DDL:

.. code-block:: python

   store = AsyncpgADKStore(
       config,
       owner_id_column="tenant_id INTEGER NOT NULL REFERENCES tenants(id)"
   )

   print(store.owner_id_column_name)  # "tenant_id"
   print(store.owner_id_column_ddl)   # Full DDL string

The column name is used in INSERT and SELECT statements, while the full DDL
is used in CREATE TABLE statements.

Event Filtering
===============

Retrieve only recent events:

.. code-block:: python

   from datetime import datetime, timezone, timedelta
   from google.adk.sessions.base_session_service import GetSessionConfig

   # Get session with only events from last hour
   one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)

   config = GetSessionConfig(
       after_timestamp=one_hour_ago.timestamp(),
       num_recent_events=10  # Limit to 10 most recent
   )

   session = await service.get_session(
       app_name="my_agent",
       user_id="user123",
       session_id="session_id",
       config=config
   )

Next Steps
==========

To learn more:

- :doc:`api` - Explore the complete API reference
- :doc:`adapters` - Learn about database-specific features
- :doc:`/examples/extensions/adk/litestar_aiosqlite` - Litestar web framework integration
- :doc:`/examples/patterns/multi_tenant/router` - Learn multi-tenant routing basics
- :doc:`schema` - Understand the database schema

Common Patterns
===============

Session State Updates
---------------------

Update session state as conversation progresses:

.. code-block:: python

   # Get current session
   session = await service.get_session(
       app_name="my_agent",
       user_id="user123",
       session_id=session_id
   )

   # Update state
   new_state = {**session.state, "message_count": 5}
   await store.update_session_state(session_id, new_state)

Error Handling
--------------

Handle database errors gracefully:

.. code-block:: python

   try:
       session = await service.get_session(
           app_name="my_agent",
           user_id="user123",
           session_id="invalid-id"
       )
       if session is None:
           print("Session not found")
   except Exception as e:
       print(f"Database error: {e}")

See Also
========

- :doc:`installation` - Installation instructions
- :doc:`api` - API reference
- :doc:`adapters` - Database adapter details
- :doc:`/examples/extensions/adk/litestar_aiosqlite` - Litestar framework integration
- :doc:`/examples/extensions/adk/basic_aiosqlite` - SQLite for local development
