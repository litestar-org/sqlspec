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

   config = AsyncpgConfig(pool_config={
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

   config = AiosqliteConfig(pool_config={
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
code is available at :doc:`/examples/adk_basic_asyncpg`.

.. literalinclude:: ../../examples/adk_basic_asyncpg.py
   :language: python
   :lines: 27-109
   :caption: Complete ADK session management example (adk_basic_asyncpg.py)
   :emphasize-lines: 1-5, 11-12, 17-18, 33-34

Running the Example
===================

Run the example directly:

.. code-block:: bash

   python docs/examples/adk_basic_asyncpg.py

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

Now that you understand the basics:

- :doc:`api` - Explore the complete API reference
- :doc:`adapters` - Learn about database-specific features
- :doc:`/examples/adk_litestar_asyncpg` - See Litestar web framework integration
- :doc:`/examples/adk_multi_tenant` - Learn multi-tenant patterns
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
- :doc:`/examples/adk_litestar_asyncpg` - Litestar framework integration
- :doc:`/examples/adk_basic_sqlite` - SQLite for local development
