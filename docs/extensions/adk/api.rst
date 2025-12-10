=============
API Reference
=============

This page documents the complete API for the SQLSpec ADK extension.

.. currentmodule:: sqlspec.extensions.adk

Session Service
===============

SQLSpecSessionService
---------------------

.. autoclass:: SQLSpecSessionService
   :show-inheritance:

   SQLSpec-backed implementation of Google ADK's ``BaseSessionService``.

   This service provides session and event storage using SQLSpec database adapters,
   delegating all database operations to a store implementation.

   **Attributes:**

   .. attribute:: store
      :no-index:

      The database store implementation (e.g., ``AsyncpgADKStore``).

   **Example:**

   .. code-block:: python

      from sqlspec.adapters.asyncpg import AsyncpgConfig
      from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore
      from sqlspec.extensions.adk import SQLSpecSessionService

      config = AsyncpgConfig(connection_config={"dsn": "postgresql://..."})
      store = AsyncpgADKStore(config)
      await store.create_tables()

      service = SQLSpecSessionService(store)

      # Create a session
      session = await service.create_session(
          app_name="my_app",
          user_id="user123",
          state={"key": "value"}
      )

   .. seealso::

      :doc:`/examples/extensions/adk/basic_aiosqlite`
         Complete runnable example with session creation and event management

      :doc:`/examples/extensions/adk/litestar_aiosqlite`
         Web framework integration using Litestar

Base Store Classes
==================

BaseAsyncADKStore
------------

.. autoclass:: BaseAsyncADKStore
   :show-inheritance:

   Abstract base class for async SQLSpec-backed ADK session stores.

   This class defines the interface that all database-specific async store implementations
   must follow. Each database adapter (asyncpg, psycopg, asyncmy, etc.) provides a concrete
   implementation in its ``adk/`` subdirectory.

   **Type Parameters:**

   - ``ConfigT``: The SQLSpec configuration type (e.g., ``AsyncpgConfig``)

   **Abstract Methods:**

   Subclasses must implement:

   - :meth:`create_session`
   - :meth:`get_session`
   - :meth:`update_session_state`
   - :meth:`list_sessions`
   - :meth:`delete_session`
   - :meth:`append_event`
   - :meth:`get_events`
   - :meth:`create_tables`
   - :meth:`_get_create_sessions_table_sql`
   - :meth:`_get_create_events_table_sql`
   - :meth:`_get_drop_tables_sql`

   **Properties:**

   .. attribute:: config
      :no-index:

      The SQLSpec database configuration.

   .. attribute:: session_table
      :no-index:

      Name of the sessions table (default: ``adk_sessions``).

   .. attribute:: events_table
      :no-index:

      Name of the events table (default: ``adk_events``).

   **Example:**

   .. code-block:: python

      from sqlspec.adapters.asyncpg import AsyncpgConfig
      from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore

      config = AsyncpgConfig(connection_config={"dsn": "postgresql://..."})
      store = AsyncpgADKStore(
          config,
          session_table="custom_sessions",
          events_table="custom_events"
      )
      await store.create_tables()

   .. seealso::

      :doc:`/examples/patterns/multi_tenant/router`
         Multi-tenant example showing custom table names for tenant isolation

BaseSyncADKStore
----------------

.. autoclass:: BaseSyncADKStore
   :show-inheritance:

   Abstract base class for synchronous SQLSpec-backed ADK session stores.

   Similar to :class:`BaseAsyncADKStore` but for synchronous database drivers. Currently used
   by the SQLite adapter which wraps sync operations with async compatibility.

   **Type Parameters:**

   - ``ConfigT``: The SQLSpec configuration type (e.g., ``SqliteConfig``)

   **Abstract Methods:**

   Subclasses must implement:

   - :meth:`create_session`
   - :meth:`get_session`
   - :meth:`update_session_state`
   - :meth:`list_sessions`
   - :meth:`delete_session`
   - :meth:`create_event`
   - :meth:`list_events`
   - :meth:`create_tables`
   - :meth:`_get_create_sessions_table_sql`
   - :meth:`_get_create_events_table_sql`
   - :meth:`_get_drop_tables_sql`

   **Example:**

   .. code-block:: python

      from sqlspec.adapters.sqlite import SqliteConfig
      from sqlspec.adapters.sqlite.adk import SqliteADKStore

      config = SqliteConfig(connection_config={"database": "agent.db"})
      store = SqliteADKStore(config)
      store.create_tables()

Type Definitions
================

SessionRecord
-------------

.. autoclass:: sqlspec.extensions.adk._types.SessionRecord

   TypedDict representing a session database record.

   **Fields:**

   .. attribute:: id
      :type: str

      Unique session identifier (typically a UUID).

   .. attribute:: app_name
      :type: str

      Name of the application.

   .. attribute:: user_id
      :type: str

      User identifier.

   .. attribute:: state
      :type: dict[str, Any]

      Session state dictionary (stored as JSON/JSONB).

   .. attribute:: create_time
      :type: datetime

      Timestamp when session was created (timezone-aware).

   .. attribute:: update_time
      :type: datetime

      Timestamp when session was last updated (timezone-aware).

   **Example:**

   .. code-block:: python

      from datetime import datetime, timezone

      record: SessionRecord = {
          "id": "550e8400-e29b-41d4-a716-446655440000",
          "app_name": "weather_agent",
          "user_id": "user123",
          "state": {"location": "SF", "units": "metric"},
          "create_time": datetime.now(timezone.utc),
          "update_time": datetime.now(timezone.utc)
      }

EventRecord
-----------

.. autoclass:: sqlspec.extensions.adk._types.EventRecord

   TypedDict representing an event database record.

   **Fields:**

   .. attribute:: id
      :type: str

      Unique event identifier.

   .. attribute:: app_name
      :type: str

      Application name (denormalized from session).

   .. attribute:: user_id
      :type: str

      User identifier (denormalized from session).

   .. attribute:: session_id
      :type: str

      Parent session identifier (foreign key).

   .. attribute:: invocation_id
      :type: str

      ADK invocation identifier.

   .. attribute:: author
      :type: str

      Event author (``user``, ``assistant``, ``system``).

   .. attribute:: branch
      :type: str | None

      Conversation branch identifier.

   .. attribute:: actions
      :type: bytes

      Pickled actions object.

   .. attribute:: long_running_tool_ids_json
      :type: str | None

      JSON-encoded list of long-running tool IDs.

   .. attribute:: timestamp
      :type: datetime

      Event timestamp (timezone-aware).

   .. attribute:: content
      :type: dict[str, Any] | None

      Event content (stored as JSON/JSONB).

   .. attribute:: grounding_metadata
      :type: dict[str, Any] | None

      Grounding metadata (stored as JSON/JSONB).

   .. attribute:: custom_metadata
      :type: dict[str, Any] | None

      Custom metadata (stored as JSON/JSONB).

   .. attribute:: partial
      :type: bool | None

      Whether this is a partial event.

   .. attribute:: turn_complete
      :type: bool | None

      Whether the turn is complete.

   .. attribute:: interrupted
      :type: bool | None

      Whether the event was interrupted.

   .. attribute:: error_code
      :type: str | None

      Error code if event failed.

   .. attribute:: error_message
      :type: str | None

      Error message if event failed.

Converter Functions
===================

The converter module provides functions to translate between ADK models and database records.

.. currentmodule:: sqlspec.extensions.adk.converters

session_to_record
-----------------

.. autofunction:: session_to_record

   Convert an ADK ``Session`` object to a ``SessionRecord`` for database storage.

   **Args:**

   - ``session``: ADK Session object

   **Returns:**

   - ``SessionRecord``: Database record ready for insertion

   **Example:**

   .. code-block:: python

      from google.adk.sessions import Session
      from sqlspec.extensions.adk.converters import session_to_record

      session = Session(
          id="sess_123",
          app_name="my_agent",
          user_id="user456",
          state={"count": 1},
          events=[]
      )

      record = session_to_record(session)
      # record is a SessionRecord TypedDict

record_to_session
-----------------

.. autofunction:: record_to_session

   Convert a ``SessionRecord`` and list of ``EventRecord``\s to an ADK ``Session`` object.

   **Args:**

   - ``record``: Session database record
   - ``events``: List of event records for this session

   **Returns:**

   - ``Session``: ADK Session object

   **Example:**

   .. code-block:: python

      from sqlspec.extensions.adk.converters import record_to_session

      session = record_to_session(session_record, event_records)
      # session is a google.adk.sessions.Session

event_to_record
---------------

.. autofunction:: event_to_record

   Convert an ADK ``Event`` object to an ``EventRecord`` for database storage.

   **Args:**

   - ``event``: ADK Event object
   - ``session_id``: ID of the parent session
   - ``app_name``: Application name
   - ``user_id``: User identifier

   **Returns:**

   - ``EventRecord``: Database record ready for insertion

   **Example:**

   .. code-block:: python

      from google.adk.events.event import Event
      from google.genai.types import Content, Part
      from sqlspec.extensions.adk.converters import event_to_record

      event = Event(
          id="evt_1",
          invocation_id="inv_1",
          author="user",
          content=Content(parts=[Part(text="Hello")]),
          actions=[]
      )

      record = event_to_record(
          event=event,
          session_id="sess_123",
          app_name="my_agent",
          user_id="user456"
      )

record_to_event
---------------

.. autofunction:: record_to_event

   Convert an ``EventRecord`` database record to an ADK ``Event`` object.

   **Args:**

   - ``record``: Event database record

   **Returns:**

   - ``Event``: ADK Event object

   **Example:**

   .. code-block:: python

      from sqlspec.extensions.adk.converters import record_to_event

      event = record_to_event(event_record)
      # event is a google.adk.events.event.Event

Database Adapter Stores
=======================

Each database adapter provides its own store implementation. See :doc:`adapters` for details.

Available Stores
----------------

**PostgreSQL:**

- ``sqlspec.adapters.asyncpg.adk.AsyncpgADKStore``
- ``sqlspec.adapters.psycopg.adk.PsycopgADKStore``
- ``sqlspec.adapters.psqlpy.adk.PsqlpyADKStore``

**MySQL:**

- ``sqlspec.adapters.asyncmy.adk.AsyncmyADKStore``

**SQLite:**

- ``sqlspec.adapters.sqlite.adk.SqliteADKStore``
- ``sqlspec.adapters.aiosqlite.adk.AiosqliteADKStore``

**Oracle:**

- ``sqlspec.adapters.oracledb.adk.OracleADKStore``

**DuckDB (dev/test only):**

- ``sqlspec.adapters.duckdb.adk.DuckDBADKStore``

See Also
========

- :doc:`adapters` - Database-specific implementations
- :doc:`schema` - Database schema reference
- :doc:`/examples/extensions/adk/basic_aiosqlite` - Basic usage example
- :doc:`/examples/extensions/adk/litestar_aiosqlite` - Litestar web framework integration
- :doc:`/examples/patterns/multi_tenant/router` - Multi-tenant deployment patterns
- `Google ADK Documentation <https://github.com/google/genai>`_
