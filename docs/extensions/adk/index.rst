:orphan:

====================
Google ADK Extension
====================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   quickstart
   api
   adapters
   backends/adbc
   backends/aiosqlite
   backends/asyncmy
   backends/asyncpg
   backends/bigquery
   backends/duckdb
   backends/oracledb
   backends/psqlpy
   backends/psycopg
   backends/sqlite
   migrations
   schema

Session, event, and memory storage for the Google Agent Development Kit (ADK) using SQLSpec database adapters.

Overview
========

The SQLSpec ADK extension provides persistent storage for `Google Agent Development Kit <https://github.com/google/genai>`_ sessions, events, and long-term memory entries, enabling stateful AI agent applications with database-backed conversation history and recall.

This extension implements ADK's ``BaseSessionService`` protocol, allowing AI agents to store and retrieve:

- **Session State**: Persistent conversation context and application state
- **Event History**: Complete record of user/assistant interactions
- **Long-term Memory**: Searchable memory entries extracted from completed sessions
- **Multi-User Support**: Isolated sessions per application and user
- **Type-Safe Storage**: Full type safety with TypedDicts and validated records

Key Features
============

Production Features
-------------------

- **Multiple Database Backends**: PostgreSQL, MySQL, SQLite, Oracle, DuckDB
- **ACID Transactions**: Reliable storage with database guarantees
- **Connection Pooling**: Built-in connection management via SQLSpec adapters
- **Async/Sync Support**: Native async drivers and sync adapters with async wrappers

Development Features
--------------------

- **Simple API**: Clean, intuitive interface matching ADK patterns
- **Type Safety**: Full type hints and runtime type checking
- **Flexible Schema**: Customizable table names for multi-tenant deployments
- **Owner ID Columns**: Optional foreign keys linking sessions to user tables with cascade deletes
- **Rich Metadata**: JSON storage for content, grounding, and custom data

Performance Features
--------------------

- **Indexed Queries**: Composite indexes on common query patterns
- **Efficient JSON Storage**: JSONB (PostgreSQL) or native JSON types
- **Cascade Deletes**: Automatic cleanup of related records
- **HOT Updates**: PostgreSQL fillfactor tuning for reduced bloat

Database Support Status
=======================

.. list-table::
   :header-rows: 1
   :widths: 20 20 15 45

   * - Database
     - Adapter
     - Status
     - Notes
   * - PostgreSQL
     - ``asyncpg``
     - ✅ Production
     - JSONB, microsecond timestamps
   * - PostgreSQL
     - ``psycopg``
     - ✅ Production
     - JSONB, full async support
   * - PostgreSQL
     - ``psqlpy``
     - ✅ Production
     - Rust-based, high performance
   * - MySQL/MariaDB
     - ``asyncmy``
     - ✅ Production
     - JSON type, microsecond timestamps
   * - SQLite
     - ``sqlite``
     - ✅ Production
     - Sync driver with async wrapper
   * - SQLite
     - ``aiosqlite``
     - ✅ Production
     - Native async support
   * - Oracle
     - ``oracledb``
     - ✅ Production
     - CLOB JSON, BLOB storage
   * - DuckDB
     - ``duckdb``
     - ✅ Production*
     - Best for OLAP workloads, analytics
   * - BigQuery
     - ``bigquery``
     - ✅ Production
     - Serverless, partitioned, cost-optimized
   * - ADBC
     - ``adbc``
     - ✅ Production
     - Arrow-native, multi-backend support

.. note::

   **DuckDB is optimized for OLAP workloads.** DuckDB excels at analytical queries and embedded
   use cases with zero-configuration setup. It's perfect for development, testing, and analytical
   workloads on session data. For highly concurrent DML operations (frequent inserts/updates/deletes),
   consider PostgreSQL or other OLTP-optimized databases.

Quick Example
=============

Here's a simple example of creating and managing ADK sessions with AioSQLite:

.. literalinclude:: ../../examples/extensions/adk/basic_aiosqlite.py
   :language: python
   :lines: 1-40
   :caption: Create and use an ADK session with AioSQLite
   :emphasize-lines: 1-5, 18-23

Architecture Overview
=====================

The extension follows a layered architecture:

.. code-block:: text

   ┌─────────────────────┐
   │   ADK Agent         │
   └──────────┬──────────┘
              │
   ┌──────────▼──────────┐
   ┌─────────────────────┐
   │     ADK Runner       │
   └──────────┬──────────┘
              │
   ┌──────────▼──────────┐        ┌────────────────────┐
   │ SQLSpecSessionService│        │ SQLSpecMemoryService│
   └──────────┬──────────┘        └──────────┬─────────┘
              │                             │
   ┌──────────▼──────────┐        ┌─────────▼─────────┐
   │ Session Store       │        │ Memory Store      │
   └──────────┬──────────┘        └─────────┬─────────┘
              │                             │
   ┌──────────▼──────────┐        ┌─────────▼─────────┐
   │  SQLSpec Config     │        │  SQLSpec Config   │
   └──────────┬──────────┘        └─────────┬─────────┘
              │                             │
   ┌──────────▼──────────┐        ┌─────────▼─────────┐
   │    Database         │        │    Database       │
   └─────────────────────┘        └───────────────────┘

**Layers:**

1. **Service Layer** (``SQLSpecSessionService`` / ``SQLSpecMemoryService``): Implements ADK service protocols
2. **Store Layer** (``BaseAsyncADKStore`` / ``BaseAsyncADKMemoryStore``): Abstract database operations per adapter
3. **Config Layer** (SQLSpec): Connection pooling and resource management
4. **Database Layer**: Physical storage with database-specific optimizations

Examples
========

New curated examples live in the :doc:`examples catalog </examples/index>`:

* :doc:`/examples/extensions/adk/basic_aiosqlite` – create a session, append two events, and read the transcript using AioSQLite storage.
* :doc:`/examples/extensions/adk/litestar_aiosqlite` – initialize ``SQLSpecSessionService`` inside a Litestar app and expose a ``/sessions`` route.
* :doc:`/examples/extensions/adk/runner_memory_aiosqlite` – run an ADK ``Runner`` with SQLSpec-backed memory and search stored memories.

Next Steps
==========

.. grid:: 2
   :gutter: 3

   .. grid-item-card:: 📦 Installation
      :link: installation
      :link-type: doc

      Install the extension and database adapters

   .. grid-item-card:: 🚀 Quick Start
      :link: quickstart
      :link-type: doc

      Get up and running in 5 minutes

   .. grid-item-card:: 📚 API Reference
      :link: api
      :link-type: doc

      Complete API documentation

   .. grid-item-card:: 🔌 Adapters
      :link: adapters
      :link-type: doc

      Database-specific implementations

See Also
========

- :doc:`/usage/framework_integrations` - Framework integration guide
- :doc:`/reference/extensions` - SQLSpec extensions reference
- :doc:`/reference/adapters` - Database adapters documentation
- `Google ADK Documentation <https://github.com/google/genai>`_
