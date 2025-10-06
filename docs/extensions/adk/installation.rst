============
Installation
============

Requirements
============

Python Version
--------------

SQLSpec ADK extension requires:

- **Python 3.10 or higher**
- **Google ADK** (``google-genai`` package)
- **SQLSpec** with a supported database adapter

Database Drivers
----------------

Choose at least one database adapter based on your production database.

Installing SQLSpec with ADK Support
====================================

The ADK extension is included in the main SQLSpec package. You need to install SQLSpec with your chosen database adapter(s).

PostgreSQL (Recommended)
------------------------

PostgreSQL is the recommended production database for AI agents due to its robust JSONB support, ACID compliance, and excellent concurrency.

.. tab-set::

   .. tab-item:: asyncpg (recommended)

      Fast, async-native PostgreSQL driver with connection pooling.

      .. code-block:: bash

         pip install sqlspec[asyncpg] google-genai
         # or
         uv pip install sqlspec[asyncpg] google-genai

   .. tab-item:: psycopg

      Modern PostgreSQL adapter with both sync and async support.

      .. code-block:: bash

         pip install sqlspec[psycopg] google-genai
         # or
         uv pip install sqlspec[psycopg] google-genai

   .. tab-item:: psqlpy

      High-performance async PostgreSQL driver built with Rust.

      .. code-block:: bash

         pip install sqlspec[psqlpy] google-genai
         # or
         uv pip install sqlspec[psqlpy] google-genai

MySQL / MariaDB
---------------

MySQL 8.0+ and MariaDB 10.5+ support native JSON columns suitable for session storage.

.. code-block:: bash

   pip install sqlspec[asyncmy] google-genai
   # or
   uv pip install sqlspec[asyncmy] google-genai

SQLite
------

SQLite is great for development, testing, and single-user applications.

.. tab-set::

   .. tab-item:: sqlite (sync)

      Standard library synchronous driver with async wrapper.

      .. code-block:: bash

         pip install sqlspec google-genai
         # sqlite3 is included in Python standard library

   .. tab-item:: aiosqlite (async)

      Native async SQLite driver.

      .. code-block:: bash

         pip install sqlspec[aiosqlite] google-genai
         # or
         uv pip install sqlspec[aiosqlite] google-genai

Oracle Database
---------------

Oracle Database 19c+ with JSON support.

.. code-block:: bash

   pip install sqlspec[oracledb] google-genai
   # or
   uv pip install sqlspec[oracledb] google-genai

DuckDB (Development/Testing Only)
----------------------------------

.. warning::

   **DuckDB is NOT recommended for production AI agents.** DuckDB is an OLAP database designed for
   analytical queries, not concurrent transactional workloads. Use it only for development or testing.

.. code-block:: bash

   pip install sqlspec[duckdb] google-genai
   # or
   uv pip install sqlspec[duckdb] google-genai

Installing Multiple Adapters
=============================

You can install multiple database adapters for testing across different databases:

.. code-block:: bash

   pip install sqlspec[asyncpg,sqlite,asyncmy] google-genai
   # or
   uv pip install sqlspec[asyncpg,sqlite,asyncmy] google-genai

Optional Dependencies
=====================

Type-Safe Result Mapping
------------------------

For enhanced type safety with result mapping:

.. code-block:: bash

   # Pydantic (default, included with google-genai)
   pip install sqlspec[asyncpg,pydantic]

   # msgspec (high performance)
   pip install sqlspec[asyncpg,msgspec]

Framework Integrations
----------------------

Integrate with Litestar web framework:

.. code-block:: bash

   pip install sqlspec[asyncpg,litestar] google-genai
   # or
   uv pip install sqlspec[asyncpg,litestar] google-genai

Verification
============

Verify your installation:

.. code-block:: python

   import asyncio
   from sqlspec import SQLSpec
   from sqlspec.extensions.adk import SQLSpecSessionService

   # Check imports work
   print("✅ SQLSpec ADK extension installed successfully")

   # Check adapter imports
   try:
       from sqlspec.adapters.asyncpg import AsyncpgConfig
       from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore
       print("✅ AsyncPG adapter available")
   except ImportError:
       print("❌ AsyncPG adapter not installed")

   try:
       from google.adk.sessions import Session
       print("✅ Google ADK installed")
   except ImportError:
       print("❌ Google ADK not installed - run: pip install google-genai")

Development Installation
========================

For contributing to SQLSpec or running tests:

.. code-block:: bash

   git clone https://github.com/litestar-org/sqlspec.git
   cd sqlspec
   make install
   # or
   uv sync --all-extras --dev

This installs all database adapters, testing tools, and development dependencies.

Running Tests
-------------

Run ADK extension tests:

.. code-block:: bash

   # Run all ADK tests
   uv run pytest tests/integration/extensions/test_adk/ -v

   # Run specific adapter tests
   uv run pytest tests/integration/extensions/test_adk/test_asyncpg_store.py -v

Docker Infrastructure
---------------------

Start development databases:

.. code-block:: bash

   # Start all databases
   make infra-up

   # Start specific database
   make infra-postgres
   make infra-mysql
   make infra-oracle

   # Stop all databases
   make infra-down

Next Steps
==========

Now that the ADK extension is installed, proceed to the :doc:`quickstart` guide to create your first session-backed agent!

See Also
========

- :doc:`quickstart` - Get started in 5 minutes
- :doc:`adapters` - Database-specific configuration
- :doc:`/getting_started/installation` - General SQLSpec installation
