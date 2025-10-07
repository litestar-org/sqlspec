============
Installation
============

Requirements
============

Python Version
--------------

SQLSpec Litestar extension requires:

- **Python 3.10 or higher**
- **Litestar 2.0 or higher**
- **SQLSpec** with a supported database adapter

Database Drivers
----------------

Choose at least one database adapter based on your database.

Installing SQLSpec with Litestar Support
=========================================

The Litestar extension is included in the main SQLSpec package when installed with the ``litestar`` extra.

PostgreSQL (Recommended)
------------------------

PostgreSQL is the recommended database for web applications due to its robust ACID compliance, excellent concurrency, and rich feature set.

.. tab-set::

   .. tab-item:: asyncpg (recommended)

      Fast, async-native PostgreSQL driver with connection pooling.

      .. code-block:: bash

         pip install sqlspec[asyncpg,litestar]
         # or
         uv pip install sqlspec[asyncpg,litestar]

   .. tab-item:: psycopg

      Modern PostgreSQL adapter with both sync and async support.

      .. code-block:: bash

         pip install sqlspec[psycopg,litestar]
         # or
         uv pip install sqlspec[psycopg,litestar]

   .. tab-item:: psqlpy

      High-performance async PostgreSQL driver built with Rust.

      .. code-block:: bash

         pip install sqlspec[psqlpy,litestar]
         # or
         uv pip install sqlspec[psqlpy,litestar]

MySQL / MariaDB
---------------

MySQL 8.0+ and MariaDB 10.5+ are well-supported for web applications.

.. code-block:: bash

   pip install sqlspec[asyncmy,litestar]
   # or
   uv pip install sqlspec[asyncmy,litestar]

SQLite
------

SQLite is great for development, testing, and single-server applications.

.. tab-set::

   .. tab-item:: sqlite (sync)

      Standard library synchronous driver with async wrapper.

      .. code-block:: bash

         pip install sqlspec[litestar]
         # sqlite3 is included in Python standard library

   .. tab-item:: aiosqlite (async)

      Native async SQLite driver.

      .. code-block:: bash

         pip install sqlspec[aiosqlite,litestar]
         # or
         uv pip install sqlspec[aiosqlite,litestar]

Oracle Database
---------------

Oracle Database 19c+ with async support.

.. code-block:: bash

   pip install sqlspec[oracledb,litestar]
   # or
   uv pip install sqlspec[oracledb,litestar]

DuckDB (Development/Testing Only)
----------------------------------

.. warning::

   **DuckDB is NOT recommended for production web applications.** DuckDB is an OLAP database designed for
   analytical queries, not concurrent transactional workloads. Use it only for development or testing.

.. code-block:: bash

   pip install sqlspec[duckdb,litestar]
   # or
   uv pip install sqlspec[duckdb,litestar]

Installing Multiple Adapters
=============================

Install multiple database adapters for multi-database applications or testing:

.. code-block:: bash

   pip install sqlspec[asyncpg,duckdb,litestar]
   # or
   uv pip install sqlspec[asyncpg,duckdb,litestar]

Optional Dependencies
=====================

Type-Safe Result Mapping
------------------------

For type safety with result mapping:

.. code-block:: bash

   # Pydantic (recommended, often included with Litestar)
   pip install sqlspec[asyncpg,litestar,pydantic]

   # msgspec (high performance)
   pip install sqlspec[asyncpg,litestar,msgspec]

Migration Tools
---------------

For database migrations:

.. code-block:: bash

   pip install sqlspec[asyncpg,litestar,migrations]

SQL File Loading
----------------

For loading SQL from cloud storage:

.. code-block:: bash

   pip install sqlspec[asyncpg,litestar,fsspec]

Verification
============

Verify your installation:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.extensions.litestar import SQLSpecPlugin

   # Check imports work
   print("✅ SQLSpec Litestar extension installed successfully")

   # Check adapter imports
   try:
       from sqlspec.adapters.asyncpg import AsyncpgConfig
       print("✅ AsyncPG adapter available")
   except ImportError:
       print("❌ AsyncPG adapter not installed")

   try:
       from litestar import Litestar
       print("✅ Litestar installed")
   except ImportError:
       print("❌ Litestar not installed - run: pip install litestar")

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

Run Litestar extension tests:

.. code-block:: bash

   # Run all Litestar tests
   uv run pytest tests/integration/extensions/test_litestar/ -v

   # Run specific test file
   uv run pytest tests/integration/extensions/test_litestar/test_plugin.py -v

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

With the Litestar extension installed, proceed to the :doc:`quickstart` guide to create your first Litestar application with SQLSpec!

See Also
========

- :doc:`quickstart` - Get started in 5 minutes
- :doc:`dependency_injection` - Learn about dependency injection
- :doc:`/getting_started/installation` - General SQLSpec installation
