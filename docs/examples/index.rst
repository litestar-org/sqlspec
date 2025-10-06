================
Examples Gallery
================

Welcome to the SQLSpec examples gallery! This collection demonstrates real-world usage patterns, database-specific implementations, framework integrations, and advanced features.

.. tip::
   All examples are fully functional and can be run directly. Many include inline dependencies using PEP 723 (``/// script`` blocks) for easy execution with tools like ``uv run``.

Getting Started Examples
========================

Basic Usage
-----------

.. card:: Standalone Demo - Interactive SQL Builder
   :link: standalone_demo.py

   **Comprehensive demonstration** of SQLSpec's capabilities using DuckDB:

   - Advanced SQL query construction patterns
   - Filter composition and pipeline processing
   - Statement analysis and validation
   - Performance instrumentation
   - Interactive CLI mode with rich terminal output

   Perfect for learning SQLSpec's core features interactively.

.. card:: SQL File Loader Demo
   :link: sql_file_loader_demo.py

   Learn how to organize SQL queries in files using **aiosql-style named queries**:

   - Loading SQL from files and URIs
   - Caching behavior and performance
   - Database integration patterns
   - Mixing file-loaded and runtime queries
   - Storage backend usage

   Ideal for projects that prefer SQL files over builder patterns.

Database-Specific Examples
===========================

PostgreSQL Adapters
-------------------

.. grid:: 2

   .. grid-item-card:: AsyncPG (Async)
      :link: asyncpg_example.py

      Fast async PostgreSQL with native connection pooling:

      - Connection pool configuration
      - Query mixins (select, select_one, select_value)
      - Query builder integration
      - Transaction handling
      - Pagination patterns

   .. grid-item-card:: Psycopg (Async)
      :link: psycopg_async_example.py

      Modern async PostgreSQL driver:

      - Async connection management
      - Parameter binding ($1, $2 style)
      - CRUD operations
      - Error handling

   .. grid-item-card:: Psycopg (Sync)
      :link: psycopg_sync_example.py

      Synchronous PostgreSQL operations:

      - Blocking connection patterns
      - Traditional sync workflow
      - Simple setup for scripts

   .. grid-item-card:: psqlpy (High Performance)
      :link: psqlpy_example.py

      Rust-based PostgreSQL driver for maximum performance:

      - Zero-copy result handling
      - Connection pooling
      - Advanced query patterns

SQLite Adapters
---------------

.. grid:: 2

   .. grid-item-card:: SQLite (Sync)
      :link: sqlite_example.py

      Standard synchronous SQLite:

      - File and in-memory databases
      - Simple setup
      - Local development

   .. grid-item-card:: aiosqlite (Async)
      :link: aiosqlite_example.py

      Async SQLite adapter:

      - Non-blocking operations
      - Async/await patterns
      - Integration with async apps

DuckDB
------

.. card:: DuckDB Adapter
   :link: duckdb_example.py

   Embedded analytical database:

   - In-memory analytics
   - Parquet/CSV import
   - OLAP query patterns
   - Fast aggregations

.. card:: Standalone DuckDB Demo
   :link: standalone_duckdb.py

   Advanced DuckDB usage with SQLSpec's SQL builder and filter system.

Other Databases
---------------

.. grid:: 3

   .. grid-item-card:: MySQL (asyncmy)
      :link: asyncmy_example.py

      Async MySQL operations

   .. grid-item-card:: Oracle (Sync)
      :link: oracledb_sync_example.py

      Synchronous Oracle database

   .. grid-item-card:: Oracle (Async)
      :link: oracledb_async_example.py

      Async Oracle operations

   .. grid-item-card:: BigQuery
      :link: bigquery_example.py

      Google Cloud BigQuery integration

   .. grid-item-card:: ADBC
      :link: adbc_example.py

      Arrow Database Connectivity

Framework Integration Examples
===============================

Litestar Web Framework
----------------------

.. grid:: 2

   .. grid-item-card:: Single Database Setup
      :link: litestar_single_db.py

      Simple Litestar app with one database:

      - SQLSpec plugin configuration
      - Dependency injection
      - Route handler patterns
      - Health check endpoints

   .. grid-item-card:: Multi-Database Setup
      :link: litestar_multi_db.py

      Managing multiple databases:

      - Multiple adapter configurations
      - Database-specific route handlers
      - Connection management
      - Cross-database operations

   .. grid-item-card:: AsyncPG Integration
      :link: litestar_asyncpg.py

      Production-ready PostgreSQL setup:

      - Connection pooling
      - Async request handlers
      - Error handling
      - Database introspection

   .. grid-item-card:: Psycopg Integration
      :link: litestar_psycopg.py

      Alternative PostgreSQL setup with psycopg driver

   .. grid-item-card:: DuckLLM Example
      :link: litestar_duckllm.py

      Advanced example combining DuckDB with LLM features

Example Categories
==================

By Use Case
-----------

**Learning SQLSpec Basics:**

1. ``standalone_demo.py`` - Interactive exploration
2. ``asyncpg_example.py`` - Core query patterns
3. ``sqlite_example.py`` - Simple local setup

**Production Applications:**

1. ``litestar_asyncpg.py`` - Web app with PostgreSQL
2. ``sql_file_loader_demo.py`` - Query organization
3. ``litestar_multi_db.py`` - Complex applications

**Performance Critical:**

1. ``psqlpy_example.py`` - Rust-based driver
2. ``duckdb_example.py`` - In-memory analytics
3. ``adbc_example.py`` - Arrow integration

By Database Type
----------------

**Relational (OLTP):**

- PostgreSQL: asyncpg, psycopg, psqlpy
- MySQL: asyncmy
- SQLite: sqlite, aiosqlite
- Oracle: oracledb (sync/async)

**Analytical (OLAP):**

- DuckDB (embedded analytics)
- BigQuery (cloud data warehouse)

**Modern Standards:**

- ADBC (Arrow Database Connectivity)

Running Examples
================

Most examples can be run directly with Python:

.. code-block:: bash

   # Run interactive demo
   python docs/examples/standalone_demo.py

   # Run with uv (automatically installs dependencies from script block)
   uv run docs/examples/litestar_asyncpg.py

   # Run with specific adapter
   python docs/examples/asyncpg_example.py

Some examples require running databases. Use the development infrastructure:

.. code-block:: bash

   # Start all databases
   make infra-up

   # Start specific database
   make infra-postgres   # PostgreSQL on port 5433
   make infra-mysql      # MySQL on port 3306
   make infra-oracle     # Oracle on port 1521

   # Stop infrastructure
   make infra-down

Example File Structure
======================

Each example typically includes:

1. **Docstring** - Explains what the example demonstrates
2. **Dependencies** - PEP 723 script block (where applicable)
3. **Imports** - Required SQLSpec and adapter imports
4. **Configuration** - Database connection setup
5. **Demonstration Code** - Practical usage patterns
6. **Main Function** - Entry point with error handling

Contributing Examples
=====================

Have a useful SQLSpec pattern to share? Contributions are welcome!

See :doc:`/contributing/index` for guidelines on adding new examples.
