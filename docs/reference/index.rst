=============
API Reference
=============

The API reference is automatically generated from the docstrings in the code, and is useful for
finding out what methods and attributes are available for a given class.

.. note:: Private methods and attributes are not included in the API reference.

Overview
========

SQLSpec is organized into several core modules, each serving a specific purpose:

**Core Components:**

- :doc:`base` - Main ``SQLSpec`` registry and configuration management
- :doc:`core` - SQL processing (statements, parameters, results, compilation)
- :doc:`driver` - Base driver classes and mixins for database operations

**Database Connectivity:**

- :doc:`adapters` - Database-specific adapters (PostgreSQL, SQLite, DuckDB, etc.)

**Query Building:**

- :doc:`builder` - Fluent SQL builder API for programmatic query construction

**Framework Integration:**

- :doc:`extensions` - Web framework integrations (Litestar, FastAPI, Flask, etc.)

Quick Navigation
================

.. grid:: 2

   .. grid-item-card:: SQLSpec Base
      :link: base
      :link-type: doc

      Main entry point for SQLSpec. Configure databases and manage sessions.

   .. grid-item-card:: Database Adapters
      :link: adapters
      :link-type: doc

      Adapter implementations for PostgreSQL, SQLite, DuckDB, MySQL, Oracle, and more.

   .. grid-item-card:: SQL Builder
      :link: builder
      :link-type: doc

      Fluent API for building SQL queries programmatically with method chaining.

   .. grid-item-card:: Query Stack
      :link: query-stack
      :link-type: doc

      Immutable multi-statement execution with native pipelines, sequential fallbacks, and stack-aware telemetry.

   .. grid-item-card:: Core Components
      :link: core
      :link-type: doc

      Statement processing, parameter binding, result handling, and SQL compilation.

   .. grid-item-card:: Driver System
      :link: driver
      :link-type: doc

      Base driver classes and mixins for sync/async database operations.

   .. grid-item-card:: Framework Extensions
      :link: extensions
      :link-type: doc

      Integration modules for Litestar, FastAPI, Flask, Sanic, and Starlette.

Module Organization
===================

SQLSpec follows a layered architecture:

1. **User-Facing API** (``sqlspec.base``, ``sqlspec.builder``)

   - ``SQLSpec`` - Main registry class
   - SQL Builder - Fluent query construction

2. **Adapter Layer** (``sqlspec.adapters.*``)

   - Database-specific configurations
   - Driver implementations
   - Parameter style handling

3. **Core Processing** (``sqlspec.core``)

   - SQL statement parsing and validation
   - Parameter binding and conversion
   - Result set processing
   - Statement caching

4. **Driver Foundation** (``sqlspec.driver``)

   - Base sync/async drivers
   - Transaction management
   - Query execution mixins

5. **Framework Integration** (``sqlspec.extensions.*``)

   - Dependency injection
   - Lifecycle management
   - Framework-specific utilities

Available API References
========================

.. toctree::
   :hidden:

   base
   adapters
   builder
   query-stack
   core
   driver
   extensions

Common Workflows
================

**Setting Up a Database:**

1. Import adapter config: ``from sqlspec.adapters.asyncpg import AsyncpgConfig``
2. Create SQLSpec instance: ``sql = SQLSpec()``
3. Add configuration: ``db = sql.add_config(AsyncpgConfig(...))``
4. Get session: ``async with sql.provide_session(db) as session:``

**Building Queries:**

1. Import builder: ``from sqlspec import sql``
2. Chain methods: ``query = sql.select("*").from_("users").where("active = true")``
3. Convert to SQL: ``stmt = query.to_statement()``
4. Execute: ``result = await session.execute(stmt)``

**Processing Results:**

1. Execute query: ``result = await session.execute(sql)``
2. Get all rows: ``result.all()`` (list of dicts)
3. Get one row: ``result.one()`` (raises if not exactly one)
4. Get first row: ``result.get_first()`` (returns first or None)
5. Map to models: ``result.all(schema_type=User)`` or ``result.one(schema_type=User)``

See Also
========

- :doc:`/getting_started/index` - Getting started guide
- :doc:`/usage/index` - Usage documentation
- :doc:`/contributing/index` - Contributing guide

Type Hints and Protocols
=========================

SQLSpec makes extensive use of type hints and protocols for type safety:

- ``sqlspec.protocols`` - Protocol definitions for runtime type checking
- ``sqlspec.typing`` - Type aliases and generic types
- ``sqlspec.utils.type_guards`` - Type guard functions

All public APIs are fully typed and compatible with mypy, pyright, and other type checkers.
