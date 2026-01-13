=============
API Reference
=============

Auto-generated documentation from source code docstrings. Private methods and attributes
are excluded.

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

.. toctree::
   :hidden:

   base
   adapters
   builder
   query-stack
   core
   driver
   extensions

Type System
===========

SQLSpec uses type hints and protocols extensively:

- ``sqlspec.protocols`` - Protocol definitions for runtime type checking
- ``sqlspec.typing`` - Type aliases and generic types

All public APIs are fully typed and compatible with mypy and pyright.
