=============
API Reference
=============

Auto-generated documentation from source code docstrings. Private methods and attributes
are excluded.

Getting Started
===============

.. grid:: 2

   .. grid-item-card:: SQLSpec Base
      :link: base
      :link-type: doc

      Main entry point for SQLSpec. Configure databases and manage sessions.

   .. grid-item-card:: Database Configuration
      :link: config
      :link-type: doc

      Base config classes: AsyncDatabaseConfig, SyncDatabaseConfig, NoPoolConfig.

Database Layer
==============

.. grid:: 2

   .. grid-item-card:: Database Adapters
      :link: adapters/index
      :link-type: doc

      Adapter implementations for PostgreSQL, SQLite, DuckDB, MySQL, Oracle, and more.

   .. grid-item-card:: Driver System
      :link: driver
      :link-type: doc

      Base driver classes and mixins for sync/async database operations.

Query Building
==============

.. grid:: 3

   .. grid-item-card:: SQL Builder
      :link: builder/index
      :link-type: doc

      Fluent API for building SQL queries: SELECT, INSERT, UPDATE, DELETE, DDL.

   .. grid-item-card:: Query Stack
      :link: query-stack
      :link-type: doc

      Immutable multi-statement execution with native pipelines.

   .. grid-item-card:: Dialects
      :link: dialects
      :link-type: doc

      Custom sqlglot dialects for pgvector, ParadeDB, and Spanner extensions.

Core Infrastructure
===================

.. grid:: 3

   .. grid-item-card:: Core Components
      :link: core/index
      :link-type: doc

      Statement processing, parameters, results, filters, cache, and query modifiers.

   .. grid-item-card:: SQL File Loader
      :link: loader
      :link-type: doc

      Load and cache SQL files with named statement support.

   .. grid-item-card:: Exceptions
      :link: exceptions
      :link-type: doc

      Full exception hierarchy with SQLSTATE mapping.

Operations
==========

.. grid:: 3

   .. grid-item-card:: Migrations
      :link: migrations
      :link-type: doc

      Migration runners, commands, trackers, loaders, and squashing.

   .. grid-item-card:: Storage
      :link: storage
      :link-type: doc

      Storage pipelines, backends, and Arrow table export/import.

   .. grid-item-card:: Observability
      :link: observability
      :link-type: doc

      Telemetry, logging, diagnostics, and Prometheus integration.

Integrations
============

.. grid:: 2

   .. grid-item-card:: Framework Extensions
      :link: extensions/index
      :link-type: doc

      Integration modules for Litestar, FastAPI, Flask, Starlette, and Google ADK.

   .. grid-item-card:: Types & Protocols
      :link: typing
      :link-type: doc

      Type aliases, metadata types, feature flags, and driver protocols.

.. toctree::
   :hidden:

   base
   config
   adapters/index
   driver
   builder/index
   query-stack
   dialects
   core/index
   loader
   exceptions
   migrations
   storage
   observability
   extensions/index
   typing
