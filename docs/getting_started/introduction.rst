============
Introduction
============

.. grid:: 1
   :padding: 0
   :gutter: 2

   .. grid-item-card::

      **SQLSpec is a SQL first data access layer.**
      It keeps you close to SQL while adding type-safe results, consistent driver APIs, and optional
      tooling for query construction and observability.

What SQLSpec does
-----------------

.. grid:: 1 1 2 2
   :gutter: 2
   :padding: 0

   .. grid-item-card:: First-class SQL

      Write the SQL you want. SQLSpec validates and normalizes statements before execution.

   .. grid-item-card:: Type-safe results

      Map rows into typed objects like msgspec, Pydantic, or dataclasses.

   .. grid-item-card:: Unified connectivity

      One API for sync/async drivers across PostgreSQL, SQLite, DuckDB, MySQL, Oracle, and more.

   .. grid-item-card:: Optional builder + observability

      Use the fluent SQL builder and instrument queries with OpenTelemetry or Prometheus.

Not an ORM
----------

SQLSpec is intentionally **not** an ORM. It optimizes for SQL-first workflows.

.. list-table::
   :header-rows: 1
   :widths: 24 38 38

   * - Focus
     - SQLSpec
     - Traditional ORM
   * - Abstraction
     - Minimal, SQL-centric
     - Model-centric, hides SQL
   * - Core use case
     - Query mapper + data integration
     - Object graphs + unit-of-work
   * - Data workflows
     - Fast, explicit, Arrow-friendly
     - Heavier mapping and state tracking

Good fit when
-------------

- You prefer writing SQL and want predictable behavior.
- You need consistent APIs across multiple databases.
- You care about performance, type safety, and data‑engineering workflows.
- You want optional tools rather than a full ORM stack.
