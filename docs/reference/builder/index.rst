=======
Builder
=======

The builder module provides a fluent API for composing SQL statements with method
chaining, filters, and dialect-aware compilation.

.. grid:: 2

   .. grid-item-card:: Queries
      :link: queries
      :link-type: doc

      SELECT, INSERT, UPDATE, DELETE, and MERGE builders.

   .. grid-item-card:: DDL
      :link: ddl
      :link-type: doc

      CREATE TABLE, ALTER TABLE, DROP, indexes, views, and schemas.

   .. grid-item-card:: Expressions
      :link: expressions
      :link-type: doc

      Column definitions, function columns, aggregates, and expression wrappers.

   .. grid-item-card:: Factory
      :link: factory
      :link-type: doc

      SQLFactory entry point and BuiltQuery result type.

Quick Reference
===============

.. list-table::
   :header-rows: 1

   * - Builder
     - Purpose
     - Page
   * - ``SQLFactory``
     - Entry point for all builders
     - :doc:`factory`
   * - ``Select``
     - SELECT queries with joins, CTEs, window functions
     - :doc:`queries`
   * - ``Insert``
     - INSERT with values, from-select, conflict handling
     - :doc:`queries`
   * - ``Update``
     - UPDATE with SET, FROM, WHERE
     - :doc:`queries`
   * - ``Delete``
     - DELETE with WHERE, RETURNING
     - :doc:`queries`
   * - ``Merge``
     - MERGE / UPSERT operations
     - :doc:`queries`
   * - ``CreateTable``
     - DDL table creation
     - :doc:`ddl`
   * - ``AlterTable``
     - DDL table modification
     - :doc:`ddl`
   * - ``Column``
     - Column expressions and aliases
     - :doc:`expressions`

.. toctree::
   :hidden:

   queries
   ddl
   expressions
   factory
