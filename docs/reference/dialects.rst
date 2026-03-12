========
Dialects
========

SQLSpec registers custom `sqlglot <https://github.com/tobymao/sqlglot>`_ dialects
that extend built-in SQL grammars with extension-specific operators. These dialects
enable the :doc:`builder <builder/index>` to parse and generate SQL that uses
vendor-specific syntax (e.g., pgvector distance operators, ParadeDB search operators).

Import ``sqlspec.dialects`` to ensure all dialects are registered::

   import sqlspec.dialects  # registers pgvector, paradedb, spanner, spangres

PostgreSQL Extensions
=====================

PGVector
--------

.. autoclass:: sqlspec.dialects.postgres.pgvector.PGVector
   :members:
   :show-inheritance:
   :no-index:

Adds support for pgvector distance operators:

.. list-table::
   :header-rows: 1

   * - Operator
     - Description
   * - ``<->``
     - L2 (Euclidean) distance
   * - ``<#>``
     - Negative inner product
   * - ``<=>``
     - Cosine distance
   * - ``<+>``
     - L1 (Manhattan) distance
   * - ``<~>``
     - Hamming distance (binary vectors)
   * - ``<%>``
     - Jaccard distance (binary vectors)

ParadeDB
--------

.. autoclass:: sqlspec.dialects.postgres.paradedb.ParadeDB
   :members:
   :show-inheritance:
   :no-index:

Extends PGVector with ParadeDB pg_search operators:

.. list-table::
   :header-rows: 1

   * - Operator
     - Description
   * - ``@@@``
     - BM25 full-text search
   * - ``&&&``
     - Boolean AND search
   * - ``|||``
     - Boolean OR search
   * - ``===``
     - Exact term match
   * - ``###``
     - Score/rank retrieval
   * - ``##``
     - Snippet/highlight retrieval
   * - ``##>``
     - Snippet/highlight with options

Spanner
=======

.. autoclass:: sqlspec.dialects.spanner.Spanner
   :members:
   :show-inheritance:
   :no-index:

.. autoclass:: sqlspec.dialects.spanner.Spangres
   :members:
   :show-inheritance:
   :no-index:

Expression Types
================

.. autoclass:: sqlspec.dialects.postgres.pgvector.VectorDistance
   :members:
   :no-index:

.. autoclass:: sqlspec.dialects.postgres.paradedb.SearchOperator
   :members:
   :no-index:
