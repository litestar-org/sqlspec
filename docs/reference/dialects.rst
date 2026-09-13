========
Dialects
========

SQLSpec registers custom `sqlglot <https://github.com/tobymao/sqlglot>`_ dialects
that extend built-in SQL grammars with extension-specific operators. These dialects
enable the :doc:`builder <builder/index>` to parse and generate SQL that uses
vendor-specific syntax (e.g., pgvector distance operators, ParadeDB search operators).

Import ``sqlspec.dialects`` to ensure all dialects are registered::

   import sqlspec.dialects  # registers pgvector, paradedb, spanner, spangres

Performance builds compile the custom dialect helper modules alongside
``sqlglot[c]``: generator transforms, operator registries, and compatibility
helpers. The small SQLGlot subclass/registration modules stay interpreted
because native-compiling those tokenizer/dialect classes is not runtime-safe.

PostgreSQL Extensions
=====================

PGVector
--------

.. autoclass:: sqlspec.dialects.postgres.PGVector
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

.. autoclass:: sqlspec.dialects.postgres.ParadeDB
   :members:
   :show-inheritance:
   :no-index:

Extends PGVector with ParadeDB pg_search operators:

.. list-table::
   :header-rows: 1

   * - Operator
     - Description
   * - ``@@@``
     - BM25 full-text search / complex query expressions
   * - ``&&&``
     - Conjunction match (all tokenized terms must match)
   * - ``|||``
     - Disjunction match (any tokenized term matches)
   * - ``===``
     - Exact term match (no tokenization of right-hand side)
   * - ``###``
     - Exact phrase match (token order and position enforced)
   * - ``##``
     - Proximity match in any order (``'a' ## n ## 'b'``)
   * - ``##>``
     - Ordered proximity match (left term must appear first)

Scoring and snippets in ParadeDB are standard SQL functions (``pdb.score()``,
``pdb.snippet()``), so they require no custom operator syntax.

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

.. autofunction:: sqlspec.builder.VectorDistance
   :no-index:
