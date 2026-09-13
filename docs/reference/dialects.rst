========
Dialects
========

SQLSpec registers custom `sqlglot <https://github.com/tobymao/sqlglot>`_ dialects
that extend built-in SQL grammars with extension-specific operators. These dialects
enable the :doc:`builder <builder/index>` to parse and generate SQL that uses
vendor-specific syntax (e.g., pgvector distance operators, ParadeDB search operators).

The dialects are registered lazily through the ``sqlglot.dialects`` entry-point
group, so ``sqlglot.parse_one(..., dialect="pgvector")`` resolves them in any
environment where SQLSpec is installed; importing ``sqlspec`` alone does not load
them. Import the classes directly when you need them as objects::

   from sqlspec.dialects import PGTextSearch, PGVector, ParadeDB, Spanner, Spangres

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

PGTextSearch
------------

.. autoclass:: sqlspec.dialects.postgres.PGTextSearch
   :members:
   :show-inheritance:
   :no-index:

Adds support for the native PostgreSQL and Google Cloud AlloyDB / AlloyDB Omni ``pg_textsearch`` BM25 extension:

.. list-table::
   :header-rows: 1

   * - Operator
     - Description
   * - ``<@>``
     - BM25 score ranking operator (returns negative score for ASC index scans)

Indices are created with ``USING bm25 (column) WITH (text_config='english')`` and queries order by ``column <@> 'query' ASC``.

ParadeDB
--------

.. autoclass:: sqlspec.dialects.postgres.ParadeDB
   :members:
   :show-inheritance:
   :no-index:

Extends PostgreSQL with ParadeDB (pg_search) operators:

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
