===============
SQL File Loader
===============

SQLSpec includes a SQL file loader that keeps your queries in ``.sql`` files and
exposes them through the registry. Load directories or individual files, then
execute named queries with ``spec.get_sql()``.

Load SQL Files
--------------

.. literalinclude:: /examples/sql_files/load_sql_files.py
   :language: python
   :caption: ``load SQL files``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Named Queries
-------------

.. literalinclude:: /examples/sql_files/named_queries.py
   :language: python
   :caption: ``named SQL``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Dynamic WHERE Chaining
----------------------

SQL objects returned by ``get_sql()`` support ``.where()`` chaining. This lets you
start with a base query and add conditions dynamically without string concatenation.

.. literalinclude:: /examples/sql_files/dynamic_where.py
   :language: python
   :caption: ``dynamic where chaining``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Available where helpers:

- ``.where(condition)`` -- raw SQL condition string
- ``.where_eq(column, value)`` -- equality
- ``.where_neq(column, value)`` -- inequality
- ``.where_lt(column, value)`` / ``.where_lte(column, value)`` -- less than
- ``.where_gt(column, value)`` / ``.where_gte(column, value)`` -- greater than
- ``.where_like(column, pattern)`` / ``.where_ilike(column, pattern)`` -- pattern matching
- ``.where_in(column, values)`` / ``.where_not_in(column, values)`` -- set membership
- ``.where_is_null(column)`` / ``.where_is_not_null(column)`` -- null checks
- ``.where_between(column, low, high)`` -- range

Each call returns a new ``SQL`` object (immutable chaining).

How Query Names Work
--------------------

- Name queries with ``-- name: query_name`` comments.
- SQLSpec normalizes names to snake_case for Python access.
- Add ``-- dialect: postgres`` on the first line of a block to bind SQL to a dialect.
- Directory structures become namespaces when you load directories (``reports/daily.sql`` -> ``reports.<query>``).
