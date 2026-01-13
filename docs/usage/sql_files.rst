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

How Query Names Work
--------------------

- Name queries with ``-- name: query_name`` comments.
- SQLSpec normalizes names to snake_case for Python access.
- Add ``-- dialect: postgres`` on the first line of a block to bind SQL to a dialect.
- Directory structures become namespaces when you load directories (``reports/daily.sql`` → ``reports.<query>``).
