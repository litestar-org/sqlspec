Query Builder
=============

.. image:: /_static/demos/query_builder.gif
   :alt: SQLSpec query builder demo
   :class: demo-gif

SQLSpec includes a fluent query builder for teams who prefer structured SQL construction.
The builder outputs ``SQL`` objects that can be executed with the same driver APIs.

Selects
-------

.. literalinclude:: /examples/builder/select_query.py
   :language: python
   :caption: ``select query``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Inserts and Updates
-------------------

.. literalinclude:: /examples/builder/insert_query.py
   :language: python
   :caption: ``insert query``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

.. literalinclude:: /examples/builder/update_query.py
   :language: python
   :caption: ``update query``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Upserts (ON CONFLICT)
---------------------

Use ``.on_conflict()`` to handle insert conflicts. Chain ``.do_nothing()`` to skip
conflicting rows, or ``.do_update(**columns)`` to update them.

Dialects natively supporting ``ON CONFLICT`` (PostgreSQL, CockroachDB, SQLite, and DuckDB)
render standard ``ON CONFLICT`` syntax. For MySQL and MariaDB, the builder automatically
transpiles ``.on_conflict().do_update()`` to ``ON DUPLICATE KEY UPDATE``, and ``.do_nothing()``
to a no-op self-assignment (e.g., ``col = col``). Dialects without native upsert clauses
(Oracle, T-SQL / SQL Server, Spanner, and BigQuery) raise :class:`~sqlspec.exceptions.SQLBuilderError`
at build time advising the use of :func:`sql.merge`.

.. literalinclude:: /examples/builder/upsert.py
   :language: python
   :caption: ``upsert with on_conflict``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Raw Expressions and RETURNING
------------------------------

Use ``sql.raw()`` to embed raw SQL fragments (like database functions) inside
builder queries. Use ``.returning()`` on INSERT, UPDATE, or DELETE to get back
the affected rows.

.. literalinclude:: /examples/builder/raw_expressions.py
   :language: python
   :caption: ``raw expressions and returning``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Joins
-----

.. literalinclude:: /examples/builder/complex_joins.py
   :language: python
   :caption: ``join query``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Query Modifiers
---------------

Row-level locking clauses such as ``.for_update()`` and ``.for_share()`` are validated against
dialect capabilities at build time. On dialects without row locking support (T-SQL, SQLite,
DuckDB, Spanner, and BigQuery), building a locked query raises :class:`~sqlspec.exceptions.SQLBuilderError`.
Similarly, ``skip_locked=True`` is validated against the dialect's ``supports_skip_locked`` capability.
The builder also normalizes common dialect aliases during build (e.g., ``mssql`` to ``tsql``,
``mariadb`` to ``mysql``, and ``cockroachdb`` to ``postgres``).

.. literalinclude:: /examples/builder/query_modifiers.py
   :language: python
   :caption: ``where helpers + pagination``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Related Guides
--------------

- :doc:`drivers_and_querying` for execution behavior and transaction management.
- :doc:`filtering` for filter types, order by, and pagination helpers.
- :doc:`/reference/builder/index` for the full query builder, DDL, and expression API reference.
