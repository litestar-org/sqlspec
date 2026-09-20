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

UPDATE ... FROM and CTEs
------------------------

Builder queries support ``UPDATE ... FROM`` with subqueries or Common Table Expressions (CTEs),
enabling queue claim statements and batch updates across supported dialects.

.. code-block:: python

    from sqlspec import sql

    claim_query = (
        sql.update("tasks")
        .set(status="processing")
        .from_(
            sql.select("id")
            .from_("tasks")
            .where_eq("status", "pending")
            .limit(1)
            .for_update(skip_locked=True),
            alias="sub",
        )
        .where("tasks.id = sub.id")
        .returning(sql.column("id", table="tasks"))
    )

Dialect Support Matrix
~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * - Dialect
     - UPDATE ... FROM Support
     - Notes
   * - PostgreSQL
     - Yes
     - Native ``UPDATE ... FROM`` with ``RETURNING`` and row locking
   * - CockroachDB
     - Yes
     - Native ``UPDATE ... FROM``
   * - SQLite
     - Yes
     - Native ``UPDATE ... FROM`` (SQLite 3.33.0+)
   * - DuckDB
     - Yes
     - Native ``UPDATE ... FROM``
   * - SQL Server (MSSQL)
     - Yes
     - Native ``UPDATE ... FROM``
   * - MySQL / MariaDB
     - No
     - Raises ``SQLBuilderError``; use multi-table join update or MERGE
   * - Oracle
     - No
     - Builder conservatively raises ``SQLBuilderError``; use MERGE or raw SQL on Oracle 23+
   * - Spanner
     - No
     - Raises ``SQLBuilderError``
   * - BigQuery
     - Yes
     - Native ``UPDATE ... FROM``; a ``WHERE`` condition is required

Upserts (ON CONFLICT)
---------------------

Use ``.on_conflict()`` to handle insert conflicts. Chain ``.do_nothing()`` to skip
conflicting rows, or ``.do_update(**columns)`` to update them.

Dialects natively supporting ``ON CONFLICT`` (PostgreSQL, CockroachDB, SQLite, DuckDB, and Spanner)
render standard ``ON CONFLICT`` syntax. For MySQL and MariaDB, the builder automatically
transpiles ``.on_conflict().do_update()`` to ``ON DUPLICATE KEY UPDATE``, and ``.do_nothing()``
to a no-op self-assignment (e.g., ``col = col``). This requires a conflict column or
explicit insert columns. MySQL handles conflicts on any unique key, regardless of the
requested conflict target; the no-op update can still fire update triggers. References
to ``excluded.column`` in update expressions become ``VALUES(column)``. Dialects without native upsert clauses
(Oracle, T-SQL / SQL Server, and BigQuery) raise :class:`~sqlspec.exceptions.SQLBuilderError`
in both ``build()`` and ``to_statement()`` advising the use of :func:`sql.merge`.

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
dialect capabilities at build time. On dialects without these locking clauses (T-SQL, SQLite,
DuckDB, and BigQuery), building a locked query raises :class:`~sqlspec.exceptions.SQLBuilderError`.
Oracle also rejects ``.for_share()``; MariaDB renders it as ``LOCK IN SHARE MODE``
and rejects ``of=`` targets for all locking clauses. PostgreSQL key lock variants
are rejected on other dialect families.
Spanner supports plain ``FOR UPDATE`` in both SQL modes, but rejects shared locks,
``SKIP LOCKED``, ``NOWAIT``, and ``OF`` modifiers. Its PostgreSQL mode requires conflict
updates to assign every inserted column from the matching ``excluded`` column and
does not accept conflict predicates or named constraints.
Similarly, ``skip_locked=True`` is validated against the dialect's ``supports_skip_locked`` capability.
The builder also normalizes common dialect aliases during build (e.g., ``mssql`` to ``tsql``,
``mariadb`` to ``mysql``, and ``cockroachdb`` to ``postgres``).

.. literalinclude:: /examples/builder/query_modifiers.py
   :language: python
   :caption: ``ordering, pagination, and row-level locking``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Deletes
-------

Construct DELETE statements with target tables, conditions, and optional RETURNING clauses:

.. code-block:: python

    from sqlspec import sql

    # DELETE FROM users WHERE status = 'inactive'
    delete_query = sql.delete("users").where_eq("status", "inactive")

    # DELETE with RETURNING on supported dialects
    delete_returning = sql.delete("tasks").where_eq("completed", True).returning("id", "title")

Dynamic Updates with Model Dumps
--------------------------------

The update builder's ``.set_from()`` method accepts dataclasses, msgspec Structs, Pydantic models, or dictionaries, automatically mapping fields to column assignments:

.. code-block:: python

    from dataclasses import dataclass
    from sqlspec import sql

    @dataclass
    class UserProfile:
        name: str
        email: str

    profile = UserProfile(name="Ada Lovelace", email="ada@example.com")

    # UPDATE users SET name = :name, email = :email WHERE id = :id
    query = sql.update("users").set_from(profile).where_eq("id", 1)

Merge Statements
----------------

For dialects that do not natively support ``ON CONFLICT`` (such as Oracle, T-SQL, and BigQuery), or for complex conditional matching, use ``sql.merge()``:

.. code-block:: python

    from sqlspec import sql

    # MERGE INTO target USING source ON target.id = source.id
    merge_query = (
        sql.merge("target_table", dialect="postgres")
        .using("source_table", "s")
        .on("target_table.id = s.id")
        .when_matched_then_update({"status": "s.status"})
    )

Set Operations
--------------

Combine queries using ``.union()``, ``.intersect()``, or ``.except_()``:

.. code-block:: python

    from sqlspec import sql

    query_a = sql.select("id", "name").from_("active_users")
    query_b = sql.select("id", "name").from_("archived_users")

    # UNION ALL via all_=True
    all_users = query_a.union(query_b, all_=True)

DDL Construction
----------------

Create tables, indexes, schemas, and views programmatically:

.. code-block:: python

    from sqlspec import sql

    create_table = (
        sql.create_table("users")
        .column("id", "integer", primary_key=True)
        .column("username", "text", nullable=False)
        .column("email", "text", unique=True)
    )

    create_idx = sql.create_index("idx_users_email", "users", "email")

Related Guides
--------------

- :doc:`drivers_and_querying` for execution behavior and transaction management.
- :doc:`filtering` for filter types, order by, and pagination helpers.
- :doc:`/reference/builder/index` for the full query builder, DDL, and expression API reference.
