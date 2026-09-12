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

Advanced Loader Access
----------------------

Use SQLSpec methods for named queries. Use ``spec.loader`` when you need the
:class:`~sqlspec.loader.SQLFileLoader` itself. It returns the given loader. If
none was given, SQLSpec makes one on first use.

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

.. _declared-parameters:

Declared Parameters
-------------------

Declare a query's parameters inline with ``-- param:`` directives in the header
block. Declared queries become self-documenting, introspectable, and
self-validating -- without SQLSpec becoming an ORM.

.. code-block:: sql

   -- name: get_offers_by_status
   -- dialect: oracle
   -- param: status_cd str         The status code to filter by
   -- param: offer_ids list[int]   List of offer IDs to include
   -- param: limit int             Maximum number of rows to return

   select offer_id, offer_name from offers
   where status_cd = :status_cd and offer_id in (:offer_ids)
   fetch first :limit rows only

The grammar is ``-- param: <name> <type> [description]``, placed alongside
``-- name:`` and ``-- dialect:`` in the leading comment block. Append ``?`` to
the declared type, or end the description with ``(optional)``, to mark a named
parameter as optional.

.. literalinclude:: /examples/sql_files/declared_params.py
   :language: python
   :caption: ``declared parameters``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

**Declaration is opt-in.** A query with **no** ``-- param:`` lines behaves
exactly as before -- same code path, zero overhead. Declaring a parameter opts
*that* query into validation:

- Required declarations must be **supplied** when the query executes.
- Missing optional named declarations are bound as ``None``, so SQL receives
  ``NULL``. The query must still express the intended nullable behavior, for
  example ``(:status_cd is null or status_cd = :status_cd)``.
- Positional placeholders still rely on arity and cannot be omitted by name.
- If its declared type resolves to a Python type, the supplied value must match
  (``isinstance``). Pass ``None`` for SQL ``NULL`` -- the key is still present and
  the type check is skipped.
- Extra parameters are never rejected -- statement filters legitimately inject
  ``limit``/``offset``, so only *declared* names are checked.

.. code-block:: sql

   -- name: list_offers
   -- param: status_cd str? Optional status filter
   select offer_id, offer_name from offers
   where (:status_cd is null or status_cd = :status_cd)

**Type vocabulary.** Declared types resolve through a fixed allowlist --
``str``, ``int``, ``float``, ``bool``, ``bytes``, ``date``, ``datetime``,
``time``, ``Decimal``, ``uuid`` / ``uuid.UUID``, ``dict``, ``dict[str, Any]``,
``json`` / ``jsonb``, and the container forms ``list``, ``list[int]``,
``list[str]``, ``list[float]``, ``list[bool]``, ``tuple``. ``json`` and
``jsonb`` use SQLSpec's existing JSON serializer to validate that values can be
encoded. The raw string is always stored and **never** evaluated. Register
custom mappings with :func:`~sqlspec.register_param_type`:

.. code-block:: python

   from decimal import Decimal

   from sqlspec import register_param_type

   register_param_type("Money", Decimal)  # -- param: price Money

Type strings that do not resolve are documentation-only -- their values are not
type-checked.

**Validation timing.**

- *Load time* -- declared names are cross-checked against the actual
  ``:placeholders`` (name drift), and declared count against placeholder count for
  positionally-bound queries. Mismatches raise :exc:`~sqlspec.exceptions.SQLSpecError`.
- *Execute time* -- presence and type are enforced for every declared parameter,
  uniformly across every adapter. ``execute_many`` binds missing optional named
  values on each row, then checks the first row only.

A **malformed** ``-- param:`` line (a typo or wrong arity) is a soft warning and
the line is skipped, preserving backward compatibility. Pass
``strict_parameter_annotations=True`` to :class:`~sqlspec.loader.SQLFileLoader`
to escalate malformed annotations to an error. (A genuine *validation mismatch*
-- drift, count, missing, or wrong type -- always raises.)

**Introspection.** Read declarations without executing via
``spec.get_query_parameters(name)`` or the ``declared_parameters`` tuple on the
``SQL`` object returned by ``spec.get_sql(name)``.

.. _sql-fragments-and-slots:

Fragments and Slots
-------------------

Fragments let several named queries share one piece of SQL, such as a chain of
CTEs. Slots mark the parts of a query that are chosen when you ask for it, such
as a ``WHERE`` predicate or an ``ORDER BY`` list. The file stays plain SQL.

.. code-block:: sql

   -- fragment: decision_fact_ctes
   decisions AS (
       SELECT d.id, d.workspace_id, d.strategy
       FROM decision d
       WHERE d.workspace_id = :workspace_id
   ),
   classified AS (
       SELECT id, workspace_id, CASE WHEN strategy = 'lift' THEN 'homogeneous' ELSE 'modernize' END AS journey
       FROM decisions
   )

   -- name: list_efforts
   -- param: workspace_id str
   -- slot: predicates = TRUE
   -- slot: order_by = e.id
   WITH
   /* include: decision_fact_ctes */
   SELECT e.id, e.journey
   FROM classified e
   WHERE /* slot: predicates */
   ORDER BY /* slot: order_by */

   -- name: count_efforts
   -- param: workspace_id str
   WITH
   /* include: decision_fact_ctes */
   SELECT count(*) FROM classified e WHERE /* slot: predicates */

- ``-- fragment: <name>`` starts a fragment section, the same way ``-- name:``
  starts a query. Fragments are never run on their own and do not appear in
  ``list_queries()``. They cannot declare ``-- dialect:``, ``-- param:``, or
  ``-- slot:`` directives.
- ``/* include: <name> */`` is replaced by the fragment's text. Fragments can
  include other fragments, and can live in a different file from the queries that
  use them. Files can be loaded in any order.
- When you load a directory, fragments get the same namespace as queries
  (``shared/ctes.sql`` -> ``shared.<fragment>``). An include looks up the name
  as written first, then inside the namespace of the query or fragment that
  contains it, so ``/* include: shared.decision_fact_ctes */`` reaches across
  namespaces.
- ``/* slot: <name> */`` marks a fill point. ``-- slot: <name> = <default sql>``
  in the query's leading comment block gives it a default: everything after
  ``=`` up to the end of the line. A slot with no default must be filled on every
  call. When a default or a string value contains ``--``, a line break is added
  after it so the rest of the query is not commented out.
- ``add_named_sql()`` accepts the same include and slot markers; slots added
  this way have no defaults.

**Reserved comment shapes.** These comment forms now have meaning in ``.sql``
files: ``-- fragment: <name>`` lines, ``-- slot:`` lines in a query's leading
comment block, and ``/* include: <name> */`` and ``/* slot: <name> */`` block
comments. Marker text inside quoted strings, quoted identifiers, dollar-quoted
bodies, or other comments is ignored. A prose comment such as
``-- Slot: morning`` in a query's leading comment block is read as a slot
declaration, and a ``-- slot:`` line that starts a line after the SQL has begun
is an error. Reword such comments when upgrading.

Pass slot values as keyword arguments to ``get_sql()``. Parameters inside the
fragments are still supplied when the query runs:

.. code-block:: python

   from sqlglot import exp

   from sqlspec import SQL

   spec.load_sql_files("queries/effort.sql")

   stmt = spec.get_sql("list_efforts")  # WHERE TRUE ORDER BY e.id
   stmt = spec.get_sql("list_efforts", order_by="e.journey, e.id DESC")
   stmt = spec.get_sql(
       "list_efforts",
       predicates=SQL("e.journey = :journey", journey="modernize"),
   )
   stmt = spec.get_sql("count_efforts", predicates=exp.column("journey").eq("modernize"))

   rows = session.select(stmt, workspace_id=workspace_id)

A slot value can be one of three types:

- ``str`` -- spliced in as written.
- A sqlglot expression -- rendered with the query's ``-- dialect:``.
- ``SQL`` -- its text is spliced in, and its named parameters are bound on the
  returned statement. Parameters you pass to ``execute()`` or ``select*()`` as
  keyword arguments or a single mapping are added to them, and a value you pass
  for the same name replaces the bound one. ``execute_many()`` does not use
  bound slot parameters; pass every value in its parameter rows instead. A
  ``SQL`` value cannot carry statement filters, and one ``SQL`` object may fill
  several slots.

Placeholders inside slot values must use the ``:name`` style that the SQL file
uses, so they are recognized alongside the query's own placeholders.

.. warning::

   Slot values are SQL, not data. Never put user input into a slot value
   directly. Put it in a parameter instead, for example
   ``SQL("e.journey = :journey", journey=user_value)``.

**Errors.**

- A missing required slot, an unknown slot name, a ``SQL`` value with positional
  parameters or statement filters, a parameter name that two slot values bind to
  different values, or a slot parameter name that the query itself already uses
  raises :exc:`~sqlspec.exceptions.SQLSlotError`.
- A slot value of any other type raises :exc:`TypeError`.
- An unknown fragment raises :exc:`~sqlspec.exceptions.SQLFragmentNotFoundError`, a
  subclass of :exc:`~sqlspec.exceptions.SQLStatementNotFoundError`.
- An include cycle, a duplicate fragment name, a directive on a fragment, a
  ``-- slot:`` line after the SQL has started, or a ``-- slot:`` default with no
  matching marker raises :exc:`~sqlspec.exceptions.SQLFileParseError`.
- For queries that use includes or slots, declared ``-- param:`` names are
  checked against the final SQL when you call ``get_sql()`` instead of at load
  time.

**Caching.** ``get_sql(name)`` without slot values returns the same cached
``SQL`` object on every call. A call with slot values builds a new object each
time. SQLSpec's statement cache is keyed on the final SQL text, so repeating the
same fill still reuses compiled work. Loading or adding a fragment clears the
cached text of every query that includes fragments.

**Programmatic access.** ``spec.loader`` adds, lists, and inspects fragments and
reports a query's slots; see :class:`~sqlspec.loader.SQLFileLoader`.

How Query Names Work
--------------------

- Name queries with ``-- name: query_name`` comments.
- SQLSpec normalizes names to snake_case for Python access.
- Add ``-- dialect: postgres`` on the first line of a block to bind SQL to a dialect.
- Declare parameters with ``-- param: <name> <type>[?] [description]`` (see `Declared Parameters`_).
- Directory structures become namespaces when you load directories (``reports/daily.sql`` -> ``reports.<query>``).
