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

How Query Names Work
--------------------

- Name queries with ``-- name: query_name`` comments.
- SQLSpec normalizes names to snake_case for Python access.
- Add ``-- dialect: postgres`` on the first line of a block to bind SQL to a dialect.
- Declare parameters with ``-- param: <name> <type>[?] [description]`` (see `Declared Parameters`_).
- Directory structures become namespaces when you load directories (``reports/daily.sql`` -> ``reports.<query>``).
