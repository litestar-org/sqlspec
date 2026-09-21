Filtering & Pagination
======================

SQLSpec provides filter types and pagination helpers that work with any driver.
The Litestar and FastAPI extensions include auto-generated filter dependencies
for REST APIs.

Pagination with SQL Objects
---------------------------

Use ``SQL.paginate()`` to add LIMIT/OFFSET to any query, and ``select_with_total``
to get both the page data and the total matching count.

.. literalinclude:: /examples/patterns/pagination.py
   :language: python
   :caption: ``pagination patterns``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

.. _cursor-pagination:

Cursor Pagination
-----------------

Cursor pagination starts each page after a row's sort-key values instead of
skipping rows with an offset. Use it to browse a large result set in either
direction. Each page includes ``items``, ``limit``, ``next_cursor``,
``previous_cursor``, ``has_next``, and ``has_previous``; it does not run a
count query or return a total.

.. literalinclude:: /examples/patterns/cursor_pagination.py
   :language: python
   :caption: ``cursor pagination``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Pass a returned token unchanged to the next request. A ``next_cursor`` moves
forward; a ``previous_cursor`` moves back while keeping the same row order.
A missing token means there is no page in that direction. Use
``SQLSpecAsyncService.paginate()`` with an async session.

For direct driver access, pass the cursor filter after other filters to
``select()``, then build the page from the returned rows:

.. code-block:: python

    cursor_filter = CursorFilter(keys, limit=10)
    page = cursor_filter.build_page(session.select(query, *filters, cursor_filter))

With an async driver, await the query before building the page:

.. code-block:: python

    rows = await session.select(query, *filters, cursor_filter)
    page = cursor_filter.build_page(rows)

Use ``paginate(..., schema_type=Item)`` on a service when you need
response conversion after cursor keys have been read.

Choosing keys
~~~~~~~~~~~~~

Use ``CursorFilter("id", limit=20)`` for one ascending key, or
``CursorFilter(["created_at", "id"], limit=20)`` for several ascending keys.
For explicit directions, pass pairs such as
``CursorFilter([("created_at", "desc"), ("id", "desc")], limit=20)``.
You can mix these forms with ``CursorKey`` objects when a key needs NULL
placement or a result alias. Framework ``cursor_keys`` settings accept the same
forms.

Choose stable sort columns and make the final key unique for each result row.
For example, a timestamp may be shared by many rows, so follow it with the
primary key. Keep the keys, directions, and NULL placement the same when you
reuse a token.

Select every key column, even when ``schema_type`` leaves it out of the response.
For a qualified name such as ``items.created_at``, the default result name is
``created_at``. If the query gives that column an alias, name it with
``CursorKey("items.created_at", "desc", result_name="created")``. Grouped,
``DISTINCT``, and set-operation queries must expose all keys as output columns.

NULL values
~~~~~~~~~~~

For a nullable key, set ``nulls="first"`` or ``nulls="last"`` on ``CursorKey``.
For cursor keys, ``nulls=None`` declares that the key is non-nullable. A NULL
key value in a returned row raises ``ImproperConfigurationError``.
This differs from ``OrderByFilter``, where ``nulls=None`` uses the database's
NULL order.

Signed cursors
~~~~~~~~~~~~~~

Pass ``secret=`` to ``CursorFilter`` to sign tokens with HMAC. Use the same
secret for every request that shares tokens, and keep it in server-side
configuration. Public endpoints should set a secret: without one, a client
can change the values and types carried by a token.

Cursors are encoded, not encrypted. Clients can read the sort-key values,
even when ``schema_type`` omits those columns from the returned items. A
signature prevents changes to the token; the query's own ``WHERE`` clause
remains the access-control boundary. Apply tenant and user restrictions on
every request.

Invalid cursors
~~~~~~~~~~~~~~~

Malformed tokens, bad signatures, and tokens for a different ordering raise
``sqlspec.exceptions.InvalidCursorError``. Catch that error at an application
boundary if you use the driver directly. The framework filter providers map
it to a validation response: HTTP 400 in Litestar and HTTP 422 in FastAPI.

Custom value types
~~~~~~~~~~~~~~~~~~

Built-in encoders preserve common Python values, including dates, timestamps,
UUIDs, decimals, and bytes. Register other types once at application startup
with a unique tag and functions that turn the value into text and back:

.. code-block:: python

    from ipaddress import IPv4Address
    from sqlspec.core import register_cursor_type

    register_cursor_type(IPv4Address, "ipv4", str, IPv4Address)

The database driver must still support binding the restored value.

Timestamp precision
~~~~~~~~~~~~~~~~~~~

Use key values with the same precision as the stored column. A type that
rounds timestamps, such as SQL Server's legacy ``datetime``, can re-match a
boundary row when the Python value has finer precision. Prefer a type that
preserves the full value, and keep a unique final key to break ties.

Query shape
~~~~~~~~~~~

The cursor filter replaces the query's ``ORDER BY``, ``LIMIT``, and ``OFFSET``.
It fetches one extra row to detect another page, then removes that row from
``items``. Grouped, ``DISTINCT``, and set-operation queries are wrapped in a
subquery before cursor predicates are applied. In that case, these changes apply
to the outer query; inner ``LIMIT`` and ``OFFSET`` bounds stay in place and cap
the rows eligible for paging. Pass exactly one ``CursorFilter``
to ``paginate()``; it applies other filters first, then the cursor filter.
When using ``select()`` directly, place the cursor filter last and pass its
returned rows to ``build_page()``.
Do not combine cursor pagination with offset pagination or a separate
``OrderByFilter``.

Core Filter Types
-----------------

SQLSpec defines filter types in ``sqlspec.core`` that can be used independently
or with framework integrations:

- ``CursorFilter(keys, limit, cursor=None, secret=None)`` -- cursor pagination
- ``CursorKey(field_name, sort_order="asc", nulls=None, result_name=None)`` -- a cursor sort key
- ``LimitOffsetFilter(limit, offset)`` -- limit and offset based pagination
- ``OrderByFilter(field_name, sort_order, nulls=None)`` -- sorting (supports expression mode)
- ``SearchFilter(field_name, value, ignore_case)`` -- text search (LIKE / ILIKE)
- ``NotInSearchFilter(field_name, value, ignore_case)`` -- negative text search (NOT LIKE / NOT ILIKE)
- ``BeforeAfterFilter(field_name, before, after)`` -- date / timestamp range
- ``OnBeforeAfterFilter(field_name, on_date, before, after)`` -- exact date or bounded range
- ``InCollectionFilter(field_name, values)`` -- set membership (IN)
- ``NotInCollectionFilter(field_name, values)`` -- set exclusion (NOT IN)
- ``AnyCollectionFilter(field_name, values)`` -- collection contains all values
- ``NotAnyCollectionFilter(field_name, values)`` -- collection contains none of values
- ``InAnyFilter(field_name, values)`` -- collection contains any of values
- ``NullFilter(field_name)`` -- IS NULL check
- ``NotNullFilter(field_name)`` -- IS NOT NULL check
- ``BooleanFilter(field_name, value)`` -- boolean comparison
- ``ChoicesFilter(field_name, choices)`` -- enumerated choice matching
- ``OffsetPagination(items, limit, offset, total)`` -- pagination response container dataclass

``OrderByFilter`` leaves NULL placement to the database by default. Pass
``nulls="first"`` or ``nulls="last"`` to put NULL rows first or last.

Qualified Field Names
~~~~~~~~~~~~~~~~~~~~~

Every field-name-bearing filter supports table-qualified field names (e.g. ``p.name``).
SQLSpec correctly parses these into qualified SQLGlot column references and sanitizes
generated parameter names (e.g. ``p_name_search``), making filters safe to use in
joined queries.

.. code-block:: python

    # Disambiguate columns in a JOIN
    query = sql.select("p.name", "c.name").from_("parent p").join("child c", "p.id = c.parent_id")
    filter_obj = SearchFilter(field_name="p.name", value="alice")
    # Results in: WHERE p.name LIKE :p_name_search

Expression Mode
~~~~~~~~~~~~~~~

Filters like ``OrderByFilter`` support passing a SQLGlot expression instead of a
string field name. This allows complex sorting and filtering logic:

.. code-block:: python

    from sqlglot import exp

    # Sort by COALESCE(lines, 0)
    expr = exp.func("COALESCE", exp.column("lines"), exp.Literal.number(0))
    filter_obj = OrderByFilter(field_name=expr, sort_order="desc")

Search Patterns
~~~~~~~~~~~~~~~

``SearchFilter`` and ``NotInSearchFilter`` expose a ``like_pattern`` property
that returns the percent-wrapped search value (e.g. ``%alice%``). This is useful
when you need to use the pattern construction logic outside of the filter system.

Framework Filter Dependencies
-----------------------------

When using the Litestar extension, ``create_filter_dependencies()`` auto-generates
Litestar dependency providers from a declarative configuration. FastAPI provides
the same filter contract through ``SQLSpecPlugin.provide_filters()`` for use with
``Depends()``. These providers parse query parameters from incoming requests and
produce filter objects.

.. literalinclude:: /examples/patterns/filter_dependencies.py
   :language: python
   :caption: ``Litestar filter dependency generation``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Using filters in a Litestar handler:

.. code-block:: python

    from litestar import get
    from sqlspec import sql
    from sqlspec.adapters.asyncpg import AsyncpgDriver
    from sqlspec.core import FilterTypes
    from sqlspec.extensions.litestar.providers import create_filter_dependencies

    user_filter_deps = create_filter_dependencies({
        "pagination_type": "limit_offset",
        "pagination_size": 20,
        "pagination_max_size": 1000,
        "sort_field": ["created_at", "uploaded_collections", "name"],
        "sort_order": "desc",
        "search": "name,email",
    })

    @get("/users", dependencies=user_filter_deps)
    async def list_users(
        db_session: AsyncpgDriver,
        filters: list[FilterTypes],
    ) -> dict:
        query = sql.select("*").from_("users")
        data, total = await db_session.select_with_total(query, *filters)
        return {"data": data, "total": total}

The generated dependencies automatically handle query parameters for configured fields like
``?currentPage=2&pageSize=10&searchString=alice&orderBy=uploadedCollections&sortOrder=asc``.
Camelized ``orderBy`` values are accepted by default for every configured
``sort_field`` value, so ``orderBy=uploadedCollections`` is normalized to the
SQL-facing field ``uploaded_collections`` before the ``OrderByFilter`` is
created. Raw configured values such as ``orderBy=uploaded_collections`` also
remain accepted for compatibility.

Generated framework filters reject ``pageSize`` above ``pagination_max_size``
(default ``1000``). Set this key in ``FilterConfig`` to change the limit;
``pagination_size`` must not exceed it.

Sort aliases are closed over the configured ``sort_field`` allowlist. Use
``sort_field_aliases`` when the public API name is not a mechanical camel-case
conversion, or set ``sort_field_camelize=False`` to require raw configured
``orderBy`` values only:

.. code-block:: python

    user_filter_deps = create_filter_dependencies({
        "sort_field": ["created_at", "uploaded_collections"],
        "sort_field_aliases": {"lastUpload": "uploaded_collections"},
    })

    snake_case_only_filter_deps = create_filter_dependencies({
        "sort_field": ["created_at", "uploaded_collections"],
        "sort_field_camelize": False,
    })

``orderBy=lastUpload`` is accepted, but aliases that target fields outside
``sort_field`` are rejected when the provider is created. Unknown ``orderBy``
values still fail validation before reaching SQL construction.

For FastAPI, use the same configuration with ``Depends()``:

.. code-block:: python

    from fastapi import Depends
    from sqlspec.core import StatementFilter
    from sqlspec.extensions.fastapi import provide_filters

    @app.get("/items")
    async def list_items(
        filters: list[StatementFilter] = Depends(
            provide_filters({
                "sort_field": ["created_at", "uploaded_collections"],
            })
        ),
    ):
        ...

SQLSpec does not ship generated filter providers for Flask, Starlette, or Sanic;
their integrations do not have a runtime ``orderBy`` alias surface.

Cursor filter dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~

Set ``pagination_type="cursor"`` and provide the ordered keys. Keep a signing
secret in server configuration and pass it as ``cursor_secret``:

.. code-block:: python

    import os
    from dataclasses import dataclass
    from datetime import datetime
    from typing import cast

    from litestar import get
    from litestar.di import NamedDependency
    from litestar.params import SkipValidation
    from sqlspec.adapters.asyncpg import AsyncpgDriver
    from sqlspec.core import CursorPagination, FilterTypes
    from sqlspec.extensions.litestar.providers import create_filter_dependencies
    from sqlspec.service import SQLSpecAsyncService

    @dataclass
    class Item:
        id: int
        name: str
        created_at: datetime

    cursor_deps = create_filter_dependencies({
        "pagination_type": "cursor",
        "cursor_keys": [("created_at", "desc"), ("id", "desc")],
        "cursor_secret": os.environ["CURSOR_SECRET"],
        "pagination_size": 20,
    })

    @get("/items", dependencies=cursor_deps)
    async def list_cursor_items(
        db_session: AsyncpgDriver,
        filters: NamedDependency[SkipValidation[list[FilterTypes]]],
    ) -> CursorPagination[Item]:
        service = SQLSpecAsyncService(db_session)
        page = await service.paginate(
            "SELECT id, name, created_at FROM items", *filters, schema_type=Item
        )
        return cast("CursorPagination[Item]", page)

Clients send ``cursor`` and ``pageSize``. The first request omits ``cursor``;
subsequent requests pass ``next_cursor`` or ``previous_cursor`` from the response.
The default page size is 20 and the default maximum is 1000; set
``pagination_max_size`` to change the cap.

When ``sort_field`` is configured, clients may also use ``orderBy`` and
``sortOrder``. The provider keeps the unique final cursor key as a tiebreaker.
Changing the requested ordering starts a new traversal; an old token for a
different ordering fails validation. The returned filter list contains a
``CursorFilter`` instead of separate offset and ordering filters.

Service Layer
-------------

For common database operations and pagination in application services, SQLSpec provides
base classes ``SQLSpecAsyncService`` and ``SQLSpecSyncService`` in ``sqlspec.service``.

.. code-block:: python

    from dataclasses import dataclass
    from sqlspec import sql
    from sqlspec.adapters.asyncpg import AsyncpgDriver
    from sqlspec.core import OffsetPagination, StatementFilter
    from sqlspec.service import SQLSpecAsyncService

    @dataclass
    class User:
        id: int
        name: str

    class UserService(SQLSpecAsyncService[AsyncpgDriver]):
        async def list_users(self, filters: list[StatementFilter]) -> OffsetPagination[User]:
            query = sql.select("*").from_("users")
            return await self.paginate(query, *filters, schema_type=User)

    async def some_handler(db_session: AsyncpgDriver, filters: list[StatementFilter]) -> OffsetPagination[User]:
        service = UserService(db_session)
        return await service.list_users(filters)

Related Guides
--------------

- :doc:`drivers_and_querying` for ``select_with_total`` and query methods.
- :doc:`query_builder` for building queries with ``.where()`` clauses.
- :doc:`/recipes/service_layer` for building robust application service layers.
- :doc:`/reference/core/filters` for the core filter classes and parameters API.
