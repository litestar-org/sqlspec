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
Core Filter Types
-----------------

SQLSpec defines filter types in ``sqlspec.core`` that can be used independently
or with framework integrations:

- ``LimitOffsetFilter(limit, offset)`` -- pagination
- ``OrderByFilter(field_name, sort_order)`` -- sorting (supports expression mode)
- ``SearchFilter(field_name, value, ignore_case)`` -- text search
- ``BeforeAfterFilter(field_name, before, after)`` -- date range
- ``InCollectionFilter(field_name, values)`` -- set membership
- ``NotInCollectionFilter(field_name, values)`` -- set exclusion
- ``NullFilter(field_name)`` -- IS NULL check
- ``NotNullFilter(field_name)`` -- IS NOT NULL check

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

    from litestar import Litestar, get
    from sqlspec.core import FilterTypes
    from sqlspec.extensions.litestar.providers import FilterConfig, create_filter_dependencies

    user_filter_deps = create_filter_dependencies({
        "pagination_type": "limit_offset",
        "pagination_size": 20,
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

    filters = Depends(
        db_ext.provide_filters({
            "sort_field": ["created_at", "uploaded_collections"],
        })
    )

SQLSpec does not ship generated filter providers for Flask, Starlette, or Sanic;
their integrations do not have a runtime ``orderBy`` alias surface.

Service Layer
-------------

For common database operations and pagination in application services, SQLSpec provides
base classes ``SQLSpecAsyncService`` and ``SQLSpecSyncService`` in ``sqlspec.service``.

.. code-block:: python

    from sqlspec.service import SQLSpecAsyncService
    from sqlspec.core.filters import LimitOffsetFilter

    class UserService(SQLSpecAsyncService):
        async def list_users(self, filters: list[StatementFilter]) -> OffsetPagination[User]:
            query = sql.select("*").from_("users")
            return await self.paginate(query, *filters, schema_type=User)

    async def some_handler(db_session: AsyncDriver, filters: list[StatementFilter]):
        service = UserService(db_session)
        page = await service.list_users(filters)
        return page  # Returns OffsetPagination container

Related Guides
--------------

- :doc:`drivers_and_querying` for ``select_with_total`` and query methods.
- :doc:`query_builder` for building queries with ``.where()`` clauses.
