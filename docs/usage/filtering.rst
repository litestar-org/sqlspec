Filtering & Pagination
======================

SQLSpec provides filter types and pagination helpers that work with any driver.
The Litestar extension includes auto-generated filter dependencies for REST APIs.

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

Litestar Filter Dependencies

-----------------------------

When using the Litestar extension, ``create_filter_dependencies()`` auto-generates
Litestar dependency providers from a declarative configuration. These providers
parse query parameters from incoming requests and produce filter objects.

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
        "sort_field": ["created_at", "name"],
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
``?currentPage=2&pageSize=10&searchString=alice&orderBy=name&sortOrder=asc``.

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
