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
- ``OrderByFilter(field_name, sort_order)`` -- sorting
- ``SearchFilter(field_name, value, ignore_case)`` -- text search
- ``BeforeAfterFilter(field_name, before, after)`` -- date range
- ``InCollectionFilter(field_name, values)`` -- set membership
- ``NotInCollectionFilter(field_name, values)`` -- set exclusion
- ``NullFilter(field_name)`` -- IS NULL check
- ``NotNullFilter(field_name)`` -- IS NOT NULL check

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
        "sort_field": "created_at",
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

The generated dependencies automatically handle query parameters like
``?currentPage=2&pageSize=10&searchString=alice&orderBy=name&sortOrder=asc``.

Related Guides
--------------

- :doc:`drivers_and_querying` for ``select_with_total`` and query methods.
- :doc:`query_builder` for building queries with ``.where()`` clauses.
