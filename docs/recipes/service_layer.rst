=====================
Service Layer Pattern
=====================

SQLSpec ships a base service class that wraps a driver and provides pagination,
single-row fetching, existence checks, and transaction helpers. Import it and
subclass it — you do not need to write your own.

Parameters and filters passed as ``*parameters`` are forwarded directly to the
driver, which applies filters to the statement and binds parameters automatically.

Base Service
============

``SQLSpecAsyncService`` and ``SQLSpecSyncService`` live in :mod:`sqlspec.service`.
The five web-framework extensions — ``litestar``, ``fastapi``, ``flask``,
``starlette``, and ``sanic`` — each re-export the same two objects, so
``from sqlspec.extensions.litestar import SQLSpecAsyncService`` gives you the
identical class::

   from sqlspec.service import SQLSpecAsyncService, SQLSpecSyncService

Each takes a driver session in its constructor and exposes it as both
``.session`` and ``.driver``:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Member
     - Purpose
   * - ``paginate(statement, *parameters, schema_type=None, count_with_window=False)``
     - Runs the query and returns an ``OffsetPagination`` with a total count.
   * - ``get_one(statement, *parameters, schema_type=None, error_message=None)``
     - Returns exactly one row, raising if there is not exactly one.
   * - ``exists(statement, *parameters)``
     - Returns ``True`` when the query matches at least one row.
   * - ``begin_transaction()``
     - Context manager that commits on success and rolls back on error.
   * - ``begin()`` / ``commit()`` / ``rollback()``
     - Pass straight through to the driver for manual control.
   * - ``session`` / ``driver``
     - The wrapped driver session.

Subclass whichever matches your driver and add your own query methods:

.. tab-set::

   .. tab-item:: Async

      .. code-block:: python

         from sqlspec.adapters.asyncpg import AsyncpgDriver
         from sqlspec.service import SQLSpecAsyncService


         class UserService(SQLSpecAsyncService[AsyncpgDriver]):
             """Async service for the users table."""

   .. tab-item:: Sync

      .. code-block:: python

         from sqlspec.adapters.sqlite import SqliteDriver
         from sqlspec.service import SQLSpecSyncService


         class UserService(SQLSpecSyncService[SqliteDriver]):
             """Sync service for the users table."""

See :doc:`/reference/service` for the full signatures.

Domain Services
===============

Inherit from the base and use the ``sql`` builder or SQL file loader for queries.
Filters from Litestar dependencies flow straight through ``*filters`` to the driver:

.. tab-set::

   .. tab-item:: Async

      .. code-block:: python

         from typing import TYPE_CHECKING

         from pydantic import BaseModel
         from sqlspec import sql
         from sqlspec.adapters.asyncpg import AsyncpgDriver
         from sqlspec.core.filters import OffsetPagination, StatementFilter
         from sqlspec.service import SQLSpecAsyncService

         if TYPE_CHECKING:
             from uuid import UUID


         class User(BaseModel):
             id: str
             email: str
             name: str


         class UserService(SQLSpecAsyncService[AsyncpgDriver]):

             async def list_with_count(self, *filters: StatementFilter) -> OffsetPagination[User]:
                 return await self.paginate(
                     sql.select("id", "email", "name").from_("users"),
                     *filters,
                     schema_type=User,
                 )

             async def get_user(self, user_id: "UUID") -> User:
                 return await self.get_one(
                     sql.select("id", "email", "name").from_("users").where_eq("id", user_id),
                     schema_type=User,
                     error_message=f"User {user_id} not found",
                 )

             async def create_user(self, email: str, name: str) -> User:
                 return await self.driver.select_one(
                     sql.insert("users").columns("email", "name").values(email, name).returning("id", "email", "name"),
                     schema_type=User,
                 )

   .. tab-item:: Sync

      .. code-block:: python

         from typing import TYPE_CHECKING

         from pydantic import BaseModel
         from sqlspec import sql
         from sqlspec.adapters.sqlite import SqliteDriver
         from sqlspec.core.filters import OffsetPagination, StatementFilter
         from sqlspec.service import SQLSpecSyncService

         if TYPE_CHECKING:
             from uuid import UUID


         class User(BaseModel):
             id: str
             email: str
             name: str


         class UserService(SQLSpecSyncService[SqliteDriver]):

             def list_with_count(self, *filters: StatementFilter) -> OffsetPagination[User]:
                 return self.paginate(
                     sql.select("id", "email", "name").from_("users"),
                     *filters,
                     schema_type=User,
                 )

             def get_user(self, user_id: "UUID") -> User:
                 return self.get_one(
                     sql.select("id", "email", "name").from_("users").where_eq("id", user_id),
                     schema_type=User,
                     error_message=f"User {user_id} not found",
                 )

             def create_user(self, email: str, name: str) -> User:
                 return self.driver.select_one(
                     sql.insert("users").columns("email", "name").values(email, name).returning("id", "email", "name"),
                     schema_type=User,
                 )

Using with Litestar
===================

With ``create_filter_dependencies``, filters are injected from query parameters and
forwarded through the service to the driver.

This continues the ``UserService`` and ``User`` from the previous section, and
provides the service itself alongside the filter dependencies:

.. code-block:: python

   from litestar import Controller, get
   from litestar.di import NamedDependency, Provide
   from litestar.params import SkipValidation
   from sqlspec.adapters.asyncpg import AsyncpgDriver
   from sqlspec.core.filters import FilterTypes, OffsetPagination
   from sqlspec.extensions.litestar.providers import create_filter_dependencies


   async def provide_users_service(db_session: AsyncpgDriver) -> UserService:
       return UserService(db_session)


   class UserController(Controller):
       path = "/api/users"
       dependencies = {
           "users_service": Provide(provide_users_service),
           **create_filter_dependencies(
               {
                   "pagination_type": "limit_offset",
                   "pagination_size": 20,
                   "sort_field": ["created_at", "uploaded_collections", "name"],
                   "sort_order": "desc",
                   "search": "name,email",
               }
           ),
       }

       @get()
       async def list_users(
           self,
           users_service: UserService,
           filters: SkipValidation[NamedDependency[list[FilterTypes]]],
       ) -> OffsetPagination[User]:
           return await users_service.list_with_count(*filters)

``sort_field`` remains the SQL-facing allowlist. Clients can request
``?orderBy=uploadedCollections`` by default while the service receives an
``OrderByFilter`` for ``uploaded_collections``. Existing snake_case values, such
as ``?orderBy=uploaded_collections``, remain valid for compatibility. Set
``sort_field_camelize=False`` when an endpoint must accept only raw configured
``orderBy`` values.

.. seealso::

   - :doc:`/usage/drivers_and_querying` for the driver methods used here
   - :doc:`/usage/filtering` for filter types and Litestar filter dependencies
