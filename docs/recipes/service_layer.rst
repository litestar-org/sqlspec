=====================
Service Layer Pattern
=====================

A common pattern in production sqlspec applications is a base service class that wraps
a driver and provides convenience methods for pagination, error handling, and transactions.
Parameters and filters passed as ``*parameters`` are forwarded directly to the driver,
which applies filters to the statement and binds parameters automatically.

Base Service
============

.. tab-set::

   .. tab-item:: Async

      .. code-block:: python

         from __future__ import annotations

         from contextlib import asynccontextmanager
         from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast, overload

         from sqlspec import AsyncDriverAdapterBase
         from sqlspec.core.filters import (
             FilterTypes,
             LimitOffsetFilter,
             OffsetPagination,
             StatementFilter,
         )
         from sqlspec.typing import SchemaT, StatementParameters

         AsyncDriverT = TypeVar("AsyncDriverT", bound=AsyncDriverAdapterBase)

         if TYPE_CHECKING:
             from collections.abc import AsyncIterator

             from sqlspec import QueryBuilder, Statement, StatementConfig


         class SQLSpecAsyncService(Generic[AsyncDriverT]):
             """Base async service with pagination, get-or-404, and transactions."""

             def __init__(self, driver: AsyncDriverT) -> None:
                 self.driver = driver

             @overload
             async def paginate(
                 self,
                 statement: Statement | QueryBuilder,
                 /,
                 *parameters: StatementParameters | StatementFilter,
                 schema_type: type[SchemaT],
                 statement_config: StatementConfig | None = None,
                 **kwargs: Any,
             ) -> OffsetPagination[SchemaT]: ...

             @overload
             async def paginate(
                 self,
                 statement: Statement | QueryBuilder,
                 /,
                 *parameters: StatementParameters | StatementFilter,
                 schema_type: None = None,
                 statement_config: StatementConfig | None = None,
                 **kwargs: Any,
             ) -> OffsetPagination[dict[str, Any]]: ...

             async def paginate(
                 self,
                 statement: Statement | QueryBuilder,
                 /,
                 *parameters: StatementParameters | StatementFilter,
                 schema_type: type[SchemaT] | None = None,
                 statement_config: StatementConfig | None = None,
                 **kwargs: Any,
             ) -> OffsetPagination[SchemaT] | OffsetPagination[dict[str, Any]]:
                 """Execute a paginated query using filters from ``*parameters``."""
                 results, total = await self.driver.select_with_total(
                     statement, *parameters, schema_type=schema_type,
                     statement_config=statement_config, **kwargs,
                 )
                 limit_offset = self.driver.find_filter(LimitOffsetFilter, parameters)
                 return OffsetPagination(
                     items=cast("list[Any]", results),
                     limit=limit_offset.limit if limit_offset else 10,
                     offset=limit_offset.offset if limit_offset else 0,
                     total=total,
                 )

             async def get_or_404(
                 self,
                 statement: Statement | QueryBuilder,
                 /,
                 *parameters: StatementParameters,
                 schema_type: type[SchemaT] | None = None,
                 error_message: str | None = None,
                 statement_config: StatementConfig | None = None,
                 **kwargs: Any,
             ) -> SchemaT | dict[str, Any]:
                 """Fetch one row or raise an application-level not-found error."""
                 result = await self.driver.select_one_or_none(
                     statement, *parameters, schema_type=schema_type,
                     statement_config=statement_config, **kwargs,
                 )
                 if result is None:
                     raise ValueError(error_message or "Record not found")
                 return result

             async def exists(
                 self,
                 statement: Statement | QueryBuilder,
                 /,
                 *parameters: StatementParameters,
                 statement_config: StatementConfig | None = None,
                 **kwargs: Any,
             ) -> bool:
                 """Return True if the query matches at least one row."""
                 result = await self.driver.select_one_or_none(
                     statement, *parameters, statement_config=statement_config, **kwargs,
                 )
                 return result is not None

             @asynccontextmanager
             async def begin_transaction(self) -> AsyncIterator[None]:
                 """Context manager that commits on success, rolls back on error."""
                 await self.driver.begin()
                 try:
                     yield
                 except Exception:
                     await self.driver.rollback()
                     raise
                 else:
                     await self.driver.commit()

   .. tab-item:: Sync

      .. code-block:: python

         from __future__ import annotations

         from contextlib import contextmanager
         from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast, overload

         from sqlspec import SyncDriverAdapterBase
         from sqlspec.core.filters import (
             FilterTypes,
             LimitOffsetFilter,
             OffsetPagination,
             StatementFilter,
         )
         from sqlspec.typing import SchemaT, StatementParameters

         SyncDriverT = TypeVar("SyncDriverT", bound=SyncDriverAdapterBase)

         if TYPE_CHECKING:
             from collections.abc import Iterator

             from sqlspec import QueryBuilder, Statement, StatementConfig


         class SQLSpecSyncService(Generic[SyncDriverT]):
             """Base sync service with pagination, get-or-404, and transactions."""

             def __init__(self, driver: SyncDriverT) -> None:
                 self.driver = driver

             @overload
             def paginate(
                 self,
                 statement: Statement | QueryBuilder,
                 /,
                 *parameters: StatementParameters | StatementFilter,
                 schema_type: type[SchemaT],
                 statement_config: StatementConfig | None = None,
                 **kwargs: Any,
             ) -> OffsetPagination[SchemaT]: ...

             @overload
             def paginate(
                 self,
                 statement: Statement | QueryBuilder,
                 /,
                 *parameters: StatementParameters | StatementFilter,
                 schema_type: None = None,
                 statement_config: StatementConfig | None = None,
                 **kwargs: Any,
             ) -> OffsetPagination[dict[str, Any]]: ...

             def paginate(
                 self,
                 statement: Statement | QueryBuilder,
                 /,
                 *parameters: StatementParameters | StatementFilter,
                 schema_type: type[SchemaT] | None = None,
                 statement_config: StatementConfig | None = None,
                 **kwargs: Any,
             ) -> OffsetPagination[SchemaT] | OffsetPagination[dict[str, Any]]:
                 """Execute a paginated query using filters from ``*parameters``."""
                 results, total = self.driver.select_with_total(
                     statement, *parameters, schema_type=schema_type,
                     statement_config=statement_config, **kwargs,
                 )
                 limit_offset = self.driver.find_filter(LimitOffsetFilter, parameters)
                 return OffsetPagination(
                     items=cast("list[Any]", results),
                     limit=limit_offset.limit if limit_offset else 10,
                     offset=limit_offset.offset if limit_offset else 0,
                     total=total,
                 )

             def get_or_404(
                 self,
                 statement: Statement | QueryBuilder,
                 /,
                 *parameters: StatementParameters,
                 schema_type: type[SchemaT] | None = None,
                 error_message: str | None = None,
                 statement_config: StatementConfig | None = None,
                 **kwargs: Any,
             ) -> SchemaT | dict[str, Any]:
                 """Fetch one row or raise an application-level not-found error."""
                 result = self.driver.select_one_or_none(
                     statement, *parameters, schema_type=schema_type,
                     statement_config=statement_config, **kwargs,
                 )
                 if result is None:
                     raise ValueError(error_message or "Record not found")
                 return result

             def exists(
                 self,
                 statement: Statement | QueryBuilder,
                 /,
                 *parameters: StatementParameters,
                 statement_config: StatementConfig | None = None,
                 **kwargs: Any,
             ) -> bool:
                 """Return True if the query matches at least one row."""
                 result = self.driver.select_one_or_none(
                     statement, *parameters, statement_config=statement_config, **kwargs,
                 )
                 return result is not None

             @contextmanager
             def begin_transaction(self) -> Iterator[None]:
                 """Context manager that commits on success, rolls back on error."""
                 self.driver.begin()
                 try:
                     yield
                 except Exception:
                     self.driver.rollback()
                     raise
                 else:
                     self.driver.commit()

Domain Services
===============

Inherit from the base and use the ``sql`` builder or SQL file loader for queries.
Filters from Litestar dependencies flow straight through ``*filters`` to the driver:

.. tab-set::

   .. tab-item:: Async

      .. code-block:: python

         from __future__ import annotations

         from typing import TYPE_CHECKING

         from pydantic import BaseModel
         from sqlspec import sql
         from sqlspec.core.filters import OffsetPagination, StatementFilter

         if TYPE_CHECKING:
             from uuid import UUID


         class User(BaseModel):
             id: str
             email: str
             name: str


         class UserService(SQLSpecAsyncService[AsyncDriverT]):

             async def list_with_count(self, *filters: StatementFilter) -> OffsetPagination[User]:
                 return await self.paginate(
                     sql.select("id", "email", "name").from_("users"),
                     *filters,
                     schema_type=User,
                 )

             async def get_user(self, user_id: UUID) -> User:
                 return await self.get_or_404(
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

         from __future__ import annotations

         from typing import TYPE_CHECKING

         from pydantic import BaseModel
         from sqlspec import sql
         from sqlspec.core.filters import OffsetPagination, StatementFilter

         if TYPE_CHECKING:
             from uuid import UUID


         class User(BaseModel):
             id: str
             email: str
             name: str


         class UserService(SQLSpecSyncService[SyncDriverT]):

             def list_with_count(self, *filters: StatementFilter) -> OffsetPagination[User]:
                 return self.paginate(
                     sql.select("id", "email", "name").from_("users"),
                     *filters,
                     schema_type=User,
                 )

             def get_user(self, user_id: UUID) -> User:
                 return self.get_or_404(
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
forwarded through the service to the driver:

.. code-block:: python

   from typing import Annotated

   from litestar import Controller, get
   from litestar.params import Dependency
   from sqlspec.core.filters import FilterTypes, OffsetPagination
   from sqlspec.extensions.litestar.providers import create_filter_dependencies


   class UserController(Controller):
       path = "/api/users"
       dependencies = create_filter_dependencies({
           "pagination_type": "limit_offset",
           "pagination_size": 20,
           "sort_field": "created_at",
           "sort_order": "desc",
           "search": "name,email",
       })

       @get()
       async def list_users(
           self,
           users_service: UserService,
           filters: Annotated[list[FilterTypes], Dependency(skip_validation=True)],
       ) -> OffsetPagination[User]:
           return await users_service.list_with_count(*filters)

.. seealso::

   - :doc:`/usage/drivers_and_querying` for the driver methods used here
   - :doc:`/usage/filtering` for filter types and Litestar filter dependencies
