=====================
Service Layer Pattern
=====================

Build a service from a database config to fetch rows and run transactions.
Each query helper opens a short session and releases it before it returns,
even when the query raises.

The service can then outlive a single session. You can still pass a session
as the first argument when the caller should own its lifetime.

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

Pass exactly one of ``Service(config=cfg)`` or ``Service(session)``. An optional
``loader=SQLFileLoader()`` is available through ``service.loader``; the service
does not resolve named SQL for you.

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Member
     - Purpose
   * - ``paginate(statement, *parameters, schema_type=None, count_with_window=False, session=None)``
     - Runs the query and returns an ``OffsetPagination`` with a total count.
   * - ``get_one(statement, *parameters, schema_type=None, error_message=None, session=None)``
     - Returns a row or raises ``NotFoundError`` when no row matches.
   * - ``exists(statement, *parameters, session=None)``
     - Returns ``True`` when the query matches at least one row.
   * - ``begin_transaction()``
     - Holds one session across helpers, commits on success, and rolls back on error.
   * - ``provide_session(session=None)``
     - Yields a session for explicit reuse. Acquired sessions are released on exit.
   * - ``begin(session=None)`` / ``commit(session=None)`` / ``rollback(session=None)``
     - Control an available session; they never acquire a disposable session.
   * - ``session`` / ``driver``
     - The caller's session or the active transaction's driver.

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

Use a Config for Short Sessions
==============================

The examples below use a local SQLite file. Install ``sqlspec`` for the sync
example or ``sqlspec[aiosqlite]`` for the async example. ``tmp_path`` is a pytest
temporary directory; use your application's database path outside a test.

Bare helpers each acquire their own session. To share one session without
starting a transaction, use ``provide_session()`` and pass the yielded driver
as ``session=``. Merely entering ``provide_session()`` does not bind later helpers
to that session.

.. tab-set::

   .. tab-item:: Sync

      .. literalinclude:: /examples/drivers/service_config.py
         :language: python
         :dedent: 4
         :start-after: # start-sync-example
         :end-before: # end-sync-example
         :no-upgrade:

   .. tab-item:: Async

      .. literalinclude:: /examples/drivers/service_config.py
         :language: python
         :dedent: 4
         :start-after: # start-async-example
         :end-before: # end-async-example
         :no-upgrade:

Both examples leave Ada and Grace in the table. The last transaction rolls back
its insert when the block raises. Query helpers do not add commits between calls;
``begin_transaction()`` commits once when its block succeeds.

Session Ownership
=================

A passed ``session=`` wins over the active transaction. The service borrows it;
the caller decides when to commit and release it.
Without an override, helpers use the active transaction, then the constructor
session, or acquire a new session from the config.

Outside a config service's transaction, ``session`` and ``driver`` raise
``ImproperConfigurationError``. Manual ``begin()``, ``commit()``, and
``rollback()`` also require an available session. Use ``begin_transaction()``
or pass a driver explicitly with ``session=`` for manual control.

Tasks and threads can use the same service, within the adapter's concurrency
limits. A child task of a config service cannot implicitly reuse its parent's
transaction, even after that transaction exits. Start independent work outside
the parent's transaction context, or pass a session whose use you control.

Nested Transaction Blocks
=========================

A ``begin_transaction()`` block entered inside another block on the same service
runs in a savepoint on the outer session; no second session is acquired. When the
inner block raises, only its work is rolled back and the exception propagates, so
the outer block can catch it and keep using the session. This lets an insert that
may hit a unique constraint run without aborting the surrounding transaction:

.. code-block:: python

    from sqlspec.exceptions import UniqueViolationError

    async with service.begin_transaction() as session:
        try:
            async with service.begin_transaction():
                await session.execute("INSERT INTO users (email) VALUES ($1)", email)
        except UniqueViolationError:
            pass
        user = await service.get_one("SELECT id, email FROM users WHERE email = $1", email)

The outer block commits everything that was not rolled back; if the outer block
raises, work from inner blocks that succeeded is rolled back with it. Nesting works
the same way for services built from a session, including a block entered inside
the session's own ``transaction()`` block. When a service built from a session enters
its outermost block while that session already has an open transaction, for example
after an earlier statement, the block joins that transaction instead of calling
``begin()``, and exiting the block commits or rolls back all of it, including the
earlier work. If the outer commit fails, the block
attempts a rollback before raising the commit error. Adapters without savepoint
support raise ``ImproperConfigurationError`` when a nested block is entered.

A nested block must run in the task or thread that entered the outer block. Entering
``begin_transaction()`` from another task or thread while the outer block is active
raises ``ImproperConfigurationError`` instead of starting a second transaction on the
same session. The outer block itself may be entered and exited in different tasks or
threads, as happens with framework dependencies that run in a thread pool.

Services expose ``config``, the configuration they were built from, so a service
can construct a collaborating service without reaching into private state:

.. code-block:: python

    audit = AuditService(config=users.config)

``audit`` is a separate service with its own sessions and transactions. It does not
join a transaction that ``users`` has open; pass ``session=`` to run its helpers on
that session instead.

Domain Services with a Caller-Owned Session
==========================================

Inherit from the base and use the ``sql`` builder or SQL file loader for queries.
Filters from Litestar dependencies flow straight through ``*filters`` to the driver.
These examples receive a caller-owned session, so direct ``self.driver`` calls
remain available. For a config-built service, perform direct driver operations
inside ``begin_transaction()`` or through an explicitly provided session.

.. tab-set::

   .. tab-item:: Async

      .. code-block:: python

         from uuid import UUID

         from pydantic import BaseModel
         from sqlspec import sql
         from sqlspec.adapters.asyncpg import AsyncpgDriver
         from sqlspec.core.filters import OffsetPagination, StatementFilter
         from sqlspec.service import SQLSpecAsyncService


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

             async def get_user(self, user_id: UUID) -> User:
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

         from uuid import UUID

         from pydantic import BaseModel
         from sqlspec import sql
         from sqlspec.adapters.sqlite import SqliteDriver
         from sqlspec.core.filters import OffsetPagination, StatementFilter
         from sqlspec.service import SQLSpecSyncService


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

             def get_user(self, user_id: UUID) -> User:
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
