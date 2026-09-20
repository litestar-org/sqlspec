=============
Multi-Tenancy
=============

Multi-tenancy architectures allow a single application to serve multiple distinct clients (tenants)
while guaranteeing data isolation. SQLSpec supports three common multi-tenancy patterns:

1. **Discriminator Column (Shared Schema)**: All tenants share the same tables, isolated by a tenant ID column.
2. **Schema-Per-Tenant**: Each tenant has their own schema inside a shared database instance.
3. **Database-Per-Tenant**: Each tenant has a completely separate database connection or configuration.

.. contents:: On this page
   :local:
   :depth: 2

Pattern 1: Discriminator Column (Shared Schema)
==============================================

In this pattern, every tenant row includes a ``tenant_id`` column. SQLSpec allows you to enforce
tenant isolation consistently using query builder helpers, named SQL parameter binding, or custom filters.

Using the Query Builder
-----------------------

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgDriver
   from sqlspec.builder import sql


   async def get_tenant_orders(driver: AsyncpgDriver, tenant_id: str, status: str) -> list[dict]:
       query = (
           sql.select("id", "total", "status")
           .from_("orders")
           .where(tenant_id=tenant_id, status=status)
       )
       return (await driver.select(query)).as_dicts()

Using Tenant Statement Filters
------------------------------

For dynamic query filtering, you can write a tenant filter or combine standard filters:

.. code-block:: python

   from dataclasses import dataclass
   from sqlspec.adapters.asyncpg import AsyncpgDriver
   from sqlspec.core import StatementFilter


   @dataclass(frozen=True)
   class TenantFilter(StatementFilter):
       tenant_id: str

       def apply(self, statement: str) -> tuple[str, dict[str, str]]:
           # Appends tenant isolation predicate to outgoing SQL
           return f"SELECT * FROM ({statement}) AS _tenant_subq WHERE tenant_id = :tenant_id", {
               "tenant_id": self.tenant_id
           }

Pattern 2: Schema-Per-Tenant
============================

In PostgreSQL, each tenant can have an isolated schema (e.g. ``tenant_123``). You can dynamically set
the schema when acquiring a session or setting the PostgreSQL ``search_path``:

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver


   async def get_tenant_session(
       config: AsyncpgConfig, tenant_slug: str
   ) -> AsyncpgDriver:
       session = config.create_driver()
       # Set search_path to tenant schema with public fallback
       await session.execute(f"SET search_path TO {tenant_slug}, public")
       return session

When using transaction blocks:

.. code-block:: python

   async with session.transaction():
       await session.execute("SET LOCAL search_path TO tenant_a")
       results = await session.select("SELECT * FROM users")

Pattern 3: Database-Per-Tenant (Dynamic Routing)
================================================

In high-compliance or isolated deployments, each tenant has a distinct database instance or DSN.
You can register multiple database configs with SQLSpec or register them dynamically at runtime:

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig

   sqlspec = SQLSpec()

   # Register known tenants at startup
   sqlspec.add_config(
       AsyncpgConfig(
           connection_config={"dsn": "postgresql://localhost/tenant_alpha"},
           extension_config={"fastapi": {"session_key": "tenant_alpha"}},
       )
   )
   sqlspec.add_config(
       AsyncpgConfig(
           connection_config={"dsn": "postgresql://localhost/tenant_beta"},
           extension_config={"fastapi": {"session_key": "tenant_beta"}},
       )
   )


   # Acquire session by tenant key
   async def fetch_tenant_data(tenant_key: str):
       config = sqlspec.get_config(tenant_key)
       async with sqlspec.provide_session(config) as session:
           return await session.select("SELECT * FROM settings")

Dynamically Registering Configurations
--------------------------------------

If tenant databases are provisioned at runtime, dynamically add configs to the shared ``SQLSpec`` registry:

.. code-block:: python

   def ensure_tenant_config(sqlspec: SQLSpec, tenant_id: str, dsn: str) -> AsyncpgConfig:
       key = f"tenant_{tenant_id}"
       try:
           return sqlspec.get_config(key)  # type: ignore[return-value]
       except KeyError:
           new_config = AsyncpgConfig(
               connection_config={"dsn": dsn},
           )
           sqlspec.add_config(new_config)
           return new_config

.. seealso::

   - :doc:`/usage/configuration` for multi-database configuration options.
   - :doc:`/usage/filtering` for statement filter composition.
   - :doc:`/usage/framework_integrations` for wiring tenant sessions into web frameworks.
