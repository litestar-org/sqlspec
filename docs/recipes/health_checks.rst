=============
Health Checks
=============

Production deployments in Kubernetes, Docker Compose, or Cloud Run require health check
probes to determine whether an application container is live (running) and ready (able
to serve database queries).

This recipe demonstrates how to implement efficient liveness and readiness checks with SQLSpec.

.. contents:: On this page
   :local:
   :depth: 2

Liveness vs. Readiness
======================

- **Liveness Probe**: Verifies that the application process is not deadlocked or frozen.
  *Do not* ping external databases in a liveness probe; if the database experiences a transient outage,
  failing liveness probes causes Kubernetes to restart the container in a crash loop.
- **Readiness Probe**: Verifies that the application can reach the database and execute queries.
  If the readiness check fails, the orchestrator stops routing incoming HTTP traffic to the container
  until connectivity is restored.

Basic Ping Query
================

The simplest readiness check executes a lightweight query such as ``SELECT 1`` using a driver session:

.. code-block:: python

   import asyncio
   from sqlspec.adapters.asyncpg import AsyncpgDriver


   async def check_database_ready(driver: AsyncpgDriver, timeout: float = 2.0) -> bool:
       try:
           async with asyncio.timeout(timeout):
               result = await driver.select_value("SELECT 1")
               return result == 1
       except Exception:
           return False

Multi-Database Readiness Probe
==============================

When an application connects to multiple databases or read replicas, verify each config:

.. code-block:: python

   import asyncio
   from sqlspec import SQLSpec


   async def check_all_databases(sqlspec: SQLSpec, timeout: float = 2.0) -> dict[str, str]:
       status: dict[str, str] = {}
       for key, config in sqlspec.configs.items():
           try:
               async with asyncio.timeout(timeout):
                   async with sqlspec.provide_session(config) as session:
                       val = await session.select_value("SELECT 1")
                       status[key] = "healthy" if val == 1 else "unhealthy"
           except Exception as exc:
               status[key] = f"error: {exc}"
       return status

FastAPI Readiness Endpoint
==========================

.. code-block:: python

   from typing import Annotated
   from fastapi import Depends, FastAPI, HTTPException, status
   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
   from sqlspec.extensions.fastapi import SQLSpecPlugin

   app = FastAPI()
   sqlspec = SQLSpec()
   config = sqlspec.add_config(
       AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/mydb"})
   )
   plugin = SQLSpecPlugin(sqlspec)
   plugin.init_app(app)


   @app.get("/health/live")
   async def liveness() -> dict[str, str]:
       return {"status": "ok"}


   @app.get("/health/ready")
   async def readiness(
       db: Annotated[AsyncpgDriver, Depends(plugin.provide_session())],
   ) -> dict[str, str]:
       try:
           val = await db.select_value("SELECT 1")
           if val != 1:
               raise HTTPException(
                   status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                   detail="Database ping failed",
               )
           return {"status": "ready"}
       except Exception as exc:
           raise HTTPException(
               status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
               detail=f"Database unreachable: {exc}",
           ) from exc

Litestar Readiness Route
========================

In Litestar, use route handlers with dependency injection:

.. code-block:: python

   from litestar import Litestar, get, status_codes
   from litestar.exceptions import ServiceUnavailableException
   from litestar.params import NamedDependency
   from sqlspec.adapters.aiosqlite import AiosqliteDriver, AiosqliteConfig
   from sqlspec.extensions.litestar import SQLSpecPlugin

   config = AiosqliteConfig(connection_config={"database": "app.db"})
   plugin = SQLSpecPlugin(config=config)


   @get("/health/ready")
   async def health_ready(
       db_session: NamedDependency[AiosqliteDriver, "db_session"],
   ) -> dict[str, str]:
       try:
           val = await db_session.select_value("SELECT 1")
           if val != 1:
               raise ServiceUnavailableException(detail="Ping returned invalid value")
           return {"status": "ready"}
       except Exception as exc:
           raise ServiceUnavailableException(detail=f"Database unreachable: {exc}") from exc


   app = Litestar(route_handlers=[health_ready], plugins=[plugin])

Pool Teardown and Graceful Shutdown
===================================

When an application container is stopped (e.g. ``SIGTERM`` during rolling deployments),
database connections must be closed gracefully to avoid orphan connections on the server:

- In **Litestar**, ``SQLSpecPlugin`` automatically registers shutdown lifespan hooks that drain and close all connection pools.
- In **Starlette / FastAPI**, ``SQLSpecPlugin`` attaches to the ASGI lifespan context to manage pool startup and teardown.
- In standalone scripts or background workers, call ``await sqlspec.close()`` on shutdown:

.. code-block:: python

   import asyncio
   from sqlspec import SQLSpec

   sqlspec = SQLSpec()
   # ... register configs and run application ...


   async def shutdown() -> None:
       # Gracefully drains connection pools and closes drivers
       await sqlspec.close()

.. seealso::

   - :doc:`/usage/framework_integrations` for framework lifecycle integration details.
   - :doc:`/usage/drivers_and_querying` for query execution methods.
