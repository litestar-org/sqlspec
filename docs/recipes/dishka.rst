===========================
Dishka Dependency Injection
===========================

.. note::

   This recipe requires `dishka <https://github.com/reagento/dishka>`_.

Dishka is an inversion-of-control container used to manage scoped dependencies in Python
applications. This recipe demonstrates how to register SQLSpec configs and drivers into
Dishka's scope system.

Setup
=====

The core pattern: database configs live at ``APP`` scope (created once for the application),
while drivers are ``REQUEST``-scoped (one per HTTP request or task execution).

.. code-block:: python

   from collections.abc import AsyncIterator
   from dishka import Provider, Scope, provide

   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver


   class DatabaseProvider(Provider):
       @provide(scope=Scope.APP)
       def provide_sqlspec(self) -> tuple[SQLSpec, AsyncpgConfig]:
           sqlspec = SQLSpec()
           config = sqlspec.add_config(
               AsyncpgConfig(
                   connection_config={"dsn": "postgresql://localhost/mydb"},
                   extension_config={"fastapi": {"disable_di": True}},
               )
           )
           return sqlspec, config

       @provide(scope=Scope.REQUEST)
       async def provide_driver(
           self, app_deps: tuple[SQLSpec, AsyncpgConfig]
       ) -> AsyncIterator[AsyncpgDriver]:
           sqlspec, config = app_deps
           async with sqlspec.provide_session(config) as driver:
               yield driver

Container Factories
===================

Different application entry points (web server, CLI, background worker) share the same
providers:

.. code-block:: python

   from dishka import make_async_container
   from sqlspec.adapters.asyncpg import AsyncpgDriver

   container = make_async_container(DatabaseProvider())

   # Inside a request or task scope
   async with container() as request_container:
       driver = await request_container.get(AsyncpgDriver)
       result = await driver.select("SELECT 1 as status")

Service Integration
===================

Inject drivers into domain services via Dishka's ``FromDishka`` marker:

.. code-block:: python

   from typing import Any
   from dishka import FromDishka
   from sqlspec.adapters.asyncpg import AsyncpgDriver


   class UserService:
       def __init__(self, driver: FromDishka[AsyncpgDriver]) -> None:
           self.driver = driver

       async def get_user(self, user_id: str) -> dict[str, Any] | None:
           return await self.driver.select_one_or_none(
               "SELECT * FROM users WHERE id = :id",
               {"id": user_id},
           )

.. seealso::

   - `Dishka documentation <https://dishka.readthedocs.io/>`_
   - :doc:`/usage/framework_integrations` for built-in framework integrations
