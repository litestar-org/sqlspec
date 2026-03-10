===========================
Dishka Dependency Injection
===========================

.. note::

   This recipe requires `dishka <https://github.com/reagento/dishka>`_.

Dishka is a dependency injection framework used in several sqlspec production projects.
This recipe shows how to wire sqlspec configs and drivers into Dishka's scope system.

Setup
=====

The core pattern: database configs live at ``APP`` scope (created once), while drivers
are ``REQUEST``-scoped (one per HTTP request or CLI invocation).

.. code-block:: python

   from dishka import Provider, Scope, provide
   from collections.abc import AsyncIterator

   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig


   db_manager = SQLSpec()
   db = db_manager.add_config(
       AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/mydb"})
   )


   class DatabaseProvider(Provider):
       @provide(scope=Scope.REQUEST)
       async def provide_driver(self) -> AsyncIterator:
           async with db_manager.provide_session(db) as driver:
               yield driver

Container Factories
===================

Different entry points (web server, CLI, worker) share the same providers but
create separate containers:

.. code-block:: python

   from dishka import make_async_container

   # Litestar app
   container = make_async_container(DatabaseProvider())

   # CLI tool
   async with make_async_container(DatabaseProvider()) as container:
       driver = await container.get(AsyncDriverAdapterBase)

Service Integration
===================

Inject drivers into service classes via Dishka:

.. code-block:: python

   from dishka import FromDishka

   class UserService:
       def __init__(self, driver: FromDishka[AsyncDriverAdapterBase]) -> None:
           self.driver = driver

       async def get_user(self, user_id: str):
           return await self.driver.select_one(
               "SELECT * FROM users WHERE id = :id",
               {"id": user_id},
           )

.. seealso::

   - `Dishka documentation <https://dishka.readthedocs.io/>`_
   - :doc:`/usage/framework_integrations` for built-in framework support
