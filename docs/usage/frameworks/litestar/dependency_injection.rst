======================
Dependency Injection
======================

The SQLSpec plugin integrates with Litestar's dependency injection system. By default,
it provides a session under the key ``db``. You can customize this key or register
multiple databases with distinct keys.

Default Injection
-----------------

When you add ``SQLSpecPlugin`` to your app, handlers can request ``db`` to receive
a session scoped to the request:

.. code-block:: python

   @get("/users")
   async def list_users(db: AsyncSession) -> list[User]:
       result = await db.execute("SELECT * FROM users")
       return result.all(schema_type=User)

Custom Keys
-----------

Use ``session_dependency_key`` and ``config_dependency_key`` to customize injection keys.
This is useful when connecting to multiple databases.

.. literalinclude:: /examples/extensions/litestar/dependency_keys.py
   :language: python
   :caption: ``dependency keys``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Multiple Databases
------------------

Register separate plugins for each database, each with its own key:

.. code-block:: python

   primary_plugin = SQLSpecPlugin(
       sqlspec,
       config_name="primary",
       session_dependency_key="primary_db",
   )
   analytics_plugin = SQLSpecPlugin(
       sqlspec,
       config_name="analytics",
       session_dependency_key="analytics_db",
   )

   @get("/report")
   async def report(primary_db: AsyncSession, analytics_db: AsyncSession) -> dict:
       # Use both databases in one handler
       ...
