=========
Starlette
=========

SQLSpec provides a Starlette extension that hooks database sessions into the ASGI
lifecycle. The extension uses Starlette's lifespan context to manage connection pools
and provides middleware for request-scoped sessions and transactions.

Installation
============

Install SQLSpec with the Starlette extra:

.. tab-set::

   .. tab-item:: uv

      .. code-block:: bash

         uv add "sqlspec[starlette]"

   .. tab-item:: pip

      .. code-block:: bash

         pip install "sqlspec[starlette]"

   .. tab-item:: Poetry

      .. code-block:: bash

         poetry add "sqlspec[starlette]"

   .. tab-item:: PDM

      .. code-block:: bash

         pdm add "sqlspec[starlette]"

Basic Setup
===========

Create a SQLSpec instance, register your database config, and attach ``SQLSpecPlugin``
to your Starlette app. The plugin adds lifespan handlers to manage pools and middleware
for request sessions.

.. literalinclude:: /examples/frameworks/starlette/basic_setup.py
   :language: python
   :caption: ``starlette basic setup``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Transaction Modes
=================

Configure the commit mode under ``extension_config["starlette"]``:

``manual`` (default)
   SQLSpec manages the connection lifecycle and closes connections before completing
   the request, leaving commits and rollbacks to your route handlers.

``autocommit``
   SQLSpec commits transactions for successful 2xx responses and rolls back on 4xx/5xx
   responses or unhandled exceptions.

``autocommit_include_redirect``
   Extends autocommit to commit transactions on redirect responses (2xx and 3xx).

.. code-block:: python

   from sqlspec.adapters.aiosqlite import AiosqliteConfig

   config = AiosqliteConfig(
       connection_config={"database": "app.db"},
       extension_config={
           "starlette": {
               "commit_mode": "autocommit",
               "extra_rollback_statuses": {409},
               "session_key": "db",
           }
       },
   )

Multiple Databases
==================

For multiple databases, configure each config with unique ``session_key``,
``connection_key``, and ``pool_key`` settings. Retrieve sessions by key:

.. literalinclude:: /examples/frameworks/starlette/multi_database.py
   :language: python
   :caption: ``starlette multi database``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Request and App Access
======================

The plugin provides methods to access sessions, connections, and pools:

- ``db_plugin.get_session(request, key=None)``: Returns the request-scoped driver session.
- ``db_plugin.get_connection(request, key=None)``: Returns the underlying database connection.
- ``db_plugin.get_pool(app, key=None)``: Returns the application connection pool from ``app.state``.

disable_di
==========

Set ``disable_di=True`` when an external dependency injection framework (such as Dishka)
manages request-scoped connections. SQLSpec still initializes and cleans up pools via
Starlette lifespan, but omits request-scoped session middleware.

Related Guides
==============

- :doc:`/usage/configuration` for detailed config options.
- :doc:`/reference/adapters` for adapter-specific settings.
- :doc:`fastapi` extends this Starlette foundation with FastAPI dependency injection.
