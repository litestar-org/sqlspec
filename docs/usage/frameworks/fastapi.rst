=======
FastAPI
=======

SQLSpec provides a FastAPI extension that wires database sessions into the request lifecycle
using standard dependency injection. The extension manages connection pools and ensures proper
cleanup between requests.

Installation
============

Install SQLSpec with the FastAPI extra:

.. tab-set::

   .. tab-item:: uv

      .. code-block:: bash

         uv add "sqlspec[fastapi]"

   .. tab-item:: pip

      .. code-block:: bash

         pip install "sqlspec[fastapi]"

   .. tab-item:: Poetry

      .. code-block:: bash

         poetry add "sqlspec[fastapi]"

   .. tab-item:: PDM

      .. code-block:: bash

         pdm add "sqlspec[fastapi]"

Basic Setup
===========

Create a SQLSpec instance, register your database config, and attach the plugin to your
FastAPI app. The plugin provides ``provide_session`` dependencies that yield a session
for each request. Configure FastAPI-specific options under ``extension_config["fastapi"]``.

.. literalinclude:: /examples/frameworks/fastapi/basic_setup.py
   :language: python
   :caption: ``fastapi basic setup``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Transaction Modes
=================

FastAPI uses Starlette-compatible transaction middleware configured via ``extension_config["fastapi"]``:

``manual`` (default)
   SQLSpec manages the connection lifecycle and closes connections at the end of the
   request, leaving transaction commit and rollback decisions to your handler logic.

``autocommit``
   SQLSpec automatically commits transactions for 2xx responses and rolls back on 4xx/5xx
   responses or unhandled exceptions.

``autocommit_include_redirect``
   Extends autocommit to also commit on redirect responses (2xx and 3xx).

.. code-block:: python

   from sqlspec.adapters.aiosqlite import AiosqliteConfig

   config = AiosqliteConfig(
       connection_config={"database": "app.db"},
       extension_config={
           "fastapi": {
               "commit_mode": "autocommit",
               "extra_rollback_statuses": {409},
               "session_key": "db",
           }
       },
   )

Multiple Databases
==================

Configure each database with unique ``session_key``, ``connection_key``, and ``pool_key``
values under ``extension_config["fastapi"]``. Inject distinct sessions by passing the
key to ``provide_session()``:

.. literalinclude:: /examples/frameworks/fastapi/multi_database.py
   :language: python
   :caption: ``fastapi multi database``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Dependency Injection Helpers
============================

The ``SQLSpecPlugin`` provides several dependency factories for FastAPI routes:

- ``db_ext.provide_session(key=None)``: Injects a driver session (sync or async depending on config).
- ``db_ext.provide_async_session(key=None)``: Type-narrowed dependency returning ``AsyncDriverAdapterBase``.
- ``db_ext.provide_sync_session(key=None)``: Type-narrowed dependency returning ``SyncDriverAdapterBase``.
- ``db_ext.provide_connection(key=None)``: Injects the raw database connection.

Filter Dependencies
===================

SQLSpec provides ``provide_filters`` to parse HTTP query parameters into typed
``StatementFilter`` objects (pagination, sorting, search, before/after):

.. code-block:: python

   from typing import Annotated
   from fastapi import Depends, FastAPI
   from sqlspec.core.filters import FilterTypes
   from sqlspec.extensions.fastapi import provide_filters

   app = FastAPI()

   filter_dep = provide_filters(
       {
           "pagination_type": "limit_offset",
           "pagination_size": 20,
           "sort_field": ["name", "created_at"],
           "search": "name,email",
       }
   )

   @app.get("/items")
   async def list_items(
       filters: Annotated[list[FilterTypes], Depends(filter_dep)],
   ) -> dict[str, str]:
       return {"status": "ok"}

Related Guides
==============

- :doc:`/usage/configuration` for detailed config options.
- :doc:`/usage/filtering` for available filter types.
- :doc:`/reference/adapters` for adapter-specific settings.
- :doc:`starlette` for underlying ASGI middleware details.
