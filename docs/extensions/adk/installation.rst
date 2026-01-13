============
Installation
============

Install SQLSpec with a database adapter and the Google ADK SDK.

.. tab-set::

   .. tab-item:: PostgreSQL

      .. code-block:: bash

         pip install "sqlspec[asyncpg,adk]"

   .. tab-item:: SQLite

      .. code-block:: bash

         pip install "sqlspec[aiosqlite,adk]"

   .. tab-item:: MySQL

      .. code-block:: bash

         pip install "sqlspec[asyncmy,adk]"

   .. tab-item:: DuckDB

      .. code-block:: bash

         pip install "sqlspec[duckdb,adk]"

What This Provides
------------------

The ``adk`` extra includes the Google ADK SDK (``google-genai``). SQLSpec provides:

- **Session Store** - Persist ADK agent sessions to your database.
- **Memory Store** - Store agent memory for context across conversations.
- **Event Store** - Log agent events for observability.

Next Steps
----------

Proceed to :doc:`quickstart` to set up stores for your ADK agent.
