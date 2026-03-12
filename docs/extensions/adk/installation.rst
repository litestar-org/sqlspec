============
Installation
============

Install SQLSpec with a database adapter and the Google ADK SDK.

.. tab-set::

   .. tab-item:: PostgreSQL

      .. tab-set::

         .. tab-item:: uv

            .. code-block:: bash

               uv add "sqlspec[asyncpg,adk]"

         .. tab-item:: pip

            .. code-block:: bash

               pip install "sqlspec[asyncpg,adk]"

         .. tab-item:: Poetry

            .. code-block:: bash

               poetry add "sqlspec[asyncpg,adk]"

         .. tab-item:: PDM

            .. code-block:: bash

               pdm add "sqlspec[asyncpg,adk]"

   .. tab-item:: SQLite

      .. tab-set::

         .. tab-item:: uv

            .. code-block:: bash

               uv add "sqlspec[aiosqlite,adk]"

         .. tab-item:: pip

            .. code-block:: bash

               pip install "sqlspec[aiosqlite,adk]"

         .. tab-item:: Poetry

            .. code-block:: bash

               poetry add "sqlspec[aiosqlite,adk]"

         .. tab-item:: PDM

            .. code-block:: bash

               pdm add "sqlspec[aiosqlite,adk]"

   .. tab-item:: MySQL

      .. tab-set::

         .. tab-item:: uv

            .. code-block:: bash

               uv add "sqlspec[asyncmy,adk]"

         .. tab-item:: pip

            .. code-block:: bash

               pip install "sqlspec[asyncmy,adk]"

         .. tab-item:: Poetry

            .. code-block:: bash

               poetry add "sqlspec[asyncmy,adk]"

         .. tab-item:: PDM

            .. code-block:: bash

               pdm add "sqlspec[asyncmy,adk]"

   .. tab-item:: DuckDB

      .. tab-set::

         .. tab-item:: uv

            .. code-block:: bash

               uv add "sqlspec[duckdb,adk]"

         .. tab-item:: pip

            .. code-block:: bash

               pip install "sqlspec[duckdb,adk]"

         .. tab-item:: Poetry

            .. code-block:: bash

               poetry add "sqlspec[duckdb,adk]"

         .. tab-item:: PDM

            .. code-block:: bash

               pdm add "sqlspec[duckdb,adk]"

What This Provides
------------------

The ``adk`` extra includes the Google ADK SDK (``google-genai``). SQLSpec provides:

- **Session Store** - Persist ADK agent sessions to your database.
- **Memory Store** - Store agent memory for context across conversations.
- **Event Store** - Log agent events for observability.

Next Steps
----------

Proceed to :doc:`quickstart` to set up stores for your ADK agent.
