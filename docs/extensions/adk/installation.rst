============
Installation
============

Install SQLSpec with a database adapter and the Google ADK SDK.

.. tab-set::

   .. tab-item:: PostgreSQL (recommended)

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

   .. tab-item:: CockroachDB

      .. tab-set::

         .. tab-item:: uv

            .. code-block:: bash

               uv add "sqlspec[cockroach-asyncpg,adk]"

         .. tab-item:: pip

            .. code-block:: bash

               pip install "sqlspec[cockroach-asyncpg,adk]"

         .. tab-item:: Poetry

            .. code-block:: bash

               poetry add "sqlspec[cockroach-asyncpg,adk]"

         .. tab-item:: PDM

            .. code-block:: bash

               pdm add "sqlspec[cockroach-asyncpg,adk]"

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

- **Session Service** -- Persist ADK agent sessions and events to your database
  with atomic ``append_event_and_update_state()`` writes.
- **Memory Service** -- Store agent memory with database-native full-text search
  for context retrieval across conversations.
- **Artifact Service** -- Version and store binary artifacts with SQL metadata
  and pluggable object storage backends.
- **Event Storage** -- Full-event JSON storage (EventRecord) that captures the
  entire ADK Event without schema drift.

Next Steps
----------

Proceed to :doc:`quickstart` to set up stores for your ADK agent, or see
:doc:`backends` for the full support matrix.
