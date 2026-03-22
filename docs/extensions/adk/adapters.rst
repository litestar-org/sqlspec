========
Adapters
========

ADK stores use the same adapters as the rest of SQLSpec. Configure your database
with a standard config class, then pass it to the ADK store.

Choosing an Adapter
===================

Use async adapters for best performance with ADK runners:

- **PostgreSQL** (recommended): ``asyncpg``, ``psycopg`` (async mode), ``psqlpy``
- **CockroachDB**: ``cockroach_asyncpg``, ``cockroach_psycopg`` (full FTS support)
- **MySQL/MariaDB**: ``asyncmy``
- **SQLite**: ``aiosqlite`` (development and single-process)
- **Oracle**: ``oracledb``
- **DuckDB**: ``duckdb`` (analytics; reduced-scope for ADK)
- **ADBC**: ``adbc`` (Arrow-native, driver-agnostic)
- **Spanner**: ``spanner`` (Google Cloud, globally distributed)

Sync adapters (``psycopg`` sync mode, ``sqlite``, ``mysqlconnector``, ``pymysql``)
work but require wrapping with ``anyio`` for async ADK runners.

Each Adapter Provides
=====================

Every adapter with ADK support ships three store classes:

- **Session store** (e.g., ``AsyncpgADKStore``) -- sessions and events.
- **Memory store** (e.g., ``AsyncpgADKMemoryStore``) -- long-term memory with FTS.
- **Artifact store** (e.g., ``AsyncpgADKArtifactStore``) -- artifact metadata.

Import from the adapter's ``adk`` subpackage:

.. code-block:: python

   from sqlspec.adapters.asyncpg.adk import (
       AsyncpgADKStore,
       AsyncpgADKMemoryStore,
       AsyncpgADKArtifactStore,
   )

Example
=======

.. literalinclude:: /examples/extensions/adk/backend_config.py
   :language: python
   :caption: ``adk backend config``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

See Also
========

- :doc:`backends` for the full support matrix and backend-specific notes.
- :doc:`/usage/drivers_and_querying` for adapter configuration patterns.
- :doc:`/reference/adapters` for the complete adapter API.
