========
Adapters
========

ADK stores use the same adapters as the rest of SQLSpec. Configure your database
with a standard config class, then pass it to the ADK store.

Choosing an Adapter
===================

Use async adapters for best performance with ADK runners:

- **PostgreSQL**: ``asyncpg`` (recommended), ``psycopg`` (async mode)
- **SQLite**: ``aiosqlite``
- **MySQL**: ``asyncmy``

Sync adapters work but require wrapping with ``anyio`` for async ADK runners.

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

- :doc:`backends` for the full adapter support matrix.
- :doc:`/usage/drivers_and_querying` for adapter configuration patterns.
- :doc:`/reference/adapters` for the complete adapter API.
