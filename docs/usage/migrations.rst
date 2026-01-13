Migrations
==========

SQLSpec ships with a built-in migration system backed by the SQL file loader.
Use it when you want a lightweight, code-first workflow without pulling in
Alembic or a full ORM stack.

Core Concepts
-------------

- Migrations are SQL or Python files stored in a migrations directory.
- Each database configuration can include its own migration settings.
- Extension migrations (ADK, events, Litestar sessions) are opt-in and versioned.

Common Commands
---------------

.. code-block:: console

   sqlspec db init
   sqlspec db create-migration -m "add users"
   sqlspec db upgrade

Configuration
-------------

Set ``migration_config`` on your database configuration to customize script
locations, version table names, and extension migration behavior.

Related Guides
--------------

- :doc:`cli` for full CLI usage.
- :doc:`../reference/migrations` for API details.
