Migrations
==========

.. image:: /_static/demos/migration_workflow.gif
   :alt: SQLSpec migration workflow demo
   :class: demo-gif

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

.. code-block:: python

    from sqlspec.adapters.duckdb import DuckDBConfig

    config = DuckDBConfig(
        connection_config={"database": "/tmp/analytics.db"},
        migration_config={
            "migration_dir": "migrations/duckdb",
            "version_table": "_schema_versions",
        },
    )

    # Apply all pending migrations
    config.migrate_up()

    # Apply up to a specific revision
    config.migrate_up(revision="003")

    # Dry run to see what would happen
    config.migrate_up(dry_run=True)

For async configs, ``migrate_up()`` returns an awaitable:

.. code-block:: python

    from sqlspec.adapters.asyncpg import AsyncpgConfig

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/app"},
        migration_config={"migration_dir": "migrations/postgres"},
    )

    await config.migrate_up()

Logging and Echo Controls
-------------------------

Configure output behavior with ``migration_config`` or CLI flags:

- ``use_logger`` to emit structured logs instead of console output.
- ``echo`` to control console output when not using the logger.
- ``summary_only`` to emit a single summary log entry when logger output is enabled.

The CLI equivalents are ``--use-logger``, ``--no-echo``, and ``--summary``.

Related Guides
--------------

- :doc:`cli` for full CLI usage.
- :doc:`../reference/migrations` for API details.
