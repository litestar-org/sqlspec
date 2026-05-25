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

   sqlspec init
   sqlspec create-migration -m "add users"
   sqlspec upgrade

Configuration
-------------

Set ``migration_config`` on your database configuration to customize script
locations, version table names, and extension migration behavior.

The migration CLI resolves config from ``--config``, ``SQLSPEC_CONFIG``, or
``[tool.sqlspec]`` in ``pyproject.toml``.

.. code-block:: python

    from sqlspec.adapters.duckdb import DuckDBConfig

    config = DuckDBConfig(
        connection_config={"database": "/tmp/analytics.db"},
        migration_config={
            "script_location": "migrations/duckdb",
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
        migration_config={"script_location": "migrations/postgres"},
    )

    await config.migrate_up()

Extension migrations are auto-included when the corresponding entry exists in
``extension_config``. Use ``migration_config["exclude_extensions"]`` to skip a
specific extension, ``migration_config["include_extensions"]`` to opt in
explicitly by extension name, or ``migration_config["enabled"] = False`` to
disable migrations entirely for a database config.

Configuring a Default Schema
----------------------------

Use ``migration_config["default_schema"]`` when migration SQL should run
against a pre-existing schema or dataset without qualifying every table in each
migration file. SQLSpec validates the schema before creating the tracker table
or applying DDL, then configures the migration session before each migration is
executed.

Use ``migration_config["version_table_schema"]`` when the migration tracker
table should live somewhere different from the objects managed by migrations.
If ``version_table_schema`` is not set, the tracker schema resolves to
``default_schema``. If neither field is set, the tracker table is unqualified and
uses the adapter's normal default namespace.

.. code-block:: python

    from sqlspec.adapters.asyncpg import AsyncpgConfig

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/app"},
        migration_config={
            "script_location": "migrations/postgres",
            "version_table_name": "schema_versions",
            "default_schema": "app_schema",
            "version_table_schema": "admin_schema",
        },
    )

The operator must create the target schema or dataset before running
migrations. The migration role also needs the database-specific privileges to
create objects there. For PostgreSQL, that usually means ``USAGE`` and
``CREATE`` on the target schema, plus permission to create or update the
tracker table.

Adapter support:

.. list-table::
   :header-rows: 1

   * - Adapter
     - Behavior
   * - ``asyncpg``, ``psycopg``, ``psqlpy``, ADBC PostgreSQL
     - Uses PostgreSQL ``search_path`` and validates ``information_schema.schemata``.
   * - ``oracledb``
     - Uses ``ALTER SESSION SET CURRENT_SCHEMA`` and validates Oracle users.
   * - ``duckdb``
     - Uses ``SET search_path`` and validates ``information_schema.schemata``.
   * - ``bigquery``
     - Treats schemas as datasets and sets the BigQuery job ``default_dataset``.
   * - ``sqlite``, ``aiosqlite``, ``asyncmy``
     - Accept the setting as an explicit no-op and log that default schemas are unsupported.
   * - ADBC SQL Server
     - Accepts the setting as a no-op; configure the default schema at the user or login level.

Example with unqualified DDL:

.. literalinclude:: /examples/patterns/migrations_with_schema.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example

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
