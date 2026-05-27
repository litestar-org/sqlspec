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
against a pre-existing schema without qualifying every table in each migration
file. SQLSpec validates the schema before creating the tracker table or applying
DDL, then configures the migration session before each migration is executed.

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

The operator must create the target schema before running migrations. The
migration role also needs the database-specific privileges to create objects
there. For PostgreSQL, that usually means ``USAGE`` and
``CREATE`` on the target schema, plus permission to create or update the
tracker table.

Adapter support is opt-in via the ``supports_migration_schemas`` class flag on
each config. Configuring ``default_schema`` against an adapter that does not
opt in raises ``MigrationError`` before any DDL is issued.

.. list-table::
   :header-rows: 1
   :widths: 28 18 54

   * - Adapter
     - Default schema
     - Mechanism
   * - ``asyncpg``
     - Supported
     - ``SET LOCAL search_path`` (transactional) / ``SET search_path`` + ``RESET`` (non-transactional);
       validates ``information_schema.schemata``.
   * - ``psycopg`` (sync and async)
     - Supported
     - Same as ``asyncpg``.
   * - ``psqlpy``
     - Supported
     - Same as ``asyncpg``.
   * - ``cockroach_asyncpg``
     - Supported
     - Inherits ``asyncpg`` behavior. CockroachDB exposes the PostgreSQL
       wire protocol and accepts ``SET search_path``.
   * - ``cockroach_psycopg`` (sync and async)
     - Supported
     - Inherits ``psycopg`` behavior.
   * - ``adbc`` (PostgreSQL dialect)
     - Supported
     - Same as ``asyncpg``. Detection is dialect-based on the configured
       ADBC URI; ``supports_migration_schemas`` becomes ``True`` only when
       the resolved dialect is PostgreSQL-compatible.
   * - ``oracledb`` (sync and async)
     - Supported
     - ``ALTER SESSION SET CURRENT_SCHEMA``; validates ``ALL_USERS``. Quoted
       identifiers preserve case; unquoted identifiers are uppercased per
       Oracle's normalization rules.
   * - ``duckdb``
     - Supported
     - ``SET search_path``; validates ``information_schema.schemata``.
   * - ``sqlite``, ``aiosqlite``
     - Not supported
     - SQLite has no schema namespace; use ``ATTACH DATABASE`` to layer
       additional databases instead.
   * - ``asyncmy``, ``aiomysql``, ``mysqlconnector``, ``pymysql``
     - Not supported
     - MySQL conflates schema and database. Select the target database in
       the connection URL or via ``USE`` inside the migration.
   * - ``adbc`` (non-PostgreSQL dialects, including SQL Server)
     - Not supported
     - ADBC does not expose a portable per-session schema setter for these
       dialects. Configure the default schema at the user or login level in
       the underlying database.
   * - ``mssql_python``
     - Not supported
     - SQL Server resolves the default schema from the login. Set it with
       ``ALTER USER ... WITH DEFAULT_SCHEMA = ...`` in your database.
   * - ``bigquery``
     - Not supported
     - BigQuery requires fully qualified ``project.dataset.table`` references
       for cross-dataset DDL; there is no session-scoped default dataset.
   * - ``spanner``
     - Not supported
     - Cloud Spanner ties objects to a single schema per database; there is
       no session-scoped switch.
   * - ``arrow_odbc``
     - Not supported
     - ODBC connection-string semantics vary per driver. Configure the
       default schema through the underlying DSN.

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
