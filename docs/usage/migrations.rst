Migrations
==========

SQLSpec ships with a built-in migration system backed by the SQL file loader.
Use it when you want a lightweight, code-first workflow without pulling in
Alembic or a full ORM stack.

- Migrations are SQL or Python files stored in a migrations directory.
- Each database configuration carries its own migration settings.
- Extension migrations (ADK, events, Litestar sessions) are opt-in and versioned.
- Any installed package can ship migrations, not just those under ``sqlspec.extensions``.

Quickstart
----------

Export a configuration from a module the CLI can import. Importing this module
only defines the configuration -- migrations run when you invoke the CLI, not at
application import time.

.. literalinclude:: /examples/migration_quickstart_config.py
   :language: python
   :caption: database.py

Point the CLI at that object with ``module:attribute`` and run the workflow:

.. code-block:: console

   sqlspec --config database:database_config show-config
   sqlspec --config database:database_config init --no-prompt
   sqlspec --config database:database_config create-migration -m "create users table" --no-prompt
   sqlspec --config database:database_config upgrade --no-prompt
   sqlspec --config database:database_config show-current-revision

``show-config`` is the fastest way to confirm the CLI found what you expected
before running anything that touches the database.

This example and the full command sequence are exercised by the documentation
test suite.

.. _pointing-the-cli-at-your-configuration:

Pointing the CLI at your configuration
--------------------------------------

The reference names a module and the attribute holding your configuration. Both
separators work, so ``database:database_config`` and
``database.database_config`` are equivalent:

.. code-block:: console

   sqlspec --config database:database_config show-config

The attribute may be a single configuration, a list of configurations, or a
factory function returning either. Naming the module alone is not enough --
SQLSpec reports the references the module exports so you can correct the
command.

To avoid repeating ``--config``, set an environment variable:

.. code-block:: console

   export SQLSPEC_CONFIG=database:database_config
   sqlspec show-config

Or record it once in ``pyproject.toml``:

.. code-block:: toml

   [tool.sqlspec]
   config = "database:database_config"

``--config`` wins over ``SQLSPEC_CONFIG``, which wins over ``pyproject.toml``.

To manage several databases at once, separate references with commas.
Configurations are deduplicated by ``bind_key``, so give each one a distinct
key:

.. code-block:: console

   sqlspec --config database:primary_config,database:replica_config upgrade

Modules are imported from the current working directory, so a ``database.py``
beside your ``pyproject.toml`` is importable without installing your project.

Configuration
-------------

``migration_config`` customizes script locations, the version table, and
extension behavior. Unrecognized keys raise
:class:`~sqlspec.exceptions.ImproperConfigurationError` at construction rather
than being silently ignored, so a misspelling surfaces immediately.

.. code-block:: python

    from sqlspec.adapters.duckdb import DuckDBConfig

    config = DuckDBConfig(
        connection_config={"database": "/tmp/analytics.db"},
        migration_config={
            "script_location": "migrations/duckdb",
            "version_table_name": "_schema_versions",
        },
    )

Migrations can also be driven in process:

.. code-block:: python

    config.migrate_up()
    config.migrate_up(revision="003")
    config.migrate_up(dry_run=True)

For async configurations, ``migrate_up()`` returns an awaitable:

.. code-block:: python

    from sqlspec.adapters.asyncpg import AsyncpgConfig

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/app"},
        migration_config={"script_location": "migrations/postgres"},
    )

    await config.migrate_up()

Common keys
~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 26 74

   * - Key
     - Purpose
   * - ``script_location``
     - Migrations directory. Defaults to ``migrations``.
   * - ``version_table_name``
     - Tracking table name. Defaults to ``ddl_migrations``.
   * - ``enabled``
     - Set ``False`` to exclude this configuration from CLI operations.
   * - ``strict_ordering``
     - Reject out-of-order migrations. Defaults to ``False``.
   * - ``transactional``
     - Wrap each migration in a transaction where the adapter supports it.
   * - ``include_extensions`` / ``exclude_extensions``
     - Opt extensions into or out of migration discovery by name.

See :class:`~sqlspec.config.MigrationConfig` for the complete set.

Running Against an Existing Schema
----------------------------------

Set ``default_schema`` when migration SQL should run against a pre-existing
schema without qualifying every table in every migration file. SQLSpec
validates the schema before creating the tracker table or applying DDL, then
configures the session before each migration runs.

Set ``version_table_schema`` when the tracker table belongs somewhere other
than the objects being migrated. It falls back to ``default_schema``; if
neither is set, the tracker table is unqualified and uses the adapter's normal
default namespace.

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

Create the target schema before running migrations. The migration role needs
the database-specific privileges to create objects there -- for PostgreSQL,
usually ``USAGE`` and ``CREATE`` on the target schema plus permission to create
or update the tracker table.

Example with unqualified DDL:

.. literalinclude:: /examples/patterns/migrations_with_schema.py
   :language: python
   :start-after: # start-example
   :end-before: # end-example

Adapter support
~~~~~~~~~~~~~~~

Support is opt-in per adapter via the ``supports_migration_schemas`` class
flag. Configuring ``default_schema`` against an adapter that does not opt in
raises ``MigrationError`` before any DDL is issued.

.. list-table:: Supported
   :header-rows: 1
   :widths: 34 66

   * - Adapter
     - Mechanism
   * - ``asyncpg``, ``psycopg``, ``psqlpy``
     - ``SET LOCAL search_path`` when transactional, otherwise ``SET
       search_path`` followed by ``RESET``. Validates against
       ``information_schema.schemata``.
   * - ``cockroach_asyncpg``, ``cockroach_psycopg``
     - Inherit the PostgreSQL driver behavior above; CockroachDB accepts ``SET
       search_path`` over the PostgreSQL wire protocol.
   * - ``adbc`` (PostgreSQL dialect)
     - Same as ``asyncpg``. Detection is dialect-based on the configured ADBC
       URI, so ``supports_migration_schemas`` is ``True`` only when the
       resolved dialect is PostgreSQL-compatible.
   * - ``oracledb``
     - ``ALTER SESSION SET CURRENT_SCHEMA``, validated against ``ALL_USERS``.
       Names follow Oracle's stored identifier rules: unquoted lowercase names
       are uppercased, mixed-case and quoted names are preserved.
   * - ``duckdb``
     - ``SET search_path``. Validates against ``information_schema.schemata``.

.. list-table:: Not supported
   :header-rows: 1
   :widths: 34 66

   * - Adapter
     - Use instead
   * - ``sqlite``, ``aiosqlite``
     - SQLite has no schema namespace. Layer additional databases with
       ``ATTACH DATABASE``.
   * - ``asyncmy``, ``aiomysql``, ``mysqlconnector``, ``pymysql``
     - MySQL conflates schema and database. Select the target database in the
       connection URL, or issue ``USE`` inside the migration.
   * - ``adbc`` (non-PostgreSQL dialects, including SQL Server)
     - No portable per-session schema setter. Configure the default schema at
       the user or login level in the database.
   * - ``mssql_python``
     - SQL Server resolves the default schema from the login. Set it with
       ``ALTER USER ... WITH DEFAULT_SCHEMA = ...``.
   * - ``bigquery``
     - Cross-dataset DDL requires fully qualified
       ``project.dataset.table`` references; there is no session-scoped default
       dataset.
   * - ``spanner``
     - Objects are tied to a single schema per database, with no session-scoped
       switch.
   * - ``arrow_odbc``
     - ODBC connection-string semantics vary per driver. Configure the default
       schema through the DSN.

Extension Migrations
--------------------

Extensions are auto-included when a matching entry exists in
``extension_config``. Names resolve against the ``sqlspec.extensions.<name>``
namespace by default.

A package distributed separately from SQLSpec points at its own migrations
directory with ``migrations_path``:

.. code-block:: python

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/app"},
        extension_config={
            "litestar_queues": {
                "migrations_path": "litestar_queues.backends.sqlspec:migrations",
                "table_name": "queue_tasks",
            }
        },
    )

Declaring ``migrations_path`` auto-includes the extension, so it does not also
need to appear in ``include_extensions``. ``exclude_extensions`` still opts it
back out. The value takes either form:

- A ``'<dotted.module>:<subdir>'`` specification resolved against the installed
  package. Portable across machines, so prefer it in ``pyproject.toml``.
- A filesystem path, absolute or relative to the working directory, matching
  how ``script_location`` resolves.

Packages that register migrations at runtime can call
``add_extension_migrations`` instead of declaring the key:

.. code-block:: python

    from pathlib import Path

    config.add_extension_migrations(
        "litestar_queues",
        Path(__file__).parent / "migrations",
        settings={"table_name": "queue_tasks"},
    )

Both forms record the extension under ``extension_config`` and opt it into
``include_extensions``. Call ``add_extension_migrations`` before
``get_migration_commands()`` -- mutating ``extension_config`` directly after the
configuration is built does not re-run discovery.

SQL files inside a registered extension directory use their filename-local
version in named-query directives. For example,
``migrations/0001_create_queue.sql`` declares ``migrate-0001-up`` and
``migrate-0001-down``. SQLSpec still records that migration as
``ext_litestar_queues_0001`` when the registered extension name is
``litestar_queues``.

An extension migration stored in the application's main migration directory
instead carries the prefix in its filename, such as
``ext_litestar_queues_0001_create_queue.sql``, and therefore declares
``migrate-ext_litestar_queues_0001-up`` and
``migrate-ext_litestar_queues_0001-down``.

.. note::

   Extension migrations are versioned under an ``ext_{name}_`` prefix, and that
   prefix is written to the tracking table. Keep the extension name stable once
   migrations have been applied -- renaming it orphans the applied records.

   A package shipping migrations must include the directory as package data. If
   it compiles its own modules, the migration sources must remain on disk, since
   Python migrations are read and compiled at runtime.

Migration File Templates
------------------------

``create-migration`` renders new files from a built-in template. Three
``migration_config`` keys adjust what it writes:

.. list-table::
   :header-rows: 1
   :widths: 24 76

   * - Key
     - Purpose
   * - ``default_format``
     - Format used when the command is run without ``--file-type``. Either
       ``sql`` or ``py``. Defaults to ``sql``.
   * - ``title``
     - Title rendered into generated files. Defaults to ``SQLSpec Migration``.
   * - ``templates``
     - Fragment overrides for the ``sql`` and ``py`` templates.

Overrides replace individual fragments; anything omitted keeps its default:

.. code-block:: python

    config = DuckDBConfig(
        connection_config={"database": "/tmp/analytics.db"},
        migration_config={
            "title": "Acme Migration",
            "default_format": "py",
            "templates": {
                "sql": {
                    "header": "-- {title} [{adapter}]",
                    "metadata": ["-- Version: {version}", "-- Owner: {author}"],
                }
            },
        },
    )

Every fragment is rendered with ``str.format``, so these placeholders are
available: ``title``, ``version``, ``message``, ``description``, ``created_at``,
``author``, ``adapter``, ``project_slug``, and ``slug`` (the filename-safe form
of the message). An unknown placeholder raises
:class:`~sqlspec.migrations.templates.TemplateValidationError` when the file is
generated.

The SQL template accepts ``header``, ``metadata``, ``body``, and
``description_key``; the Python template accepts ``docstring``, ``imports``,
``body``, and ``description_key``. ``description_key`` names the label the
description is read back from, and takes a string or a list of strings.

.. note::

   A body override owns the whole migration body, including the
   ``-- name: migrate-{version}-up`` and ``-- name: migrate-{version}-down``
   markers for SQL, or the ``up``/``down`` functions for Python. SQLSpec does
   not merge fragments into a replaced body.

See :class:`~sqlspec.config.MigrationTemplates` for the full override shape.

Output and Logging
------------------

Control output with ``migration_config`` keys or their CLI equivalents:

.. list-table::
   :header-rows: 1
   :widths: 24 20 56

   * - Key
     - CLI flag
     - Effect
   * - ``use_logger``
     - ``--use-logger``
     - Emit structured logs instead of console output.
   * - ``echo``
     - ``--no-echo``
     - Control console output when not using the logger.
   * - ``summary_only``
     - ``--summary``
     - Emit a single summary log entry when logger output is enabled.

Related Guides
--------------

- :doc:`cli` for full CLI usage.
- :doc:`../reference/migrations` for API details.
