Command Line Interface
======================

SQLSpec includes a CLI for managing migrations, inspecting configurations, and maintaining
extensions. Use it for standalone workflows or integrate it into web framework CLIs.

Every standalone command needs a configuration reference. Pass it with ``--config``, set
``SQLSPEC_CONFIG``, or record it in ``[tool.sqlspec]`` -- see
:ref:`pointing-the-cli-at-your-configuration` for details.

Core Commands
-------------

.. code-block:: console

   # Inspect resolved database configurations and migration status
   sqlspec --config database:database_config show-config

   # Initialize migration directory structure
   sqlspec --config database:database_config init --no-prompt

   # Create a new migration revision (SQL or Python)
   sqlspec --config database:database_config create-migration -m "add users table" --no-prompt

   # Apply pending migrations up to head (or a specific revision)
   sqlspec --config database:database_config upgrade --no-prompt

   # Revert migrations down by one step (or to a target revision)
   sqlspec --config database:database_config downgrade --no-prompt

   # Check the current applied revision in the database
   sqlspec --config database:database_config show-current-revision

   # Stamp the tracking table with a revision without executing DDL
   sqlspec --config database:database_config stamp 0005

   # Reconcile timestamp-based migrations into sequential format
   sqlspec --config database:database_config fix --dry-run

   # Squash multiple sequential migrations into a single consolidated file
   sqlspec --config database:database_config squash 1:7 -m "squash initial migrations"

Command Reference
-----------------

.. list-table::
   :header-rows: 1
   :widths: 24 36 40

   * - Command
     - Arguments
     - Description
   * - ``show-config``
     - (none)
     - Display all discovered database configurations, paths, and migration statuses.
   * - ``init``
     - ``[DIRECTORY]``
     - Initialize a migrations directory. Defaults to the configured ``script_location``.
   * - ``create-migration``
     - (none)
     - Generate a new migration file. Alias: ``make-migration``.
   * - ``upgrade``
     - ``[REVISION]``
     - Upgrade database to target revision (default: ``head``).
   * - ``downgrade``
     - ``[REVISION]``
     - Downgrade database by steps or to target revision (default: ``-1``).
   * - ``show-current-revision``
     - (none)
     - Query and print current migration revision from the database tracker table.
   * - ``stamp``
     - ``REVISION``
     - Record a revision in the tracking table without executing SQL statements.
   * - ``fix``
     - (none)
     - Convert legacy timestamp migrations to sequential ``0001_...`` naming.
   * - ``squash``
     - ``VERSION_RANGE``
     - Collapse sequential migrations (e.g. ``1:5`` or ``1..5``) into one file.
   * - ``adk memory cleanup``
     - ``--days N``
     - Delete ADK session memory records older than *N* days.
   * - ``adk memory verify``
     - (none)
     - Verify ADK session memory tables exist and are reachable.

Command Options
---------------

Global & Execution Options
~~~~~~~~~~~~~~~~~~~~~~~~~~

- ``--config <path>``: Dotted path to config object or factory callable (env: ``SQLSPEC_CONFIG``). Comma-separated paths support multi-database setups.
- ``--validate-config``: Report each configuration and whether it is async-capable before running.
- ``--bind-key <key>``: Target a specific configuration by its bind key.
- ``--include <key>`` / ``--exclude <key>``: Filter targeted configurations for multi-database operations (can be repeated).
- ``--dry-run``: Show what would be applied without modifying database state or files.
- ``--no-prompt``: Bypass interactive confirmation prompts (ideal for CI/CD).
- ``--verbose``: Enable detailed output (supported on ``show-current-revision``).

Migration Output & Format Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- ``--format`` / ``--file-type [sql|py]``: Migration file format for ``create-migration`` (default: per template profile).
- ``--use-logger``: Emit migration output through standard Python structured logging instead of rich console output.
- ``--no-echo``: Silence console output during execution.
- ``--summary``: Emit a single aggregated summary log record when logger output is enabled.
- ``--no-auto-sync``: Disable automatic checksum reconciliation when migrations have been renamed during ``upgrade``.
- ``--output-format [sql|py]``: Output format for squashed migrations in ``squash`` (default: ``sql``).
- ``--allow-gaps``: Allow gaps in the version sequence during ``squash``.

Multi-Database CLI Operations
-----------------------------

When managing applications with multiple databases (e.g., primary and analytics replica),
pass multiple configurations separated by commas:

.. code-block:: console

   sqlspec --config app.db:primary_config,app.db:analytics_config show-config

Target all databases simultaneously or scope commands using ``--include``, ``--exclude``,
or ``--bind-key``:

.. code-block:: console

   # Upgrade all configured databases
   sqlspec --config app.db:get_configs upgrade --no-prompt

   # Upgrade only the primary database
   sqlspec --config app.db:get_configs upgrade --bind-key primary --no-prompt

   # Exclude specific databases
   sqlspec --config app.db:get_configs upgrade --exclude analytics --no-prompt

Framework Integration (Litestar)
--------------------------------

When using the Litestar extension (``SQLSpecPlugin``), all migration commands are
automatically exposed under Litestar's CLI group:

.. code-block:: console

   litestar db show-config
   litestar db init
   litestar db create-migration -m "add orders"
   litestar db upgrade
   litestar db downgrade
   litestar db show-current-revision

Related Guides
--------------

- :doc:`migrations` for complete migration configuration, templates, and Python migration APIs.
- :doc:`frameworks/litestar/index` for Litestar integration details.
