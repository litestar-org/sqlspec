=========
Changelog
=========

All commits to this project will be documented in this file.

SQLSpec Changelog
==================

Recent Updates
==============

ADK Memory Store
----------------

- Added ``SQLSpecMemoryService`` and ``SQLSpecSyncMemoryService`` for SQLSpec-backed ADK memory storage.
- Implemented adapter-specific memory stores with optional full-text search (`memory_use_fts`) and simple fallback search.
- Extended ADK migrations to include memory tables with configurable ``include_memory_migration`` toggles.
- Added CLI commands for memory cleanup and verification (`sqlspec adk memory cleanup/verify`).

Driver Layer Compilation
------------------------

- Compiled driver base classes and mixins with mypyc to reduce dispatch overhead in the execution pipeline.
- Replaced dynamic ``getattr`` patterns with protocol-driven access for mypyc compatibility.
- Added driver protocols and updated mypyc build configuration to include driver modules.

Database Event Channels
-----------------------

- Added ``sqlspec.extensions.events.EventChannel`` with queue-backed publish/listen APIs that work uniformly across sync and async adapters.
- Exposed ``SQLSpec.event_channel(config)`` so applications and agents can build channels directly from registered configs.
- Introduced the ``events`` extension migrations (``ext_events_0001``) which create the durable queue table plus composite index.
- Added the first native backend (AsyncPG LISTEN/NOTIFY) enabled via ``driver_features["events_backend"] = "listen_notify"``; the API automatically falls back to the queue backend for other adapters.
- Introduced experimental Oracle Advanced Queuing support (sync adapters) via ``driver_features["events_backend"] = "advanced_queue"`` with automatic fallback when AQ is unavailable.
- Documented configuration patterns (queue table naming, lease/retention windows, Oracle ``INMEMORY`` toggle, Postgres native mode) in :doc:`/guides/events/database-event-channels`.
- Event telemetry now tracks ``events.publish``, ``events.publish.native``, ``events.deliver``, ``events.ack``, ``events.nack``, ``events.shutdown`` and listener lifecycle, so Prometheus/Otel exporters see event workloads alongside query metrics.
- Added adapter-specific runtime hints (asyncmy, duckdb, bigquery/adbc) plus a ``poll_interval`` extension option so operators can tune leases and cadence per database.
- Publishing, dequeue, ack, nack, and shutdown operations now emit ``sqlspec.events.*`` spans whenever ``extension_config["otel"]`` is enabled, giving full trace coverage without extra plumbing.
- Documented adapter-specific guidance (asyncpg, psycopg, psqlpy, asyncmy, duckdb, oracle) and added a DuckDB integration test to cover the queue fallback path.

v0.33.0 - Configuration Parameter Standardization (BREAKING CHANGE)
--------------------------------------------------------------------

**Breaking Change:** All adapter configuration parameter names have been standardized for consistency across the entire library.

**What Changed:**

All database adapter configurations now use consistent parameter names:

- ``pool_config`` → ``connection_config`` (configuration dictionary)
- ``pool_instance`` → ``connection_instance`` (pre-created pool/connection instance)

This affects **all 11 database adapters**: AsyncPG, Psycopg, Asyncmy, Psqlpy, OracleDB, SQLite, AioSQLite, DuckDB, BigQuery, ADBC, and Spanner.

**Migration:**

Simple search and replace in your codebase:

.. code-block:: bash

   # Replace pool_config with connection_config
   find . -name "*.py" -exec sed -i 's/pool_config=/connection_config=/g' {} +

   # Replace pool_instance with connection_instance
   find . -name "*.py" -exec sed -i 's/pool_instance=/connection_instance=/g' {} +

**Before:**

.. code-block:: python

   config = AsyncpgConfig(
       pool_config={"dsn": "postgresql://localhost/db"},
       pool_instance=my_pool
   )

**After:**

.. code-block:: python

   config = AsyncpgConfig(
       connection_config={"dsn": "postgresql://localhost/db"},
       connection_instance=my_pool
   )

**Why This Change:**

- Eliminates inconsistency between pooled and non-pooled adapters
- More intuitive naming (``connection_instance`` works semantically for both pools and single connections)
- Reduces cognitive load when switching between adapters
- Clearer API for new users

**See:** :doc:`/guides/migration/connection-config` for detailed migration guide with before/after examples for all adapters.

Query Stack Documentation Suite
--------------------------------

- Expanded the :doc:`/reference/query-stack` API reference (``StatementStack``, ``StackResult``, driver hooks, and ``StackExecutionError``) with the high-level workflow, execution modes, telemetry, and troubleshooting tips.
- Added :doc:`/examples/patterns/stacks/query_stack_example` that runs the same stack against SQLite and AioSQLite.
- Captured the detailed architecture and performance guidance inside the internal specs workspace for future agent runs.
- Updated every adapter reference with a **Query Stack Support** section so behavior is documented per database.

Migration Convenience Methods on Config Classes
------------------------------------------------

Added migration methods directly to database configuration classes, eliminating the need to instantiate separate command objects.

**What's New:**

All database configs (both sync and async) now provide migration methods:

- ``migrate_up()`` / ``upgrade()`` - Apply migrations up to a revision
- ``migrate_down()`` / ``downgrade()`` - Rollback migrations
- ``get_current_migration()`` - Check current version
- ``create_migration()`` - Create new migration file
- ``init_migrations()`` - Initialize migrations directory
- ``stamp_migration()`` - Stamp database to specific revision
- ``fix_migrations()`` - Convert timestamp to sequential migrations

**Before (verbose):**

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.migrations.commands import AsyncMigrationCommands

   config = AsyncpgConfig(
       connection_config={"dsn": "postgresql://..."},
       migration_config={"script_location": "migrations"}
   )

   commands = AsyncMigrationCommands(config)
   await commands.upgrade("head")

**After (recommended):**

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig

   config = AsyncpgConfig(
       connection_config={"dsn": "postgresql://..."},
       migration_config={"script_location": "migrations"}
   )

   await config.upgrade("head")

**Key Benefits:**

- Simpler API - no need to import and instantiate command classes
- Works with both sync and async adapters
- Full backward compatibility - command classes still available
- Cleaner test fixtures and deployment scripts

**Async Adapters** (AsyncPG, Asyncmy, Aiosqlite, Psqlpy):

.. code-block:: python

   await config.migrate_up("head")
   await config.create_migration("add users")

**Sync Adapters** (SQLite, DuckDB):

.. code-block:: python

   config.migrate_up("head")  # No await needed
   config.create_migration("add users")

SQL Loader Graceful Error Handling
-----------------------------------

**Breaking Change**: Files without named statements (``-- name:``) are now gracefully skipped instead of raising ``SQLFileParseError``.

This allows loading directories containing both aiosql-style named queries and raw DDL/DML scripts without errors.

**What Changed:**

- Files without ``-- name:`` markers return empty dict instead of raising exception
- Directory loading continues when encountering such files
- Skipped files are logged at DEBUG level
- Malformed named statements (duplicate names, etc.) still raise exceptions

**Migration Guide:**

Code explicitly catching ``SQLFileParseError`` for files without named statements will need updating:

.. code-block:: python

   # OLD (breaks):
   try:
       loader.load_sql("directory/")
   except SQLFileParseError as e:
       if "No named SQL statements found" in str(e):
           pass

   # NEW (recommended):
   loader.load_sql("directory/")  # Just works - DDL files skipped
   if not loader.list_queries():
       # No queries loaded
       pass

**Example Use Case:**

.. code-block:: python

   # Directory structure:
   # migrations/
   # ├── schema.sql              # Raw DDL (no -- name:) → SKIP
   # ├── queries.sql             # Named queries → LOAD
   # └── seed-data.sql          # Raw DML (no -- name:) → SKIP

   loader = SQLFileLoader()
   loader.load_sql("migrations/")  # Loads only named queries, skips DDL

Hybrid Versioning with Fix Command
-----------------------------------

Added comprehensive hybrid versioning support for database migrations:

- **Fix Command** - Convert timestamp migrations to sequential format
- **Hybrid Workflow** - Use timestamps in development, sequential in production
- **Automatic Conversion** - CI integration for seamless workflow
- **Safety Features** - Automatic backup, rollback on errors, dry-run preview

Key Features:

- **Zero merge conflicts**: Developers use timestamps (``20251011120000``) during development
- **Deterministic ordering**: Production uses sequential format (``0001``, ``0002``, etc.)
- **Database synchronization**: Automatically updates version tracking table
- **File operations**: Renames files and updates SQL query names
- **CI-ready**: ``--yes`` flag for automated workflows

.. code-block:: bash

   # Preview changes
   sqlspec --config myapp.config fix --dry-run

   # Apply conversion
   sqlspec --config myapp.config fix

   # CI/CD mode
   sqlspec --config myapp.config fix --yes --no-database

Example conversion:

.. code-block:: text

   Before:                              After:
   migrations/                          migrations/
   ├── 0001_initial.sql                ├── 0001_initial.sql
   ├── 0002_add_users.sql              ├── 0002_add_users.sql
   ├── 20251011120000_products.sql →   ├── 0003_add_products.sql
   └── 20251012130000_orders.sql   →   └── 0004_add_orders.sql

**Documentation:**

- Complete CLI reference: :doc:`usage/cli`
- Workflow guide: :ref:`hybrid-versioning-guide`
- CI integration examples for GitHub Actions and GitLab CI

**Use Cases:**

- Teams with parallel development avoiding migration number conflicts
- Projects requiring deterministic migration ordering in production
- CI/CD pipelines that standardize migrations before deployment

Shell Completion Support
-------------------------

Added comprehensive shell completion support for the SQLSpec CLI:

- **Bash, Zsh, and Fish support** - Tab completion for commands and options
- **Easy setup** - One-time eval command in your shell rc file
- **Comprehensive documentation** - Setup instructions in :doc:`usage/cli`

.. code-block:: bash

   # Bash - add to ~/.bashrc
   eval "$(_SQLSPEC_COMPLETE=bash_source sqlspec)"

   # Zsh - add to ~/.zshrc
   eval "$(_SQLSPEC_COMPLETE=zsh_source sqlspec)"

   # Fish - add to ~/.config/fish/completions/sqlspec.fish
   eval (env _SQLSPEC_COMPLETE=fish_source sqlspec)

After setup, tab completion works for all commands and options:

.. code-block:: bash

   sqlspec <TAB>              # Shows: create-migration, downgrade, init, ...
   sqlspec create-migration --<TAB>  # Shows: --bind-key, --help, --message, ...

Extension Migration Configuration
----------------------------------

Extension migrations now receive automatic version prefixes and configuration has been simplified:

1. **Version Prefixing** (Automatic)

   Extension migrations are automatically prefixed to prevent version collisions:

   .. code-block:: text

      # User migrations
      0001_initial.py       → version: 0001

      # Extension migrations (automatic prefix)
      0001_create_tables.py → version: ext_adk_0001
      0001_create_session.py → version: ext_litestar_0001

2. **Configuration Format** (Important)

   Extension settings must be in ``extension_config`` only:

   .. code-block:: python

      # Incorrect format
      migration_config={
          "include_extensions": [
              {"name": "adk", "session_table": "custom"}
          ]
      }

      # Correct format
      extension_config={
          "adk": {"session_table": "custom"}
      },
      migration_config={
          "include_extensions": ["adk"]  # Simple string list
      }

**Configuration Guide**: See :doc:`/migration_guides/extension_config`

Features
--------

- Extension migrations now automatically prefixed (``ext_adk_0001``, ``ext_litestar_0001``)
- Eliminated version collision between extension and user migrations
- Simplified extension configuration API
- Single source of truth for extension settings (``extension_config``)

Bug Fixes
---------

- Fixed version collision when extension and user migrations had the same version number
- Fixed duplicate key violation in ``ddl_migrations`` table when using extensions
- Improved migration tracking with clear extension identification

Technical Changes
-----------------

- ``_load_migration_metadata()`` now accepts optional ``version`` parameter
- ``_parse_extension_configs()`` rewritten to read from ``extension_config`` only
- Extension migration version prefixing handled in ``_get_migration_files_sync()``
- Removed dict format support from ``include_extensions``

**Previous Versions**
=====================
