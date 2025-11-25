.. _migrations-guide:

===================
Database Migrations
===================

SQLSpec provides a comprehensive migration system for managing database schema changes
over time. The migration system supports both SQL and Python migrations with automatic
tracking, version reconciliation, and hybrid versioning workflows.

.. contents:: Table of Contents
   :local:
   :depth: 2

Quick Start
===========

Initialize Migrations
---------------------

.. code-block:: bash

   # Initialize migration directory
   sqlspec --config myapp.config init

   # Create your first migration
   sqlspec --config myapp.config create-migration -m "Initial schema"

   # Apply migrations
   sqlspec --config myapp.config upgrade

Programmatic API (Recommended)
===============================

SQLSpec provides migration convenience methods directly on config classes, eliminating
the need to instantiate separate command objects.

Async Adapters
--------------

For async adapters (AsyncPG, Asyncmy, Aiosqlite, Psqlpy), migration methods return awaitables:

.. literalinclude:: /examples/usage/usage_migrations_1.py
   :language: python
   :caption: `async adapters`
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example

Sync Adapters
-------------

For sync adapters (SQLite, DuckDB), migration methods execute immediately without await:

.. literalinclude:: /examples/usage/usage_migrations_2.py
   :language: python
   :caption: `sync adapters`
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example

Available Methods
-----------------

All database configs (sync and async) provide these migration methods:

``migrate_up(revision="head", allow_missing=False, auto_sync=True, dry_run=False)``
   Apply migrations up to the specified revision.

   Also available as ``upgrade()`` alias.

``migrate_down(revision="-1", dry_run=False)``
   Rollback migrations down to the specified revision.

   Also available as ``downgrade()`` alias.

``get_current_migration(verbose=False)``
   Get the current migration version.

Template Profiles & Author Metadata
===================================

Migrations inherit their header text, metadata comments, and default file format
from ``migration_config["templates"]``. Each project can define multiple
profiles and select one globally:

.. literalinclude:: /examples/usage/usage_migrations_3.py
   :language: python
   :caption: `template profile`
   :dedent: 0
   :start-after: # start-example
   :end-before: # end-example

Template fragments accept the following variables:

- ``{title}`` – shared template title
- ``{version}`` – generated revision identifier
- ``{message}`` – CLI/command message
- ``{description}`` – message fallback used in logs and docstrings
- ``{created_at}`` – UTC timestamp in ISO 8601 format
- ``{author}`` – resolved author string
- ``{adapter}`` – config driver class (useful for docstrings)
- ``{project_slug}`` / ``{slug}`` – sanitized project and message slugs

Missing placeholders raise ``TemplateValidationError`` so mistakes are caught
immediately. SQL templates list metadata rows (``metadata``) and a ``body``
block. Python templates expose ``docstring``, optional ``imports``, and ``body``.

Author attribution can be controlled via ``migration_config["author"]``:

- Literal strings (``"Data Platform"``) are stamped verbatim
- ``"env:VAR_NAME"`` pulls from the environment and fails fast if unset
- ``"callable:pkg.module:get_author"`` invokes a helper that can inspect the
  config or environment when determining the author string
- ``"git"`` reads git user.name/email; ``"system"`` uses ``$USER``

CLI Enhancements
----------------

``sqlspec create-migration`` (and ``litestar database create-migration``)
accept ``--format`` / ``--file-type`` flags:

.. code-block:: bash

   sqlspec --config myapp.config create-migration -m "Add seed data" --format py

When omitted, the CLI uses ``migration_config["default_format"]`` (``"sql"`` by default).
Upgrade/downgrade commands now echo ``{version}: {description}``, so the rich
description captured in templates is visible during deployments and matches the
continue-on-error logs.

The default Python template ships with both ``up`` and ``down`` functions that
accept an optional ``context`` argument. When migrations run via SQLSpec, that
parameter receives the active ``MigrationContext`` so you can reach the config
or connection objects directly inside your migration logic.

``create_migration(message, file_type="sql")``
   Create a new migration file.

``init_migrations(directory=None, package=None)``
   Initialize the migrations directory structure.

``stamp_migration(revision)``
   Stamp the database to a specific revision without running migrations.

``fix_migrations(dry_run=False, update_database=True, yes=False)``
   Convert timestamp migrations to sequential format.

Command Classes (Advanced)
---------------------------

For advanced use cases requiring custom logic, you can still use command classes directly:

.. literalinclude:: /examples/usage/usage_migrations_4.py
      :language: python
      :caption: `command classes`
      :dedent: 0
      :start-after: # start-example
      :end-before: # end-example

This approach is useful when:

- Building custom migration runners
- Implementing migration lifecycle hooks
- Integrating with third-party workflow tools
- Need fine-grained control over migration execution

Configuration
=============

Enable migrations in your SQLSpec configuration:

.. literalinclude:: /examples/usage/usage_migrations_5.py
      :language: python
      :caption: `configuration`
      :dedent: 0
      :start-after: # start-example
      :end-before: # end-example

Configuration Options
---------------------

``enabled``
   **Type:** ``bool``
   **Default:** ``False``

   Enable or disable migrations for this configuration.

``script_location``
   **Type:** ``str``
   **Default:** ``"migrations"``

   Path to migration files directory (relative to project root).

``version_table_name``
   **Type:** ``str``
   **Default:** ``"ddl_migrations"``

   Name of the table used to track applied migrations.

``auto_sync``
   **Type:** ``bool``
   **Default:** ``True``

   Enable automatic version reconciliation when migrations are renamed.
   When ``True``, the ``upgrade`` command automatically updates database
   tracking when migrations have been converted from timestamp to sequential
   format using the ``fix`` command.

``project_root``
   **Type:** ``Path | str | None``
   **Default:** ``None``

   Root directory for Python migration imports. If not specified, uses
   the parent directory of ``script_location``.

Migration Files
===============

SQL Migrations
--------------

SQL migrations use the aiosql-style named query format:

.. code-block:: sql

   -- migrations/0001_initial.sql

   -- name: migrate-0001-up
   CREATE TABLE users (
       id SERIAL PRIMARY KEY,
       email TEXT NOT NULL UNIQUE,
       created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
   );

   CREATE INDEX idx_users_email ON users(email);

   -- name: migrate-0001-down
   DROP TABLE users;

**Naming Convention:**

- File: ``{version}_{description}.sql``
- Upgrade query: ``migrate-{version}-up``
- Downgrade query: ``migrate-{version}-down`` (optional)

Python Migrations
-----------------

Python migrations provide more flexibility for complex operations:

.. literalinclude:: /examples/usage/usage_migrations_6.py
         :language: python
         :caption: `python migrations`
         :dedent: 0
         :start-after: # start-example
         :end-before: # end-example

**Advanced Usage:**

Python migrations can also return a list of SQL statements:

.. literalinclude:: /examples/usage/usage_migrations_7.py
         :language: python
         :caption: `advanced usage`
         :dedent: 0
         :start-after: # start-example
         :end-before: # end-example


.. _hybrid-versioning-guide:

Hybrid Versioning
=================

SQLSpec supports a hybrid versioning workflow that combines timestamp-based versions
during development with sequential versions in production.

Overview
--------

**Problem:** Timestamp versions (``20251018120000``) prevent merge conflicts when multiple
developers create migrations simultaneously, but sequential versions (``0001``) provide
more predictable ordering in production.

**Solution:** Use timestamps during development, then convert to sequential numbers before
deploying to production using the ``fix`` command.

Workflow
--------

**1. Development - Use Timestamps**

.. code-block:: bash

   # Developer A creates migration
   sqlspec --config myapp.config create-migration -m "Add users table"
   # Creates: 20251018120000_add_users_table.sql

   # Developer B creates migration (same day)
   sqlspec --config myapp.config create-migration -m "Add products table"
   # Creates: 20251018123000_add_products_table.sql

**2. Pre-Merge - Convert to Sequential**

Before merging to main branch (typically in CI):

.. code-block:: bash

   # Preview changes
   sqlspec --config myapp.config fix --dry-run

   # Apply conversion
   sqlspec --config myapp.config fix --yes

   # Results:
   # 20251018120000_add_users_table.sql    → 0001_add_users_table.sql
   # 20251018123000_add_products_table.sql → 0002_add_products_table.sql

**3. After Pull - Auto-Sync**

When teammates pull your converted migrations, they don't need to do anything special:

.. code-block:: bash

   git pull origin main

   # Just run upgrade - auto-sync handles reconciliation
   sqlspec --config myapp.config upgrade

Auto-sync automatically detects renamed migrations using checksums and updates
the database tracking table to reflect the new version numbers.

Version Formats
---------------

**Sequential Format**
   Pattern: ``^(\d+)$``

   Examples: ``0001``, ``0042``, ``9999``, ``10000``

   - Used in production
   - Deterministic ordering
   - Human-readable sequence
   - No upper limit (4-digit cap removed)

**Timestamp Format**
   Pattern: ``^(\d{14})$``

   Example: ``20251018120000`` (2025-10-18 12:00:00 UTC)

   - Used during development
   - Prevents merge conflicts
   - Chronologically ordered
   - UTC timezone

Version Comparison
------------------

SQLSpec uses type-aware version comparison:

.. literalinclude:: /examples/usage/usage_migrations_8.py
         :language: python
         :caption: `version comparison`
         :dedent: 0
         :start-after: # start-example
         :end-before: # end-example

Migration Tracking
==================

Schema
------

SQLSpec uses a tracking table to record applied migrations:

.. code-block:: sql

   CREATE TABLE ddl_migrations (
       version_num VARCHAR(32) PRIMARY KEY,
       version_type VARCHAR(16),           -- 'sequential' or 'timestamp'
       execution_sequence INTEGER,         -- Order of execution
       description TEXT,
       applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
       execution_time_ms INTEGER,
       checksum VARCHAR(64),               -- MD5 hash for auto-sync
       applied_by VARCHAR(255)
   );

**Columns:**

``version_num``
   The migration version (e.g., ``"0001"`` or ``"20251018120000"``).

``version_type``
   Format indicator: ``"sequential"`` or ``"timestamp"``.

``execution_sequence``
   Auto-incrementing counter showing actual application order.
   Preserves history when out-of-order migrations are applied.

``checksum``
   MD5 hash of migration content. Used by auto-sync to match
   renamed migrations (e.g., timestamp → sequential conversion).

``applied_by``
   Author string recorded for the migration. Defaults to the git user/system
   account but can be overridden via ``migration_config["author"]``.

Schema Migration
----------------

When upgrading from older SQLSpec versions, the tracking table schema is automatically
migrated to add the new columns (``execution_sequence``, ``version_type``, ``checksum``).

This happens transparently when you run any migration command:

.. code-block:: bash

   # First upgrade after updating SQLSpec
   sqlspec --config myapp.config upgrade

   # Output:
   # Migrating tracking table schema, adding columns: checksum, execution_sequence, version_type
   # Migration tracking table schema updated successfully

The schema migration:

1. Detects missing columns using database metadata queries
2. Adds columns one by one using ``ALTER TABLE``
3. Populates ``execution_sequence`` based on ``applied_at`` timestamps
4. Preserves all existing migration history

Extension Migrations
====================

SQLSpec supports independent migration versioning for extensions and plugins.

Configuration
-------------

.. literalinclude:: /examples/usage/usage_migrations_9.py
         :language: python
         :caption: `extension migrations`
         :dedent: 0
         :start-after: # start-example
         :end-before: # end-example

Directory Structure
-------------------

Extension migrations are stored separately:

.. code-block:: text

   migrations/
   ├── 0001_initial.sql                    # Main migrations
   ├── 0002_add_users.sql
   └── (extension migrations stored in package)

   # Extension migrations location (in package):
   sqlspec/extensions/litestar/migrations/
   ├── 0001_create_litestar_metadata.sql
   └── 0002_add_request_logging.sql

Version Prefixes
----------------

Extension migrations are prefixed to avoid conflicts:

.. code-block:: text

   Main migrations:         0001, 0002, 0003
   Litestar migrations:     ext_litestar_0001, ext_litestar_0002
   Custom extension:        ext_myext_0001, ext_myext_0002

This allows each extension to maintain its own sequential numbering while
preventing version conflicts.

Commands
--------

Extension migrations are managed alongside main migrations:

.. code-block:: bash

   # Upgrade includes extension migrations
   sqlspec --config myapp.config upgrade

   # Show all migrations (including extensions)
   sqlspec --config myapp.config show-current-revision --verbose

Advanced Topics
===============

Out-of-Order Migrations
-----------------------

When migrations are created out of chronological order (e.g., from late-merging branches),
SQLSpec detects this and logs a warning:

.. code-block:: text

   WARNING: Out-of-order migration detected
   Migration 20251017100000_feature_a was created before
   already-applied migration 20251018090000_main_branch

   This can happen when:
   - A feature branch was created before a migration on main
   - Migrations from different branches are merged

The migration is still applied, and ``execution_sequence`` preserves the actual
application order for auditing.

Manual Version Reconciliation
------------------------------

If auto-sync is disabled, manually reconcile renamed migrations:

.. literalinclude:: /examples/usage/usage_migrations_10.py
         :language: python
         :caption: `manual version`
         :dedent: 0
         :start-after: # start-example
         :end-before: # end-example

Troubleshooting
===============

Migration Not Applied
---------------------

**Symptom:** Migration exists but isn't being applied.

**Checks:**

1. Verify migration file naming: ``{version}_{description}.sql``
2. Check query names: ``migrate-{version}-up`` and ``migrate-{version}-down``
3. Ensure version isn't already in tracking table:

   .. code-block:: bash

      sqlspec --config myapp.config show-current-revision --verbose

Version Mismatch After Fix
---------------------------

**Symptom:** After running ``fix``, database still shows old timestamp versions.

**Solution:** Ensure auto-sync is enabled (default):

.. code-block:: bash

   # Should auto-reconcile
   sqlspec --config myapp.config upgrade

   # Or manually run fix with database update
   sqlspec --config myapp.config fix  # (database update is default)

Schema Migration Fails
-----------------------

**Symptom:** Error adding columns to tracking table.

**Cause:** Usually insufficient permissions or incompatible database version.

**Solution:**

1. Ensure database user has ``ALTER TABLE`` permissions
2. Check database version compatibility
3. Manually add missing columns if needed:

   .. code-block:: sql

      ALTER TABLE ddl_migrations ADD COLUMN execution_sequence INTEGER;
      ALTER TABLE ddl_migrations ADD COLUMN version_type VARCHAR(16);
      ALTER TABLE ddl_migrations ADD COLUMN checksum VARCHAR(64);

Best Practices
==============

1. **Always Use Version Control**

   Commit migration files immediately after creation:

   .. code-block:: bash

      git add migrations/
      git commit -m "Add user authentication migration"

2. **Test Migrations Both Ways**

   Always test both upgrade and downgrade:

   .. code-block:: bash

      sqlspec --config myapp.config upgrade
      sqlspec --config myapp.config downgrade

3. **Use Dry Run in Production**

   Preview changes before applying:

   .. code-block:: bash

      sqlspec --config myapp.config upgrade --dry-run

4. **Backup Before Downgrade**

   Downgrades can cause data loss:

   .. code-block:: bash

      pg_dump mydb > backup_$(date +%Y%m%d_%H%M%S).sql
      sqlspec --config myapp.config downgrade

5. **Run Fix in CI**

   Automate timestamp → sequential conversion:

   .. code-block:: yaml

      # .github/workflows/migrations.yml
      - name: Convert timestamp migrations
        run: |
          sqlspec --config myapp.config fix --dry-run
          sqlspec --config myapp.config fix --yes

6. **Descriptive Migration Names**

   Use clear, action-oriented descriptions:

   .. code-block:: bash

      # Good
      sqlspec --config myapp.config create-migration -m "Add email index to users"

      # Bad
      sqlspec --config myapp.config create-migration -m "update users"

See Also
========

- :doc:`../usage/cli` - Complete CLI command reference
- :doc:`../usage/configuration` - Migration configuration options
