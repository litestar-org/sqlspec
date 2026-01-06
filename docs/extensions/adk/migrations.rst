==========
Migrations
==========

This guide covers database migration strategies for the ADK extension tables.

Overview
========

The ADK extension provides two primary ways to manage database schema:

1. **Direct Table Creation** - Use ``store.create_tables()`` for simple deployments
2. **Migration System** - Use SQLSpec's migration system for production deployments

Direct Table Creation
=====================

The simplest approach for development and small deployments:

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore

   config = AsyncpgConfig(connection_config={"dsn": "postgresql://..."})
   store = AsyncpgADKStore(config)

   # Create tables if they don't exist
   await store.create_tables()

This method:

- Creates tables with ``CREATE TABLE IF NOT EXISTS``
- Creates all indexes
- Is idempotent (safe to call multiple times)
- Suitable for development and testing

Using SQLSpec Migration System
===============================

For production deployments, use SQLSpec's built-in migration system to track schema changes.

Setting Up Migrations
----------------------

**1. Configure Migration Settings:**

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig

   config = AsyncpgConfig(
       connection_config={"dsn": "postgresql://..."},
       extension_config={
           "adk": {
               "session_table": "adk_sessions",
               "events_table": "adk_events",
               "memory_table": "adk_memory_entries",
               "memory_use_fts": True,
               "owner_id_column": "account_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE"
           }
       },
       migration_config={
           "script_location": "migrations",
           "include_extensions": ["adk"]
       }
   )

.. note::

   **Version Prefixing**: ADK migrations are automatically prefixed with ``ext_adk_``
   to prevent version conflicts. For example, ``0001_create_adk_tables.py`` becomes
   ``ext_adk_0001`` in the database tracking table (``ddl_migrations``).

.. note::

   **Owner ID Column Support**: The migration system automatically includes the
   ``owner_id_column`` configuration when creating tables. The column is added to
   the sessions table DDL if specified in ``extension_config["adk"]["owner_id_column"]``.

.. note::

   **Memory Tables**: ``ext_adk_0001`` also creates the memory table when
   ``enable_memory`` (default) or ``include_memory_migration`` is set to ``True``.
   Set ``include_memory_migration=False`` to skip memory DDL while keeping the
   runtime memory service enabled.

**2. Initialize Migration Directory:**

.. code-block:: bash

   # Using SQLSpec CLI
   sqlspec --config myapp.config init

**3. Generate Initial Migration:**

.. code-block:: bash

   sqlspec --config myapp.config create-migration -m "Create ADK tables"

This creates a migration file in ``migrations/versions/``.

**4. Edit Migration File:**

.. code-block:: python

   """Create ADK tables

   Revision ID: 0001_create_adk_tables
   Revises: None
   Create Date: 2025-10-06 14:00:00.000000
   """

   from sqlspec.migrations import Migration


   def upgrade(migration: Migration) -> None:
       """Create ADK sessions and events tables."""
       # Get store instance
       from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore

       store = AsyncpgADKStore(migration.config)

       # Create sessions table
       migration.execute(store._get_create_sessions_table_sql())

       # Create events table
       migration.execute(store._get_create_events_table_sql())


   def downgrade(migration: Migration) -> None:
       """Drop ADK sessions and events tables."""
       from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore

       store = AsyncpgADKStore(migration.config)

       # Drop tables (order matters: events before sessions)
       for sql in store._get_drop_tables_sql():
           migration.execute(sql)

**5. Run Migration:**

.. code-block:: bash

   # Apply migration
   sqlspec --config myapp.config upgrade

   # Rollback migration
   sqlspec --config myapp.config downgrade -1

Built-In Migration Template
============================

SQLSpec includes a built-in migration for ADK tables:

.. code-block:: python

   from sqlspec.extensions.adk.migrations import create_adk_tables_migration

Location: ``sqlspec/extensions/adk/migrations/``

You can copy this template for custom migrations:

.. code-block:: python

   """Create ADK tables migration template."""

   from typing import TYPE_CHECKING

   if TYPE_CHECKING:
       from sqlspec.migrations.revision import Migration


   def upgrade(migration: "Migration") -> None:
       """Create ADK sessions and events tables.

       This migration creates the base schema for Google ADK session
       storage with the configured table names.
       """
       from sqlspec.extensions.adk.store import BaseAsyncADKStore

       config = migration.config
       extension_config = config.extension_config.get("adk", {})

       session_table = extension_config.get("session_table", "adk_sessions")
       events_table = extension_config.get("events_table", "adk_events")

       # Import correct store based on adapter
       adapter_name = config.__class__.__module__.split(".")[2]

       if adapter_name == "asyncpg":
           from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore as Store
       elif adapter_name == "asyncmy":
           from sqlspec.adapters.asyncmy.adk import AsyncmyADKStore as Store
       elif adapter_name == "sqlite":
           from sqlspec.adapters.sqlite.adk import SqliteADKStore as Store
       # Add other adapters as needed
       else:
           msg = f"Unsupported adapter: {adapter_name}"
           raise ValueError(msg)

       store = Store(config, session_table, events_table)

       # Create tables
       migration.execute(store._get_create_sessions_table_sql())
       migration.execute(store._get_create_events_table_sql())


   def downgrade(migration: "Migration") -> None:
       """Drop ADK sessions and events tables."""
       # Similar logic but call _get_drop_tables_sql()
       pass

Custom Table Names in Migrations
=================================

Configure custom table names via ``extension_config``:

.. code-block:: python

   config = AsyncpgConfig(
       connection_config={"dsn": "postgresql://..."},
       extension_config={
           "adk": {
               "session_table": "my_custom_sessions",
               "events_table": "my_custom_events"
           }
       },
       migration_config={
           "include_extensions": ["adk"]
       }
   )

The migration system reads these settings and creates tables with custom names.

.. warning::

   **Configuration Location**: Extension settings must be in ``extension_config``,
   NOT in ``migration_config``. The following is INCORRECT:

   .. code-block:: python

      # ❌ WRONG - Don't put extension settings in migration_config
      migration_config={
          "include_extensions": [
              {"name": "adk", "session_table": "custom"}  # NO LONGER SUPPORTED
          ]
      }

      # ✅ CORRECT - Use extension_config
      extension_config={
          "adk": {"session_table": "custom"}
      },
      migration_config={
          "include_extensions": ["adk"]  # Simple string only
      }

Owner ID Column in Migrations
=============================

To include a owner ID column in your ADK tables, configure it in ``extension_config``:

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig

   config = AsyncpgConfig(
       connection_config={"dsn": "postgresql://..."},
       extension_config={
           "adk": {
               "session_table": "adk_sessions",
               "events_table": "adk_events",
               "owner_id_column": "account_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE"
           }
       },
       migration_config={
           "script_location": "migrations",
           "include_extensions": ["adk"]
       }
   )

The migration will automatically create the sessions table with the owner ID column.

Prerequisites
-------------

Ensure the referenced table exists **before** running the ADK migration:

.. code-block:: python

   """Create users table migration."""

   async def up(context):
       """Create users table."""
       return ["""
           CREATE TABLE users (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               email VARCHAR(255) NOT NULL UNIQUE,
               created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
           )
       """]

   async def down(context):
       """Drop users table."""
       return ["DROP TABLE IF EXISTS users CASCADE"]

Run this migration **before** the ADK migration to ensure the foreign key reference is valid.

Migration Order
---------------

When using owner ID columns, ensure migrations run in this order:

1. Create referenced table (e.g., ``users``, ``tenants``)
2. Create ADK tables with FK column (``ext_adk_0001``)
3. Any subsequent schema changes

.. code-block:: bash

   # Example migration sequence
   sqlspec --config myapp.config upgrade

   # Migrations applied:
   # 1. 0001_create_users
   # 2. ext_adk_0001_create_adk_tables  (with owner ID column)

Database-Specific Examples
---------------------------

PostgreSQL with UUID FK:

.. code-block:: python

   extension_config={
       "adk": {
           "owner_id_column": "account_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE"
       }
   }

MySQL with BIGINT FK:

.. code-block:: python

   extension_config={
       "adk": {
           "owner_id_column": "user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE"
       }
   }

SQLite with INTEGER FK:

.. code-block:: python

   extension_config={
       "adk": {
           "owner_id_column": "tenant_id INTEGER NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"
       }
   }

Oracle with NUMBER FK:

.. code-block:: python

   extension_config={
       "adk": {
           "owner_id_column": "user_id NUMBER(10) REFERENCES users(id) ON DELETE CASCADE"
       }
   }

Multi-Tenant Migrations
========================

For multi-tenant applications, create separate migrations per tenant:

.. code-block:: python

   # Tenant A config
   config_a = AsyncpgConfig(
       connection_config={"dsn": "postgresql://..."},
       extension_config={
           "adk": {
               "session_table": "tenant_a_sessions",
               "events_table": "tenant_a_events"
           }
       }
   )

   # Tenant B config
   config_b = AsyncpgConfig(
       connection_config={"dsn": "postgresql://..."},
       extension_config={
           "adk": {
               "session_table": "tenant_b_sessions",
               "events_table": "tenant_b_events"
           }
       }
   )

Or use a single database with schema separation (PostgreSQL):

.. code-block:: python

   config_a = AsyncpgConfig(
       connection_config={"dsn": "postgresql://..."},
       extension_config={
           "adk": {
               "session_table": "tenant_a.sessions",
               "events_table": "tenant_a.events"
           }
       }
   )

Schema Evolution
================

Common schema changes and how to handle them:

Adding a Column
---------------

.. code-block:: python

   def upgrade(migration: Migration) -> None:
       """Add priority column to sessions."""
       migration.execute("""
           ALTER TABLE adk_sessions
           ADD COLUMN priority INTEGER DEFAULT 0
       """)

   def downgrade(migration: Migration) -> None:
       """Remove priority column."""
       migration.execute("""
           ALTER TABLE adk_sessions
           DROP COLUMN priority
       """)

Adding an Index
---------------

.. code-block:: python

   def upgrade(migration: Migration) -> None:
       """Add index on session state."""
       migration.execute("""
           CREATE INDEX idx_adk_sessions_priority
           ON adk_sessions(priority DESC)
       """)

   def downgrade(migration: Migration) -> None:
       """Drop priority index."""
       migration.execute("""
           DROP INDEX IF EXISTS idx_adk_sessions_priority
       """)

Renaming a Table
----------------

.. code-block:: python

   def upgrade(migration: Migration) -> None:
       """Rename sessions table."""
       migration.execute("""
           ALTER TABLE adk_sessions
           RENAME TO agent_sessions
       """)

       # Update foreign key reference
       migration.execute("""
           ALTER TABLE adk_events
           DROP CONSTRAINT adk_events_session_id_fkey,
           ADD CONSTRAINT adk_events_session_id_fkey
               FOREIGN KEY (session_id)
               REFERENCES agent_sessions(id)
               ON DELETE CASCADE
       """)

   def downgrade(migration: Migration) -> None:
       """Revert table rename."""
       # Reverse operations
       pass

Data Migration
==============

Migrating data between different schema versions:

.. code-block:: python

   def upgrade(migration: Migration) -> None:
       """Migrate state format from v1 to v2."""
       # Add new column
       migration.execute("""
           ALTER TABLE adk_sessions
           ADD COLUMN state_v2 JSONB
       """)

       # Migrate data
       migration.execute("""
           UPDATE adk_sessions
           SET state_v2 = state || '{"version": 2}'::jsonb
       """)

       # Drop old column
       migration.execute("""
           ALTER TABLE adk_sessions
           DROP COLUMN state
       """)

       # Rename new column
       migration.execute("""
           ALTER TABLE adk_sessions
           RENAME COLUMN state_v2 TO state
       """)

Zero-Downtime Migrations
========================

For production systems, use blue-green or rolling migrations:

**Step 1: Add New Column (Backward Compatible):**

.. code-block:: python

   def upgrade(migration: Migration) -> None:
       """Add new_field column (nullable)."""
       migration.execute("""
           ALTER TABLE adk_sessions
           ADD COLUMN new_field TEXT
       """)

**Step 2: Dual-Write Phase:**

Update application code to write to both old and new fields.

**Step 3: Backfill Data:**

.. code-block:: python

   def upgrade(migration: Migration) -> None:
       """Backfill new_field from old_field."""
       migration.execute("""
           UPDATE adk_sessions
           SET new_field = old_field
           WHERE new_field IS NULL
       """)

**Step 4: Make Non-Nullable:**

.. code-block:: python

   def upgrade(migration: Migration) -> None:
       """Make new_field non-nullable."""
       migration.execute("""
           ALTER TABLE adk_sessions
           ALTER COLUMN new_field SET NOT NULL
       """)

**Step 5: Remove Old Column:**

.. code-block:: python

   def upgrade(migration: Migration) -> None:
       """Drop old_field column."""
       migration.execute("""
           ALTER TABLE adk_sessions
           DROP COLUMN old_field
       """)

Testing Migrations
==================

Test migrations in a staging environment:

.. code-block:: python

   import pytest
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.migrations import MigrationRunner


   @pytest.fixture
   async def migration_config():
       """Test database configuration."""
       return AsyncpgConfig(
           connection_config={"dsn": "postgresql://localhost/test_db"}
       )


   async def test_migration_up_down(migration_config):
       """Test migration applies and reverts cleanly."""
       runner = MigrationRunner(migration_config)

       # Apply migration
       await runner.upgrade("head")

       # Verify tables exist
       # ... table existence checks ...

       # Rollback migration
       await runner.downgrade("base")

       # Verify tables don't exist
       # ... table non-existence checks ...

Best Practices
==============

1. **Always Test Migrations**

   - Test in staging before production
   - Test both upgrade and downgrade
   - Verify data integrity after migration

2. **Use Transactions**

   - SQLSpec migrations run in transactions by default
   - Ensure DDL is transactional (PostgreSQL yes, MySQL no)

3. **Backup Before Migrating**

   - Take database backup before major migrations
   - Test restoration procedure

4. **Version Control Migrations**

   - Commit migration files to git
   - Never modify applied migrations
   - Create new migrations for changes

5. **Document Breaking Changes**

   - Add comments explaining complex migrations
   - Document manual steps if needed
   - Note performance implications

Troubleshooting
===============

Migration Fails Mid-Way
-----------------------

PostgreSQL automatically rolls back failed migrations. For MySQL:

.. code-block:: bash

   # Manually revert
   sqlspec --config myapp.config downgrade -1

Table Already Exists
--------------------

Use ``IF EXISTS`` / ``IF NOT EXISTS`` clauses:

.. code-block:: python

   migration.execute("""
       CREATE TABLE IF NOT EXISTS adk_sessions (...)
   """)

Foreign Key Constraint Violation
---------------------------------

Ensure proper order when dropping tables:

.. code-block:: python

   # Drop child table first (events), then parent (sessions)
   migration.execute("DROP TABLE IF EXISTS adk_events")
   migration.execute("DROP TABLE IF EXISTS adk_sessions")

See Also
========

- :doc:`schema` - Complete schema reference
- :doc:`adapters` - Database-specific DDL
- :doc:`/reference/migrations` - SQLSpec migrations reference
- :doc:`/examples/extensions/adk/basic_aiosqlite` - Example with table creation
- :doc:`/examples/extensions/adk/litestar_aiosqlite` - Litestar example showing runtime initialization
