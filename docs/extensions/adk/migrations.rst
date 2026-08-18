==========
Migrations
==========

ADK stores use standard SQLSpec migrations. Generate migrations for the database
used by your ADK backend, then run them with the SQLSpec migration CLI.

Schema Bootstrapping
====================

You can programmatically create ADK session/event and memory tables with
``create_tables()`` / ``ensure_tables()``:

.. code-block:: python

   await session_store.ensure_tables()
   await memory_store.ensure_tables()

Alternatively, configure SQLSpec migrations on the database config and run the
migration CLI ahead of deployment:

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig

   config = AsyncpgConfig(
       connection_config={"dsn": "postgresql://localhost/app"},
       migration_config={"script_location": "migrations/postgres"},
   )

.. code-block:: console

   sqlspec upgrade

Use the code path when the store should keep its tables current. On startup,
``ensure_tables()`` checks the live tables. It creates missing tables and adds
new columns from the store DDL. It does not rename, drop, or change columns.

Control this behavior under ``extension_config["adk"]``:

.. code-block:: python

   config = AsyncpgConfig(
       connection_config={"dsn": "postgresql://localhost/app"},
       extension_config={
           "adk": {
               "manage_schema": True,
               "create_schema": True,
               "run_migrations": False,
           }
       },
   )

``manage_schema=False`` turns off all automatic table changes.
``create_schema=False`` lets the store add columns to current tables. It will
not create a missing table. ``run_migrations`` is for tools that supply a
versioned migration runner. That work stays separate from automatic checks.

For a release that only adds columns, update the adapter store DDL. You do not
need a schema seed or a numbered migration. Write a migration for renamed or
dropped columns, type changes, data backfills, and other complex work. Old
``schema_version`` rows remain readable, but they do not control the schema.

Use versioned migrations when your release process needs a ledger. Also use
them for changes that do more than add columns.

.. note::

   The migration CLI resolves configuration from ``--config``,
   ``SQLSPEC_CONFIG``, or ``[tool.sqlspec]`` in ``pyproject.toml``.

   When ``extension_config["adk"]`` is present, ADK extension migrations are
   auto-included. Use ``migration_config={"exclude_extensions": ["adk"]}``
   to skip only ADK extension migrations, or
   ``migration_config={"include_extensions": ["adk"]}`` to opt in explicitly
   by extension name. Use ``migration_config={"enabled": False}`` to disable
   migrations entirely for a given database config.

Feature Gating
==============

Your migration configuration decides whether ADK migrations run at all. Within
those migrations, ``enable_sessions`` and ``enable_memory`` are the only
per-feature switches:

.. code-block:: python

   config = AsyncpgConfig(
       connection_config={"dsn": "postgresql://localhost/app"},
       migration_config={"script_location": "migrations/postgres"},
       extension_config={"adk": {"enable_sessions": True, "enable_memory": False}},
   )

``enable_sessions=False`` suppresses the session, event, state, and metadata
DDL. ``enable_memory=False`` suppresses the memory table DDL and the PostgreSQL
vector extension statement described below. Both default to ``True``.

PostgreSQL pgvector Requirement
===============================

PostgreSQL memory tables store embeddings in a ``VECTOR`` column, which the
pgvector extension provides. When memory is enabled on PostgreSQL, migration
``0001_create_adk_tables`` runs ``CREATE EXTENSION IF NOT EXISTS vector``
immediately before the first statement that uses the type. The statement is
idempotent, so re-running the migration against a database that already has the
extension succeeds.

SQL can only enable an extension the server already ships. Two things must be
true before the migration runs:

- The pgvector server files are installed on the PostgreSQL host, so the
  extension is listed in ``pg_available_extensions``.
- The role running the migration is allowed to create the extension.

On managed database services, ask your DBA or platform team to pre-provision
pgvector through the provider's supported extension workflow rather than
granting broad privileges to the migration role. A pre-provisioned extension
costs nothing: the migration's ``IF NOT EXISTS`` statement becomes a no-op.

If either requirement is unmet, the migration fails on the ``CREATE EXTENSION``
statement with a clear permission or availability error instead of a later
``type "vector" does not exist`` error from the memory table DDL.

Automatic ``create_tables()`` / ``ensure_tables()`` reconciliation never
attempts extension installation, so it runs no repeated startup privilege
check. Deployments that rely on that path instead of versioned migrations must
pre-provision pgvector themselves.

Clean-Break Migration Notes
============================

If you are upgrading from a pre-clean-break version of the ADK extension,
note the following schema changes:

- **Events table**: The column layout changed to full-event JSON storage.
  Legacy pre-clean-break schemas used ``event_json`` for the event payload.
  The clean-break schema stores the full ADK Event in ``event_data`` alongside
  indexed scalar columns (``id``, ``app_name``, ``user_id``, ``session_id``,
  ``invocation_id``, ``timestamp``).
- **Artifact table**: New table (``adk_artifact``) for artifact
  metadata. Create this table when enabling the artifact service.
- **BigQuery**: Removed. Migrate to Spanner, PostgreSQL, or any other
  supported backend.

See :doc:`/usage/migrations` for the full workflow and commands.
