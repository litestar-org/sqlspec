==========
Migrations
==========

ADK stores use standard SQLSpec migrations. Generate migrations for the database
used by your ADK backend, then run them with the SQLSpec migration CLI.

Schema Bootstrapping
====================

You can programmatically create ADK tables with ``create_tables()`` /
``ensure_tables()``:

.. code-block:: python

   await session_store.ensure_tables()
   await memory_store.ensure_tables()
   await artifact_store.ensure_table()

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

Use the programmatic table-creation path when you want the store to bootstrap
its own schema. Use migrations when you want schema changes tracked and applied
through your deployment workflow.

.. note::

   The migration CLI resolves configuration from ``--config``,
   ``SQLSPEC_CONFIG``, or ``[tool.sqlspec]`` in ``pyproject.toml``.

   When ``extension_config["adk"]`` is present, ADK extension migrations are
   auto-included. Use ``migration_config={"exclude_extensions": ["adk"]}``
   to skip only ADK extension migrations, or
   ``migration_config={"include_extensions": ["adk"]}`` to opt in explicitly
   by extension name. Use ``migration_config={"enabled": False}`` to disable
   migrations entirely for a given database config.

Clean-Break Migration Notes
============================

If you are upgrading from a pre-clean-break version of the ADK extension,
note the following schema changes:

- **Events table**: The column layout changed to full-event JSON storage.
  The ``event_json`` column now stores the entire ADK Event as a JSON blob.
  Individual event columns (``content``, ``actions``, ``branch``, etc.) have
  been replaced by indexed scalar columns (``invocation_id``, ``author``,
  ``timestamp``) plus ``event_json``.
- **Artifact table**: New table (``adk_artifact_versions``) for artifact
  metadata. Create this table when enabling the artifact service.
- **BigQuery**: Removed. Migrate to PostgreSQL or any other supported backend.

See :doc:`/usage/migrations` for the full workflow and commands.
