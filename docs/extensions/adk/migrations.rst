==========
Migrations
==========

ADK stores use standard SQLSpec migrations. Generate migrations for the database
used by your ADK backend, then run them with the SQLSpec migration CLI.

Schema Bootstrapping
====================

You can programmatically create ADK session/event/scoped-state/metadata and
memory tables with
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

Programmatic Cutover with ``recreate_tables()``
================================================

The base store exposes ``await store.recreate_tables()``, which drops every
ADK-managed table in FK-safe order and recreates them from the current DDL.
This is the supported in-place cutover for deployments that own the database
end-to-end and can tolerate a full ADK schema reset:

.. code-block:: python

   await store.recreate_tables()

``recreate_tables()`` does not touch the SQLSpec migrations runner state, so
the next ``sqlspec upgrade`` run still sees the unchanged migration history.
Use ``await store.drop_tables()`` if you need to remove the schema without
rebuilding it.

Clean-Break Migration Notes
============================

If you are upgrading from a pre-clean-break version of the ADK extension,
note the following schema changes:

- **Events table**: The column layout changed to full-event JSON storage.
  The ``event_data`` column now stores the entire ADK Event as a JSON blob.
  Individual event columns (``content``, ``actions``, ``branch``, etc.) have
  been replaced by indexed scalar columns (``invocation_id``, ``author``,
  ``timestamp``) plus ``event_data``.
- **Scoped state tables**: New ``adk_app_state`` and ``adk_user_state`` tables
  store ``app:`` and ``user:`` scoped keys. Raw ``adk_session.state`` rows now
  contain only session-scoped keys; ``SQLSpecSessionService.get_session()``
  returns the merged ADK view.
- **Internal metadata table**: New ``adk_internal_metadata`` table seeded with
  ``schema_version = 1``.
- **Artifact table**: New table (``adk_artifact_versions``) for artifact
  metadata. Create this table when enabling the artifact service.
- **BigQuery**: Treated as an analytics-replica backend. Use Spanner or a
  PostgreSQL-family adapter for latency-sensitive live session state.

See :doc:`/usage/migrations` for the full workflow and commands.
