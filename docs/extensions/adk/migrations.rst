==========
Migrations
==========

ADK stores use standard SQLSpec migrations. Generate migrations for the database
used by your ADK backend, then run them with the SQLSpec migration CLI.

Schema Bootstrapping
====================

For development, use ``ensure_tables()`` to create tables on first use:

.. code-block:: python

   await session_store.ensure_tables()
   await memory_store.ensure_tables()
   await artifact_store.ensure_table()

For production, run migrations ahead of deployment to avoid runtime DDL.

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
