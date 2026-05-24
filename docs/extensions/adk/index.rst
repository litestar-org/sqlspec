====================
Google ADK Extension
====================

SQLSpec provides SQL-backed persistence for
`Google Agent Development Kit <https://google.github.io/adk-docs/>`_,
covering session, event, and memory storage across 15 backend packages. The
artifact service contracts are available for deployments that provide a
concrete artifact metadata store.

Key capabilities:

- **Session and event storage** with atomic ``append_event_and_update_state()``
  ensuring events and state are always consistent.
- **Full-event JSON storage** (EventRecord) that captures the entire ADK Event
  in a single column, eliminating schema drift with upstream ADK releases.
- **Scoped state semantics** (``app:``, ``user:``, ``temp:``) for controlling
  state visibility and persistence across sessions.
- **Memory service** with database-native full-text search (tsvector, FTS5,
  InnoDB FT) for long-term agent context.
- **Artifact service contracts** for append-only versioning, SQL metadata, and
  pluggable object storage backends.

Choose a guide
==============

.. grid:: 1 1 2 3
   :gutter: 2
   :padding: 0

   .. grid-item-card:: Installation
      :link: installation
      :link-type: doc

      Install the extension and configure the service.

   .. grid-item-card:: Quickstart
      :link: quickstart
      :link-type: doc

      Persist sessions and memory with minimal setup.

   .. grid-item-card:: Support Matrix
      :link: backends
      :link-type: doc

      See which backends are recommended, supported, or reduced-scope.

   .. grid-item-card:: Adapters
      :link: adapters
      :link-type: doc

      Configure supported SQLSpec adapters for ADK.

   .. grid-item-card:: Schema
      :link: schema
      :link-type: doc

      Table layouts, EventRecord, scoped state, and artifact metadata.

   .. grid-item-card:: Scoped State
      :link: scoped_state
      :link-type: doc

      App, user, session, and runtime-only state behavior.

   .. grid-item-card:: API Reference
      :link: api
      :link-type: doc

      Services, stores, and record types.

   .. grid-item-card:: Migrations
      :link: migrations
      :link-type: doc

      Apply schema changes safely over time.

.. toctree::
   :hidden:

   installation
   quickstart
   backends
   adapters
   schema
   scoped_state
   api
   migrations
