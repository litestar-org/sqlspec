====================
Google ADK Extension
====================

SQLSpec provides a full-featured backend for
`Google Agent Development Kit <https://google.github.io/adk-docs/>`_,
covering session, event, memory, and artifact storage with SQL-backed
persistence across 14 database adapters.

Key capabilities:

- **Session and event storage** with atomic ``append_event_and_update_state()``
  ensuring events and state are always consistent.
- **Full-event JSON storage** (EventRecord) that captures the entire ADK Event
  in a single column, eliminating schema drift with upstream ADK releases.
- **Scoped state semantics** (``app:``, ``user:``, ``temp:``) for controlling
  state visibility and persistence across sessions.
- **Memory service** with database-native full-text search (tsvector, FTS5,
  InnoDB FT) for long-term agent context.
- **Artifact service** with append-only versioning, SQL metadata, and pluggable
  object storage backends.

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

      Persist sessions, memory, and artifacts with minimal setup.

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
   api
   migrations
