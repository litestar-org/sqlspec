=====
Usage
=====

This section provides focused guides on core SQLSpec workflows. Each page highlights the
minimum you need to move fast, with links to deeper examples when you want more detail.

Choose a topic
==============

.. grid:: 1 1 2 3
   :gutter: 2
   :padding: 0

   .. grid-item-card:: Data Flow
      :link: data_flow
      :link-type: doc

      How SQLSpec sessions, drivers, and results flow together.

   .. grid-item-card:: Configuration
      :link: configuration
      :link-type: doc

      Configure adapters, sessions, and registry options.

   .. grid-item-card:: Drivers & Querying
      :link: drivers_and_querying
      :link-type: doc

      Execute statements, bind parameters, and batch work.

   .. grid-item-card:: Query Builder
      :link: query_builder
      :link-type: doc

      Build safe SQL using the fluent builder.

   .. grid-item-card:: SQL Files
      :link: sql_files
      :link-type: doc

      Load, organize, and reuse SQL from files.

   .. grid-item-card:: CLI
      :link: cli
      :link-type: doc

      Manage migrations and automation from the terminal.

   .. grid-item-card:: Migrations
      :link: migrations
      :link-type: doc

      Apply schema changes with consistent workflows.

   .. grid-item-card:: Framework Integrations
      :link: framework_integrations
      :link-type: doc

      Plug SQLSpec into Litestar, FastAPI, Flask, or Starlette.

   .. grid-item-card:: Observability
      :link: observability
      :link-type: doc

      Trace queries, sample traffic, and log structured events.

   .. grid-item-card:: Extensions
      :link: ../extensions/index
      :link-type: doc

      Add service-specific helpers like the ADK extension.

Recommended Path
----------------

1. **Start with** :doc:`data_flow` to understand how sessions, drivers, and results connect.
2. **Configure your database** in :doc:`configuration` with adapter-specific settings.
3. **Execute queries** using :doc:`drivers_and_querying` for transaction and parameter patterns.
4. **Build queries safely** with the :doc:`query_builder` for programmatic SQL construction.
5. **Organize SQL** in files using :doc:`sql_files` when your project grows.
6. **Integrate with your framework** via :doc:`framework_integrations` for Litestar, FastAPI, Flask, or Starlette.

.. toctree::
   :hidden:

   data_flow
   configuration
   drivers_and_querying
   query_builder
   sql_files
   cli
   migrations
   framework_integrations
   observability
   ../extensions/index
