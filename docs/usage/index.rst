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

Overview
--------

SQLSpec provides a unified interface for database operations across multiple backends while
keeping SQL at the center. Start with configuration and drivers, then move into query
construction, migration tooling, and framework integrations. Extensions live here as part of
the usage flow so you can layer in the Litestar and ADK integrations when you are ready.

Quick Reference
---------------

**Connect and run a query**

.. literalinclude:: /examples/quickstart/basic_connection.py
   :language: python
   :caption: ``basic connection``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

**Build a query programmatically**

.. literalinclude:: /examples/builder/select_query.py
   :language: python
   :caption: ``select query``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

**Load SQL from files**

.. literalinclude:: /examples/sql_files/load_sql_files.py
   :language: python
   :caption: ``load sql files``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Next Steps
----------

Start with :doc:`configuration` for connection options, then review :doc:`drivers_and_querying`
for driver-specific guidance and execution patterns.

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
