=======
Recipes
=======

Production patterns and integration examples from real-world projects. These go beyond
sqlspec's core API to show how it fits into larger application architectures.

Unlike the :doc:`usage guides </usage/index>`, which explain sqlspec's own features,
recipes show how to combine sqlspec with external libraries and common application
patterns.

.. grid:: 1 1 2 3
   :gutter: 2
   :padding: 0

   .. grid-item-card:: Dishka Dependency Injection
      :link: dishka
      :link-type: doc

      Request-scoped drivers and app-scoped configs with Dishka DI containers.

   .. grid-item-card:: Service Layer Pattern
      :link: service_layer
      :link-type: doc

      Base service classes with pagination, get-or-404, and transaction helpers.

   .. grid-item-card:: SQL Server
      :link: sql_server
      :link-type: doc

      Driver choice, pooling, GO batches, BulkCopy, migrations, ADK stores, the event queue, and Litestar on SQL Server.

   .. grid-item-card:: Multi-Tenancy
      :link: multi_tenancy
      :link-type: doc

      Tenant isolation patterns: discriminator columns, schema-per-tenant, and dynamic routing.

   .. grid-item-card:: Health Checks
      :link: health_checks
      :link-type: doc

      Liveness and readiness probes, pool teardown, and graceful shutdown.

.. toctree::
   :hidden:

   dishka
   service_layer
   sql_server
   multi_tenancy
   health_checks
