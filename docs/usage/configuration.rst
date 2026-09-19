Configuration
=============

SQLSpec configuration is centered around adapter-specific config objects. Each config
captures connection parameters, optional pooling settings, and extension-specific
options for framework integrations.

Pure-Python installations load public exports on first access. Compiled wheels
retain eager exports to preserve concurrent access after package initialization.
Both builds preserve the same public objects and static types.

Core Configuration
------------------

.. literalinclude:: /examples/quickstart/configuration.py
   :language: python
   :caption: ``basic configuration``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Pooling and Connections
-----------------------

- Sync database configs expose ``provide_session()`` and ``provide_connection()`` context managers.
- Async database configs expose async context managers with the same method names.
- ``provide_session()`` yields a driver adapter instance ready for executing queries and managing transactions.
- ``provide_connection()`` yields the raw, underlying database connection from the driver.
- Configs supporting connection pooling implement ``create_pool()`` and ``provide_pool()``.
- On a ``SQLSpec`` registry instance, call ``spec.get_pool(config)`` to obtain the managed connection pool.

Extension Settings
------------------

Use ``extension_config`` to give each extension its own settings map. Shared
types live in ``sqlspec.config``. Import an adapter's Litestar, Events, or ADK
type when you need options that are specific to its tables or native transport.
Each adapter's :doc:`reference page </reference/adapters/index>` lists those types.
Plain dictionaries use the same keys.

.. list-table:: Extension settings
   :header-rows: 1

   * - Map key
     - Shared type
     - Guide
   * - ``litestar``
     - ``LitestarConfig``
     - :doc:`/reference/extensions/litestar`
   * - ``fastapi``
     - ``FastAPIConfig``
     - :doc:`/reference/extensions/fastapi`
   * - ``starlette``
     - ``StarletteConfig``
     - :doc:`/reference/extensions/starlette`
   * - ``flask``
     - ``FlaskConfig``
     - :doc:`/reference/extensions/flask`
   * - ``sanic``
     - ``SanicConfig``
     - :doc:`/reference/extensions/sanic`
   * - ``adk``
     - ``ADKConfig``
     - :doc:`/extensions/adk/schema`
   * - ``events``
     - ``EventsConfig``
     - :doc:`/reference/extensions/events`
   * - ``otel``
     - ``OpenTelemetryConfig``
     - :doc:`/reference/extensions/otel`
   * - ``prometheus``
     - ``PrometheusConfig``
     - :doc:`/reference/extensions/prometheus`

The other web frameworks, tracing, and metrics use shared settings.
Keep Litestar, Events, and ADK table tuning in the extension map.
Use ``manage_schema`` and
``create_schema`` for automatic table checks. Run versioned migrations through
the migration commands and ``migration_config``.

Configs can run queries before they build migration helpers. To check custom
tracker setup and find extension migrations at startup, call
``config.get_migration_commands()``. See :ref:`migration-startup-checks` for a
full example and the checks that still run when you create a config.

Multiple Databases
------------------

Register multiple configs on a single ``SQLSpec`` instance and use each config
handle independently. This pattern works for any combination of sync and async
adapters.

.. literalinclude:: /examples/configuration/multi_database.py
   :language: python
   :caption: ``multi-database with observability``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Key points:

- Each ``add_config()`` call returns the config handle you pass to ``provide_session()``.
- ``ObservabilityConfig`` on the ``SQLSpec`` instance applies to all registered configs.
- ``load_sql_files()`` accepts multiple paths and loads queries into a shared namespace.

Related Guides
--------------

- :doc:`drivers_and_querying` for driver-specific connection settings and execution patterns.
- :doc:`framework_integrations` for framework extension configuration.
- :doc:`/reference/adapters/index` for adapter-specific configuration reference.
- :doc:`/reference/config` for core configuration classes and options.
