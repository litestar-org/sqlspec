Configuration
=============

SQLSpec configuration is centered around adapter-specific config objects. Each config
captures connection parameters, optional pooling settings, and extension-specific
options for framework integrations.

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

- Sync adapters expose ``provide_session()`` and ``provide_connection()`` context managers.
- Async adapters expose async context managers with the same method names.
- Drivers with pooling support provide ``create_pool()`` and ``get_pool()`` helpers.

Extension Settings
------------------

Use ``extension_config`` to pass framework- or extension-specific settings. Each
extension documents its available keys. Example keys include session key names,
commit mode, correlation middleware, and migrations toggles.

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

- :doc:`drivers_and_querying` for driver-specific connection settings.
- :doc:`framework_integrations` for framework extension configuration.
- :doc:`../reference/adapters` for adapter-specific configuration reference.
