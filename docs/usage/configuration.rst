=============
Configuration
=============

SQLSpec provides flexible configuration options for database connections, connection pooling, and statement processing. This guide covers everything you need to configure SQLSpec for production use.

Overview
--------

SQLSpec configuration is organized into three main areas:

1. **Database Configuration**: Connection parameters and pool settings
2. **Statement Configuration**: SQL processing pipeline behavior
3. **Cache Configuration**: Multi-tier caching system settings

Basic Configuration
-------------------

The simplest way to use SQLSpec is with default configuration:

.. literalinclude:: /examples/usage/usage_configuration_1.py
   :language: python
   :caption: `basic configuration`
   :lines: 2-14
   :dedent: 2

Database Configurations
-----------------------

Each database adapter has its own configuration class with adapter-specific settings.

.. note::

   Async PostgreSQL examples in this guide read their connection details from
   ``SQLSPEC_USAGE_PG_*`` environment variables. The test suite populates these
   variables (host, port, user, password, database, DSN) automatically so the
   literalincluded snippets can stay identical to the documentation.

SQLite Configuration
^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/usage_configuration_2.py
   :language: python
   :caption: `sqlite configuration`
   :lines: 2-11
   :dedent: 2

**Memory Databases**

.. literalinclude:: /examples/usage/usage_configuration_3.py
   :language: python
   :caption: `memory sqlite configuration`
   :lines: 2-13
   :dedent: 2

PostgreSQL Configuration (asyncpg)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/usage_configuration_4.py
   :language: python
   :caption: `postgres asyncpg configuration`
   :lines: 2-16
   :dedent: 2

PostgreSQL Configuration (psycopg)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/usage_configuration_5.py
   :language: python
   :caption: `psycopg async configuration`
   :lines: 2-19
   :dedent: 2

MySQL Configuration (asyncmy)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/usage_configuration_6.py
   :language: python
   :caption: `mysql asyncmy configuration`
   :lines: 2-15
   :dedent: 2

DuckDB Configuration
^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/usage_configuration_7.py
   :language: python
   :caption: `duckdb configuration`
   :lines: 2-11
   :dedent: 2

Connection Pooling
------------------

Connection pooling improves performance by reusing database connections. SQLSpec provides built-in pooling for most adapters.

Pool Configuration
^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/usage_configuration_8.py
   :language: python
   :caption: `pool configuration`
   :lines: 2-11
   :dedent: 2

**Pool Lifecycle Management**

.. literalinclude:: /examples/usage/usage_configuration_9.py
   :language: python
   :caption: `pool lifecycle management`
   :lines: 2-7
   :dedent: 2

Using Pre-Created Pools
^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/usage_configuration_10.py
   :language: python
   :caption: `using pre-created pools`
   :lines: 2-9
   :dedent: 2

No-Pooling Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/usage_configuration_11.py
   :language: python
   :caption: `no-pooling configuration`
   :lines: 2-4
   :dedent: 2

Statement Configuration
-----------------------

Statement configuration controls SQL processing pipeline behavior.

Basic Statement Config
^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/usage_configuration_12.py
   :language: python
   :caption: `basic statement config`
   :lines: 2-14
   :dedent: 2

Parameter Style Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/usage_configuration_13.py
   :language: python
   :caption: `parameter style configuration`
   :lines: 2-17
   :dedent: 2

**Parameter Styles**

SQLSpec supports multiple parameter placeholder styles:

.. literalinclude:: /examples/usage/usage_configuration_14.py
   :language: python
   :caption: `parameter styles`
   :lines: 2-21
   :dedent: 2

Validation Configuration
^^^^^^^^^^^^^^^^^^^^^^^^

Configure security and performance validation.

Disable validation for performance-critical paths where input is trusted:

.. literalinclude:: /examples/usage/usage_configuration_15.py
   :language: python
   :caption: `validation configuration`
   :lines: 5-17
   :dedent: 2

Cache Configuration
-------------------

SQLSpec uses multi-tier caching to avoid recompiling SQL statements.

Global Cache Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/usage_configuration_16.py
   :language: python
   :caption: `global cache configuration`
   :lines: 6-28
   :dedent: 2

Per-Instance Cache Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/usage_configuration_17.py
   :language: python
   :caption: `per-instance cache configuration`
   :lines: 6-27
   :dedent: 2

Cache Statistics
^^^^^^^^^^^^^^^^

Monitor cache statistics:

.. literalinclude:: /examples/usage/usage_configuration_18.py
   :language: python
   :caption: `cache statistics`
   :lines: 6-18
   :dedent: 2

Clear Cache
^^^^^^^^^^^

.. literalinclude:: /examples/usage/usage_configuration_19.py
   :language: python
   :caption: `clear cache`
   :lines: 6-24
   :dedent: 2

Multiple Database Configurations
---------------------------------

SQLSpec supports multiple database configurations in a single application.

Binding Multiple Configs
^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/usage_configuration_19.py
   :language: python
   :caption: `binding multiple configurations`
   :lines: 4-27
   :dedent: 2

Named Bindings
^^^^^^^^^^^^^^

Use bind keys for clearer configuration management:

.. literalinclude:: /examples/usage/usage_configuration_20.py
   :language: python
   :caption: `named bindings`
   :lines: 6-25
   :dedent: 2

``SQLSpec.add_config()`` returns a key for the registered configuration.
Store that value (as shown above) and reuse it when calling
``provide_session()`` or ``get_config()``. The config registry holds a single
instance per config type, so creating multiple variants of the same adapter
requires defining lightweight subclasses or binding unique config classes for
each database.

Migration Configuration
-----------------------

SQLSpec includes a migration system for schema management.

Basic Migration Config
^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/usage_configuration_22.py
   :language: python
   :caption: `basic migration config`
   :lines: 2-15
   :dedent: 2

**Migration CLI**

.. code-block:: bash

    # Create migration
    sqlspec --config myapp.config create-migration -m "Add users table"

    # Apply migrations
    sqlspec --config myapp.config upgrade

    # Rollback
    sqlspec --config myapp.config downgrade -1



Extension Migration Versioning
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Extension migrations are automatically prefixed to prevent version collisions with user migrations:

.. code-block:: text

   # User migrations
   0001_initial.py           → version: 0001
   0002_add_users.py         → version: 0002

   # Extension migrations (automatic prefix)
   ext_adk_0001              → ADK tables migration
   ext_litestar_0001         → Litestar session table migration

This ensures extension migrations never conflict with your application migrations in the version tracking table.

Extension Configuration
-----------------------

Framework integrations can be configured via ``extension_config``.

Litestar Plugin Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/usage_configuration_23.py
   :language: python
   :caption: `litestar plugin configuration`
   :lines: 4-28
   :dedent: 2

Telemetry Snapshot
~~~~~~~~~~~~~~~~~~

Call ``SQLSpec.telemetry_snapshot()`` to inspect lifecycle counters, serializer metrics, and recent storage jobs:

.. literalinclude:: /examples/usage/usage_configuration_30.py
   :language: python
   :caption: `telemetry snapshot`
   :lines: 1-15
   :dedent: 0

Environment-Based Configuration
-------------------------------

Use environment variables for configuration:

.. literalinclude:: /examples/usage/usage_configuration_24.py
   :language: python
   :caption: `environment-based configuration`
   :lines: 25-41
   :dedent: 4

Configuration Best Practices
-----------------------------

**1. Use Connection Pooling**

Always use pooling in production:

.. literalinclude:: /examples/usage/usage_configuration_25.py
   :language: python
   :caption: `connection pooling`
   :lines: 11-13
   :dedent: 2

**2. Enable Caching**

Enable caching to avoid recompiling SQL statements:

.. literalinclude:: /examples/usage/usage_configuration_26.py
   :language: python
   :caption: `enable caching`
   :lines: 6-8
   :dedent: 2

**3. Tune Pool Sizes**

Size pools based on your workload:

.. literalinclude:: /examples/usage/usage_configuration_27.py
   :language: python
   :caption: `tune pool sizes`
   :lines: 3-20
   :dedent: 2

**4. Disable Validation in Production**

For trusted, performance-critical queries:

.. literalinclude:: /examples/usage/usage_configuration_28.py
   :language: python
   :caption: `no validation`
   :lines: 6-12
   :dedent: 2


**5. Clean Up Resources**

Always close pools on shutdown:

.. literalinclude:: /examples/usage/usage_configuration_29.py
   :language: python
   :caption: `cleanup resources`
   :lines: 10-27
   :dedent: 2

Next Steps
----------

Now that you understand configuration:

- :doc:`drivers_and_querying` - Execute queries with your configured databases
- :doc:`framework_integrations` - Integrate with web frameworks
- :doc:`../reference/adapters` - Detailed adapter reference

See Also
--------

- :doc:`../reference/base` - SQLSpec base class API
- :doc:`../reference/core` - Core configuration classes
- :doc:`data_flow` - Understanding the execution pipeline
