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

.. literalinclude:: /examples/usage/test_configuration_1.py
   :language: python
   :caption: `basic configuration`
   :lines: 2-14
   :dedent: 2

Database Configurations
-----------------------

Each database adapter has its own configuration class with adapter-specific settings.

SQLite Configuration
^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_2.py
   :language: python
   :caption: `sqlite configuration`
   :lines: 2-11
   :dedent: 2

**Memory Databases**

.. literalinclude:: /examples/usage/test_configuration_3.py
   :language: python
   :caption: `memory sqlite configuration`
   :lines: 2-13
   :dedent: 2

PostgreSQL Configuration (asyncpg)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_4.py
   :language: python
   :caption: `postgres asyncpg configuration`
   :lines: 2-16
   :dedent: 2

PostgreSQL Configuration (psycopg)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

   The `PsycopgConfig` class referenced here is for documentation purposes only and may not be present in the codebase. Future releases may include this feature if demand warrants.


MySQL Configuration (asyncmy)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_6.py
   :language: python
   :caption: `mysql asyncmy configuration`
   :lines: 2-15
   :dedent: 2

DuckDB Configuration
^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_7.py
   :language: python
   :caption: `duckdb configuration`
   :lines: 2-11
   :dedent: 2

Connection Pooling
------------------

Connection pooling improves performance by reusing database connections. SQLSpec provides built-in pooling for most adapters.

Pool Configuration
^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_8.py
   :language: python
   :caption: `pool configuration`
   :lines: 2-11
   :dedent: 2

**Pool Lifecycle Management**

.. literalinclude:: /examples/usage/test_configuration_9.py
   :language: python
   :caption: `pool lifecycle management`
   :lines: 2-7
   :dedent: 2

Using Pre-Created Pools
^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_10.py
   :language: python
   :caption: `using pre-created pools`
   :lines: 2-9
   :dedent: 2

No-Pooling Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_11.py
   :language: python
   :caption: `no-pooling configuration`
   :lines: 2-4
   :dedent: 2

Statement Configuration
-----------------------

Statement configuration controls SQL processing pipeline behavior.

Basic Statement Config
^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_12.py
   :language: python
   :caption: `basic statement config`
   :lines: 2-16
   :dedent: 2

Parameter Style Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_13.py
   :language: python
   :caption: `parameter style configuration`
   :lines: 2-17
   :dedent: 2

**Parameter Styles**

SQLSpec supports multiple parameter placeholder styles:

.. literalinclude:: /examples/usage/test_configuration_14.py
   :language: python
   :caption: `parameter styles`
   :lines: 2-21
   :dedent: 2

Validation Configuration
^^^^^^^^^^^^^^^^^^^^^^^^

Configure security and performance validation.

Disable validation for performance-critical paths where input is trusted:

.. literalinclude:: /examples/usage/test_configuration_15.py
   :language: python
   :caption: `validation configuration`
   :lines: 5-17
   :dedent: 2

Cache Configuration
-------------------

SQLSpec uses multi-tier caching to avoid recompiling SQL statements.

Global Cache Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_16.py
   :language: python
   :caption: `global cache configuration`
   :lines: 6-28
   :dedent: 2

Per-Instance Cache Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_17.py
   :language: python
   :caption: `per-instance cache configuration`
   :lines: 6-27
   :dedent: 2

Cache Statistics
^^^^^^^^^^^^^^^^

Monitor cache statistics:

.. literalinclude:: /examples/usage/test_configuration_18.py
   :language: python
   :caption: `cache statistics`
   :lines: 6-18
   :dedent: 2

Clear Cache
^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_19.py
   :language: python
   :caption: `clear cache`
   :lines: 6-24
   :dedent: 2

Multiple Database Configurations
---------------------------------

SQLSpec supports multiple database configurations in a single application.

Binding Multiple Configs
^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_20.py
   :language: python
   :caption: `binding multiple configurations`
   :lines: 6-24
   :dedent: 2

Named Bindings
^^^^^^^^^^^^^^

Use bind keys for clearer configuration management:

.. literalinclude:: /examples/usage/test_configuration_21.py
   :language: python
   :caption: `named bindings`
   :lines: 11-26
   :dedent: 2

Migration Configuration
-----------------------

SQLSpec includes a migration system for schema management.

Basic Migration Config
^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_22.py
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

.. literalinclude:: /examples/usage/test_configuration_23.py
   :language: python
   :caption: `litestar plugin configuration`
   :lines: 10-31
   :dedent: 2

Environment-Based Configuration
-------------------------------

Use environment variables for configuration:

.. literalinclude:: /examples/usage/test_configuration_23.py
   :language: python
   :caption: `environnment-based configuration`
   :lines: 49-59
   :dedent: 4

Configuration Best Practices
-----------------------------

**1. Use Connection Pooling**

Always use pooling in production:

.. literalinclude:: /examples/usage/test_configuration_24.py
   :language: python
   :caption: `connection pooling`
   :lines: 11-13
   :dedent: 2

**2. Enable Caching**

Enable caching to avoid recompiling SQL statements:

.. literalinclude:: /examples/usage/test_configuration_25.py
   :language: python
   :caption: `enable caching`
   :lines: 6-8
   :dedent: 2

**3. Tune Pool Sizes**

Size pools based on your workload:

.. literalinclude:: /examples/usage/test_configuration_26.py
   :language: python
   :caption: `tune pool sizes`
   :lines: 6-14
   :dedent: 2

**4. Disable Validation in Production**

For trusted, performance-critical queries:

.. literalinclude:: /examples/usage/test_configuration_26.py
   :language: python
   :caption: `no validation`
   :lines: 19-25
   :dedent: 2


**5. Clean Up Resources**

Always close pools on shutdown:

.. literalinclude:: /examples/usage/test_configuration_27.py
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
