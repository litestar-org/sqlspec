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
   :lines: 2-12
   :dedent: 2

Database Configurations
-----------------------

Each database adapter has its own configuration class with adapter-specific settings.

SQLite Configuration
^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_2.py
   :language: python
   :caption: `sqlite configuration`

**Memory Databases**

.. literalinclude:: /examples/usage/test_configuration_3.py
   :language: python
   :caption: `memory sqlite configuration`

PostgreSQL Configuration (asyncpg)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_4.py
   :language: python
   :caption: `postgres asyncpg configuration`

PostgreSQL Configuration (psycopg)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

   The `PsycopgConfig` class referenced here is for documentation purposes only and may not be present in the codebase. Future releases may include this feature if demand warrants.

.. literalinclude:: /examples/usage/test_configuration_5.py
   :language: python
   :caption: `postgres psycopg configuration`

MySQL Configuration (asyncmy)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_6.py
   :language: python
   :caption: `mysql asyncmy configuration`

DuckDB Configuration
^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_7.py
   :language: python
   :caption: `duckdb configuration`

Connection Pooling
------------------

Connection pooling improves performance by reusing database connections. SQLSpec provides built-in pooling for most adapters.

Pool Configuration
^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_8.py
   :language: python
   :caption: `pool configuration`

**Pool Lifecycle Management**

.. literalinclude:: /examples/usage/test_configuration_9.py
   :language: python
   :caption: `pool lifecycle management`

Using Pre-Created Pools
^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_10.py
   :language: python
   :caption: `using pre-created pools`

No-Pooling Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_11.py
   :language: python
   :caption: `no-pooling configuration`

Statement Configuration
-----------------------

Statement configuration controls SQL processing pipeline behavior.

Basic Statement Config
^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_12.py
   :language: python
   :caption: `basic statement config`

Parameter Style Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_13.py
   :language: python
   :caption: `parameter style configuration`

**Parameter Styles**

SQLSpec supports multiple parameter placeholder styles:

.. literalinclude:: /examples/usage/test_configuration_14.py
   :language: python
   :caption: `parameter styles`

Validation Configuration
^^^^^^^^^^^^^^^^^^^^^^^^

Configure security and performance validation.

Disable validation for performance-critical paths where input is trusted:

.. literalinclude:: /examples/usage/test_configuration_15.py
   :language: python
   :caption: `validation configuration`

Cache Configuration
-------------------

SQLSpec uses multi-tier caching to avoid recompiling SQL statements.

Global Cache Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_16.py
   :language: python
   :caption: `global cache configuration`

Per-Instance Cache Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_17.py
   :language: python
   :caption: `per-instance cache configuration`

Cache Statistics
^^^^^^^^^^^^^^^^

Monitor cache statistics:

.. literalinclude:: /examples/usage/test_configuration_18.py
   :language: python
   :caption: `cache statistics`

Clear Cache
^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_19.py
   :language: python
   :caption: `clear cache`

Multiple Database Configurations
---------------------------------

SQLSpec supports multiple database configurations in a single application.

Binding Multiple Configs
^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_20.py
   :language: python
   :caption: `binding multiple configurations`

Named Bindings
^^^^^^^^^^^^^^

Use bind keys for clearer configuration management:

.. literalinclude:: /examples/usage/test_configuration_21.py
   :language: python
   :caption: `named bindings`

Migration Configuration
-----------------------

SQLSpec includes a migration system for schema management.

Basic Migration Config
^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: /examples/usage/test_configuration_22.py
   :language: python
   :caption: `basic migration config`
   :lines: 12-24
   :dedent: 4

**Migration CLI**

.. literalinclude:: /examples/usage/test_configuration_22.txt
   :language: text
   :caption: `migration CLI`


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

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig

   config = AsyncpgConfig(
       pool_config={"dsn": "postgresql://localhost/db"},
       extension_config={
           "litestar": {
               "connection_key": "db_connection",
               "session_key": "db_session",
               "pool_key": "db_pool",
               "commit_mode": "autocommit",
               "enable_correlation_middleware": True,
           }
       }
   )

Environment-Based Configuration
-------------------------------

Use environment variables for configuration:

.. code-block:: python

   import os
   from sqlspec.adapters.asyncpg import AsyncpgConfig

   config = AsyncpgConfig(
       pool_config={
           "host": os.getenv("DB_HOST", "localhost"),
           "port": int(os.getenv("DB_PORT", "5432")),
           "user": os.getenv("DB_USER"),
           "password": os.getenv("DB_PASSWORD"),
           "database": os.getenv("DB_NAME"),
       }
   )

Configuration Best Practices
-----------------------------

**1. Use Connection Pooling**

Always use pooling in production:

.. code-block:: python

   config = AsyncpgConfig(
       pool_config={
           "dsn": "postgresql://localhost/db",
           "min_size": 10,
           "max_size": 20,
       }
   )

**2. Enable Caching**

Enable caching to avoid recompiling SQL statements:

.. code-block:: python

   statement_config = StatementConfig(
       dialect="postgres",
       enable_caching=True
   )

**3. Tune Pool Sizes**

Size pools based on your workload:

.. code-block:: python

   # CPU-bound workload
   pool_config = {"min_size": 5, "max_size": 10}

   # I/O-bound workload
   pool_config = {"min_size": 20, "max_size": 50}

**4. Disable Validation in Production**

For trusted, performance-critical queries:

.. code-block:: python

   statement_config = StatementConfig(
       dialect="postgres",
       enable_validation=False,  # Skip security checks
   )

**5. Clean Up Resources**

Always close pools on shutdown:

.. code-block:: python

   # Synchronous cleanup (automatic with atexit)
   # Asynchronous cleanup (manual)
   await spec.close_all_pools()

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
