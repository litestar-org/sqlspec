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

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.sqlite import SqliteConfig

   # Create SQLSpec instance
   spec = SQLSpec()

   # Add database configuration
   db = spec.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

   # Use the database
   with spec.provide_session(db) as session:
       result = session.execute("SELECT 1")

Database Configurations
-----------------------

Each database adapter has its own configuration class with adapter-specific settings.

SQLite Configuration
^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.adapters.sqlite import SqliteConfig

   config = SqliteConfig(
       pool_config={
           "database": "myapp.db",           # Database file path
           "timeout": 5.0,                    # Lock timeout in seconds
           "check_same_thread": False,        # Allow multi-thread access
           "cached_statements": 100,          # Statement cache size
           "uri": False,                      # Enable URI mode
       }
   )

**Memory Databases**

.. code-block:: python

   # In-memory database (isolated per connection)
   config = SqliteConfig(pool_config={"database": ":memory:"})

   # Shared memory database
   config = SqliteConfig(
       pool_config={
           "database": "file:memdb1?mode=memory&cache=shared",
           "uri": True
       }
   )

PostgreSQL Configuration (asyncpg)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig

   config = AsyncpgConfig(
       pool_config={
           "dsn": "postgresql://user:pass@localhost:5432/dbname",
           # Or individual parameters:
           "host": "localhost",
           "port": 5432,
           "user": "myuser",
           "password": "mypassword",
           "database": "mydb",
           # Pool settings
           "min_size": 10,
           "max_size": 20,
           "max_queries": 50000,
           "max_inactive_connection_lifetime": 300.0,
       }
   )

PostgreSQL Configuration (psycopg)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.adapters.psycopg import PsycopgConfig

   # Async version
   config = PsycopgConfig(
       pool_config={
           "conninfo": "postgresql://user:pass@localhost/db",
           # Or keyword arguments:
           "host": "localhost",
           "port": 5432,
           "dbname": "mydb",
           "user": "myuser",
           "password": "mypassword",
           # Pool settings
           "min_size": 5,
           "max_size": 10,
           "timeout": 30.0,
       }
   )

MySQL Configuration (asyncmy)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.adapters.asyncmy import AsyncmyConfig

   config = AsyncmyConfig(
       pool_config={
           "host": "localhost",
           "port": 3306,
           "user": "myuser",
           "password": "mypassword",
           "database": "mydb",
           "charset": "utf8mb4",
           # Pool settings
           "minsize": 1,
           "maxsize": 10,
           "pool_recycle": 3600,
       }
   )

DuckDB Configuration
^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.adapters.duckdb import DuckDBConfig

   # In-memory database
   config = DuckDBConfig()

   # Persistent database
   config = DuckDBConfig(
       pool_config={
           "database": "analytics.duckdb",
           "read_only": False,
       }
   )

Connection Pooling
------------------

Connection pooling improves performance by reusing database connections. SQLSpec provides built-in pooling for most adapters.

Pool Configuration
^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig

   config = AsyncpgConfig(
       pool_config={
           "dsn": "postgresql://localhost/db",
           "min_size": 10,        # Minimum connections to maintain
           "max_size": 20,        # Maximum connections allowed
           "max_queries": 50000,  # Max queries per connection before recycling
           "max_inactive_connection_lifetime": 300.0,  # Idle timeout
       }
   )

**Pool Lifecycle Management**

.. code-block:: python

   # SQLSpec manages pool lifecycle automatically
   spec = SQLSpec()
   db = spec.add_config(AsyncpgConfig(pool_config={...}))

   # Pool is created on first use
   async with spec.provide_session(db) as session:
       await session.execute("SELECT 1")

   # Clean up all pools on shutdown
   await spec.close_all_pools()

Using Pre-Created Pools
^^^^^^^^^^^^^^^^^^^^^^^^

You can create and manage pools manually:

.. code-block:: python

   import asyncpg

   # Create pool manually
   pool = await asyncpg.create_pool(
       dsn="postgresql://localhost/db",
       min_size=10,
       max_size=20
   )

   # Pass to config and add to SQLSpec
   db = spec.add_config(AsyncpgConfig(pool_instance=pool))

No-Pooling Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^

For simple use cases or testing, disable pooling:

.. code-block:: python

   from sqlspec.adapters.sqlite import SqliteConfig

   # SQLite uses thread-local connections (no traditional pooling)
   config = SqliteConfig(pool_config={"database": "test.db"})

Statement Configuration
-----------------------

Statement configuration controls SQL processing pipeline behavior.

Basic Statement Config
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.core.statement import StatementConfig
   from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig

   statement_config = StatementConfig(
       dialect="postgres",                # SQLGlot dialect
       enable_parsing=True,               # Parse SQL into AST
       enable_validation=True,            # Run security/performance validators
       enable_transformations=True,       # Apply AST transformations
       enable_caching=True,               # Enable multi-tier caching
   )

   # Apply to adapter
   config = AsyncpgConfig(
       pool_config={...},
       statement_config=statement_config
   )

Parameter Style Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Control how parameters are handled:

.. code-block:: python

   from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig

   param_config = ParameterStyleConfig(
       default_parameter_style=ParameterStyle.NUMERIC,  # $1, $2, ...
       supported_parameter_styles={
           ParameterStyle.NUMERIC,
           ParameterStyle.NAMED_COLON,  # :name
       },
       has_native_list_expansion=False,
       needs_static_script_compilation=False,
          )

   statement_config = StatementConfig(
       dialect="postgres",
       parameter_config=param_config
   )

**Parameter Styles**

SQLSpec supports multiple parameter placeholder styles:

.. code-block:: python

   from sqlspec.core.parameters import ParameterStyle

   # Question mark (SQLite, DuckDB)
   ParameterStyle.QMARK          # WHERE id = ?

   # Numeric (PostgreSQL, asyncpg)
   ParameterStyle.NUMERIC        # WHERE id = $1

   # Named colon (Oracle, SQLite)
   ParameterStyle.NAMED_COLON    # WHERE id = :id

   # Named at (BigQuery)
   ParameterStyle.NAMED_AT       # WHERE id = @id

   # Format/pyformat (psycopg, MySQL)
   ParameterStyle.POSITIONAL_PYFORMAT         # WHERE id = %s
   ParameterStyle.NAMED_PYFORMAT       # WHERE id = %(id)s

Validation Configuration
^^^^^^^^^^^^^^^^^^^^^^^^

Configure security and performance validation.

Disable validation for performance-critical paths where input is trusted:

.. code-block:: python

   statement_config = StatementConfig(
       dialect="postgres",
       enable_validation=False,  # Skip validation
       enable_transformations=False,  # Skip transformations
   )

Cache Configuration
-------------------

SQLSpec uses multi-tier caching to avoid recompiling SQL statements.

Global Cache Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.core.cache import CacheConfig, update_cache_config

   cache_config = CacheConfig(
       enable_sql_cache=True,          # Cache compiled SQL strings
       enable_optimized_cache=True,    # Cache optimized AST
       enable_builder_cache=True,      # Cache QueryBuilder state
       enable_file_cache=True,         # Cache loaded SQL files
       max_cache_size=1000,            # Maximum cached items
   )

   # Update global cache configuration
   update_cache_config(cache_config)

Per-Instance Cache Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Configure cache for specific SQLSpec instance
   spec = SQLSpec()
   spec.update_cache_config(
       CacheConfig(
           enable_sql_cache=True,
           max_cache_size=500
       )
   )

Cache Statistics
^^^^^^^^^^^^^^^^

Monitor cache statistics:

.. code-block:: python

   from sqlspec.core.cache import get_cache_statistics, log_cache_stats

   # Get statistics
   stats = get_cache_statistics()
   print(f"SQL Cache hits: {stats['sql_cache_hits']}")
   print(f"File Cache hits: {stats['file_cache_hits']}")

   # Log statistics
   log_cache_stats()  # Logs to configured logger

Clear Cache
^^^^^^^^^^^

.. code-block:: python

   from sqlspec.core.cache import reset_cache_stats

   # Clear all caches and reset statistics
   reset_cache_stats()

Multiple Database Configurations
---------------------------------

SQLSpec supports multiple database configurations in a single application.

Binding Multiple Configs
^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.sqlite import SqliteConfig
   from sqlspec.adapters.asyncpg import AsyncpgConfig

   spec = SQLSpec()

   # Add multiple configurations
   sqlite_db = spec.add_config(SqliteConfig(pool_config={"database": "cache.db"}))
   postgres_db = spec.add_config(AsyncpgConfig(pool_config={"dsn": "postgresql://..."}))

   # Use specific configuration
   with spec.provide_session(sqlite_db) as session:
       session.execute("SELECT * FROM cache")

   async with spec.provide_session(postgres_db) as session:
       await session.execute("SELECT * FROM users")

Named Bindings
^^^^^^^^^^^^^^

Use bind keys for clearer configuration management:

.. code-block:: python

   # Add with bind keys
   cache_db = spec.add_config(SqliteConfig(pool_config={"database": "cache.db"}), bind_key="cache_db")
   main_db = spec.add_config(AsyncpgConfig(pool_config={"dsn": "postgresql://..."}), bind_key="main_db")

   # Access by bind key
   with spec.provide_session("cache_db") as session:
       session.execute("SELECT 1")

Migration Configuration
-----------------------

SQLSpec includes a migration system for schema management.

Basic Migration Config
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig

   config = AsyncpgConfig(
       pool_config={"dsn": "postgresql://localhost/db"},
       extension_config={
           "litestar": {"session_table": "custom_sessions"}  # Extension settings
       },
       migration_config={
           "script_location": "migrations",     # Migration directory
           "version_table": "alembic_version",  # Version tracking table
           "include_extensions": ["litestar"],  # Simple string list only
       }
   )

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
