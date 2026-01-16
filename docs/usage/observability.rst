Observability
=============

SQLSpec provides a comprehensive observability layer that integrates with standard tools
like OpenTelemetry and Prometheus. It allows you to monitor SQL execution performance,
track request correlations, and gather metrics on query duration and rows affected.

Instrumentation
---------------

To enable observability features, you typically wrap or extend your base configuration.
SQLSpec provides helper functions in `sqlspec.extensions` to make this easier.

OpenTelemetry Tracing
---------------------

SQLSpec can automatically generate OpenTelemetry spans for every SQL query. This is useful
for distributed tracing and performance bottlenecks analysis.

To enable tracing, use the ``enable_tracing`` helper:

.. code-block:: python

    from sqlspec.extensions.otel import enable_tracing
    from sqlspec.config import ObservabilityConfig

    # Create a configuration with tracing enabled
    observability = enable_tracing(
        base_config=ObservabilityConfig(),
        resource_attributes={"service.name": "my-service"}
    )

    # Use this config when initializing SQLSpec or your session
    # ...

This will create spans with attributes like:
- ``db.system`` (e.g., "postgresql", "sqlite")
- ``db.statement`` (the sanitized SQL query)
- ``db.operation`` (e.g., "SELECT", "INSERT")

Prometheus Metrics
------------------

You can expose Prometheus metrics for your database interactions, such as query counts
and execution time histograms.

To enable metrics, use the ``enable_metrics`` helper:

.. code-block:: python

    from sqlspec.extensions.prometheus import enable_metrics
    from sqlspec.config import ObservabilityConfig

    # Enable Prometheus metrics
    observability = enable_metrics(
        base_config=ObservabilityConfig(),
        namespace="myapp_sql",  # Prefix for metrics
        label_names=("db_system", "operation")
    )

    # Use this config...

Metrics exposed:
- ``myapp_sql_query_total``: Counter of executed queries.
- ``myapp_sql_query_duration_seconds``: Histogram of execution duration.
- ``myapp_sql_query_rows``: Histogram of rows affected.

Correlation Tracking
--------------------

SQLSpec can track a correlation ID across your application to link SQL logs with specific
requests.

.. literalinclude:: /examples/patterns/observability/correlation_middleware.py
   :language: python
   :caption: ``correlation context``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Logging & Sampling
------------------

You can configure detailed SQL logging and sampling to reduce noise in production.

.. literalinclude:: /examples/patterns/observability/sampling_config.py
   :language: python
   :caption: ``sampling config``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Logger Hierarchy
----------------

SQLSpec uses a hierarchical logger namespace that allows fine-grained control over log levels.
This enables you to configure SQL execution logs independently from internal debug logs.

.. code-block:: text

    sqlspec                              # Root logger for all SQLSpec logs
    ├── sqlspec.sql                      # SQL execution logs (SELECT, INSERT, etc.)
    ├── sqlspec.pool                     # Connection pool operations (acquire, release, recycle)
    ├── sqlspec.cache                    # Cache operations (hit, miss, evict)
    ├── sqlspec.driver                   # Driver base class operations
    ├── sqlspec.core
    │   ├── sqlspec.core.compiler        # SQL compilation
    │   ├── sqlspec.core.splitter        # Statement splitting
    │   └── sqlspec.core.statement       # Statement processing
    ├── sqlspec.adapters
    │   ├── sqlspec.adapters.asyncpg     # AsyncPG adapter
    │   ├── sqlspec.adapters.psycopg     # Psycopg adapter
    │   └── ...                          # Other adapters
    └── sqlspec.observability
        └── sqlspec.observability.lifecycle  # Lifecycle events

**Common Configuration Patterns:**

.. code-block:: python

    import logging

    # Pattern 1: Debug cache while keeping SQL logs at INFO
    logging.getLogger("sqlspec").setLevel(logging.WARNING)
    logging.getLogger("sqlspec.cache").setLevel(logging.DEBUG)

    # Pattern 2: Show SQL queries, suppress internal logs
    logging.getLogger("sqlspec").setLevel(logging.WARNING)
    logging.getLogger("sqlspec.sql").setLevel(logging.INFO)

    # Pattern 3: Debug connection pool while keeping other logs quiet
    logging.getLogger("sqlspec").setLevel(logging.WARNING)
    logging.getLogger("sqlspec.pool").setLevel(logging.DEBUG)

    # Pattern 4: Disable all SQLSpec logs
    logging.getLogger("sqlspec").setLevel(logging.CRITICAL)

**Using the SQL_LOGGER_NAME constant:**

.. code-block:: python

    from sqlspec.observability import SQL_LOGGER_NAME

    # Configure SQL logging level
    logging.getLogger(SQL_LOGGER_NAME).setLevel(logging.INFO)

Cache Logging
~~~~~~~~~~~~~

Cache debug logs include a ``cache_namespace`` field to identify which cache type
generated the log. The five cache namespaces are:

- ``statement`` - Compiled SQL statement cache
- ``expression`` - Parsed expression cache
- ``builder`` - Query builder cache
- ``file`` - SQL file cache
- ``optimized`` - Optimized expression cache

Example cache log output with namespace:

.. code-block:: text

    cache.miss extra_fields={'cache_namespace': 'statement', 'cache_size': 0}
    cache.hit  extra_fields={'cache_namespace': 'expression', 'cache_size': 42}

SQL Execution Logs
~~~~~~~~~~~~~~~~~~

SQL execution logs use the operation type (SELECT, INSERT, UPDATE, DELETE, etc.)
as the log message, making logs easier to scan visually.

Example SQL log output:

.. code-block:: text

    SELECT  driver=AsyncpgDriver bind_key=primary duration_ms=3.5 rows=5 sql='SELECT ...'
    INSERT  driver=AsyncpgDriver bind_key=primary duration_ms=1.2 rows=1 sql='INSERT ...'

Pool Logging
~~~~~~~~~~~~

Connection pool operations are logged to the ``sqlspec.pool`` namespace. This allows
you to debug connection lifecycle events independently from SQL execution logs.

Pool logs include structured context fields:

- ``adapter`` - The database adapter (aiosqlite, duckdb, pymysql, sqlite)
- ``pool_id`` - Unique identifier for the pool instance
- ``database`` - Database name or path (sanitized for privacy)
- ``connection_id`` - Connection identifier (when applicable)
- ``reason`` - Why an operation occurred (e.g., exceeded_recycle_time, failed_health_check)

Example pool log messages:

.. code-block:: text

    pool.connection.recycle  adapter=sqlite pool_id=a1b2c3d4 database=:memory: reason=exceeded_recycle_time
    pool.connection.close.timeout  adapter=aiosqlite pool_id=e5f6g7h8 connection_id=abc timeout_seconds=10.0
    pool.extension.load.failed  adapter=duckdb pool_id=i9j0k1l2 extension=httpfs error='...'

**Using the POOL_LOGGER_NAME constant:**

.. code-block:: python

    from sqlspec.utils.logging import POOL_LOGGER_NAME

    # Enable pool debug logs for connection troubleshooting
    logging.getLogger(POOL_LOGGER_NAME).setLevel(logging.DEBUG)

Cloud Log Formatters
--------------------

For cloud environments (like GCP or AWS), structured logging is essential.

.. literalinclude:: /examples/patterns/observability/cloud_formatters.py
   :language: python
   :caption: ``cloud formatters``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Related Guides
--------------

- :doc:`../reference/observability` for API details.
