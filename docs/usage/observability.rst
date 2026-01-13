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
