==============
Observability
==============

SQLSpec provides comprehensive observability features for monitoring, debugging, and tracing database operations. This guide covers correlation tracking, sampling, statement logging, and cloud-native log formatting.

Overview
--------

SQLSpec's observability stack includes:

- **Correlation Tracking**: Propagate request IDs through database operations for distributed tracing
- **Sampling**: Control which statements are logged to reduce volume in high-traffic systems
- **Statement Observers**: Hook into query execution for custom logging and metrics
- **Cloud Formatters**: Format logs for GCP, AWS, and Azure cloud logging services

Quick Start
-----------

Enable basic observability:

.. code-block:: python

   from sqlspec import SQLSpec, ObservabilityConfig
   from sqlspec.adapters.asyncpg import AsyncpgConfig

   spec = SQLSpec()
   db = spec.add_config(
       AsyncpgConfig(
           connection_config={"dsn": "postgresql://localhost/mydb"},
           observability_config=ObservabilityConfig(
               print_sql=True,  # Log SQL statements
           ),
       )
   )

Correlation Tracking
--------------------

Correlation IDs allow you to trace requests through your entire system, from HTTP request to database query.

CorrelationContext
^^^^^^^^^^^^^^^^^^

The :class:`~sqlspec.utils.correlation.CorrelationContext` provides async-safe correlation ID tracking:

.. code-block:: python

   from sqlspec.utils.correlation import CorrelationContext

   # Set correlation ID (typically done by middleware)
   CorrelationContext.set("request-abc-123")

   # Get current correlation ID (available anywhere in the call stack)
   correlation_id = CorrelationContext.get()

   # Use context manager for scoped correlation
   with CorrelationContext.context("batch-job-456"):
       # All database operations here will be tagged with "batch-job-456"
       await db.execute("SELECT * FROM users")

CorrelationExtractor
^^^^^^^^^^^^^^^^^^^^

The :class:`~sqlspec.core.CorrelationExtractor` extracts correlation IDs from HTTP headers with configurable priority:

.. code-block:: python

   from sqlspec.core import CorrelationExtractor

   extractor = CorrelationExtractor(
       primary_header="x-request-id",           # First priority
       additional_headers=("x-correlation-id",), # Additional headers to check
       auto_trace_headers=True,                  # Check W3C/cloud trace headers
       max_length=128,                           # Maximum ID length
   )

   # Extract from headers (works with any framework)
   correlation_id = extractor.extract(
       lambda header: request.headers.get(header)
   )

Default headers checked (in order):

1. Primary header (default: ``x-request-id``)
2. Additional headers (if configured)
3. ``x-correlation-id``
4. ``traceparent`` (W3C Trace Context)
5. ``x-cloud-trace-context`` (GCP)
6. ``x-amzn-trace-id`` (AWS)
7. ``x-b3-traceid`` (Zipkin)

If no header is found, a UUID is generated automatically.

Framework Middleware
^^^^^^^^^^^^^^^^^^^^

**Starlette/FastAPI:**

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.extensions.starlette import SQLSpecPlugin

   spec = SQLSpec()
   db = spec.add_config(
       AsyncpgConfig(
           connection_config={"dsn": "postgresql://localhost/mydb"},
           extension_config={
               "starlette": {
                   "enable_correlation_middleware": True,
                   "correlation_header": "x-request-id",
                   "correlation_headers": ("x-trace-id",),
                   "auto_trace_headers": True,
               }
           },
       )
   )

   app = Starlette(routes=[...])
   plugin = SQLSpecPlugin(spec, app)

The middleware:

- Extracts correlation ID from request headers
- Sets ``CorrelationContext`` for the request scope
- Stores correlation ID in ``request.state.correlation_id``
- Adds ``X-Correlation-ID`` header to responses

**Flask:**

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.sqlite import SqliteConfig
   from sqlspec.extensions.flask import SQLSpecPlugin

   spec = SQLSpec()
   db = spec.add_config(
       SqliteConfig(
           connection_config={"database": "app.db"},
           extension_config={
               "flask": {
                   "enable_correlation_middleware": True,
                   "correlation_header": "x-request-id",
                   "auto_trace_headers": True,
               }
           },
       )
   )

   app = Flask(__name__)
   plugin = SQLSpecPlugin(spec, app)

The Flask hooks:

- Extract correlation ID in ``before_request``
- Store in ``g.correlation_id`` and ``CorrelationContext``
- Add ``X-Correlation-ID`` header in ``after_request``
- Clear context in ``teardown_appcontext``

Sampling
--------

Control which statements are logged to reduce volume in high-traffic systems.

SamplingConfig
^^^^^^^^^^^^^^

The :class:`~sqlspec.observability.SamplingConfig` controls sampling behavior:

.. code-block:: python

   from sqlspec import ObservabilityConfig
   from sqlspec.observability import SamplingConfig

   config = ObservabilityConfig(
       sampling=SamplingConfig(
           sample_rate=0.1,              # Log 10% of queries
           deterministic=True,           # Same correlation ID = same decision
           force_sample_on_error=True,   # Always log errors
           force_sample_slow_queries_ms=100.0,  # Always log slow queries
       ),
   )

**Sample Rate**: Value between 0.0 (no sampling) and 1.0 (sample everything).

**Deterministic Sampling**: When enabled, the same correlation ID always produces the same sampling decision. This ensures all queries in a request are either all logged or all skipped.

.. code-block:: python

   # Deterministic: consistent within a request
   config = SamplingConfig(sample_rate=0.5, deterministic=True)

   # Same correlation ID always gives same result
   config.should_sample("request-123")  # True
   config.should_sample("request-123")  # True (same)
   config.should_sample("request-456")  # False (different ID)

**Force Sample Conditions**: Override sampling for important events:

.. code-block:: python

   config = SamplingConfig(
       sample_rate=0.01,  # Only 1% normally
       force_sample_on_error=True,        # But always log errors
       force_sample_slow_queries_ms=50.0, # And queries over 50ms
   )

Statement Observers
-------------------

Hook into query execution with custom observers:

.. code-block:: python

   from sqlspec import ObservabilityConfig

   def my_observer(event):
       print(f"Query: {event.sql}")
       print(f"Duration: {event.duration_s * 1000:.2f}ms")
       print(f"Correlation ID: {event.correlation_id}")
       print(f"Rows affected: {event.rows_affected}")
       print(f"Sampled: {event.sampled}")

   config = ObservabilityConfig(
       statement_observers=(my_observer,),
   )

StatementEvent Fields
^^^^^^^^^^^^^^^^^^^^^

Each :class:`~sqlspec.observability.StatementEvent` includes:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Field
     - Description
   * - ``sql``
     - The executed SQL statement
   * - ``parameters``
     - Query parameters (may be redacted)
   * - ``driver``
     - Driver class name (e.g., "AsyncpgDriver")
   * - ``adapter``
     - Adapter config name
   * - ``db_system``
     - Database system (postgresql, mysql, sqlite, etc.)
   * - ``operation``
     - SQL operation type (SELECT, INSERT, UPDATE, etc.)
   * - ``duration_s``
     - Execution time in seconds
   * - ``rows_affected``
     - Number of rows affected
   * - ``correlation_id``
     - Request correlation ID
   * - ``sampled``
     - Whether this event was sampled for logging
   * - ``trace_id``
     - OpenTelemetry trace ID
   * - ``span_id``
     - OpenTelemetry span ID
   * - ``is_many``
     - True for executemany operations
   * - ``is_script``
     - True for script execution

Redaction
---------

Protect sensitive data in logs:

.. code-block:: python

   from sqlspec import ObservabilityConfig, RedactionConfig

   config = ObservabilityConfig(
       redaction=RedactionConfig(
           mask_parameters=True,      # Replace parameter values with ***
           mask_literals=True,        # Replace SQL literals with ***
           parameter_allow_list=(     # Parameters to NOT redact
               "id", "limit", "offset"
           ),
       ),
   )

Cloud Log Formatters
--------------------

Format logs for cloud logging services with structured output.

GCP (Google Cloud Logging)
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.observability import GCPLogFormatter

   formatter = GCPLogFormatter(project_id="my-gcp-project")
   entry = formatter.format(
       "INFO",
       "Query executed",
       correlation_id="req-123",
       trace_id="abc123",
       span_id="span456",
       duration_ms=15.5,
   )
   # Output includes:
   # - severity: "INFO"
   # - logging.googleapis.com/trace: "projects/my-gcp-project/traces/abc123"
   # - logging.googleapis.com/spanId: "span456"
   # - logging.googleapis.com/labels: {"correlation_id": "req-123"}

AWS (CloudWatch Logs)
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.observability import AWSLogFormatter

   formatter = AWSLogFormatter()
   entry = formatter.format(
       "INFO",
       "Query executed",
       correlation_id="req-123",
       trace_id="1-abc-def",
       duration_ms=15.5,
   )
   # Output includes:
   # - level: "INFO"
   # - requestId: "req-123"
   # - xray_trace_id: "1-abc-def"
   # - timestamp: ISO 8601 format

Azure (Application Insights)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from sqlspec.observability import AzureLogFormatter

   formatter = AzureLogFormatter()
   entry = formatter.format(
       "INFO",
       "Query executed",
       correlation_id="req-123",
       trace_id="trace123",
       span_id="span456",
       duration_ms=15.5,
   )
   # Output includes:
   # - severityLevel: 1 (numeric)
   # - operation_Id: "trace123"
   # - operation_ParentId: "span456"
   # - properties: {"correlationId": "req-123", "durationMs": 15.5}

Custom Formatter
^^^^^^^^^^^^^^^^

Implement the :class:`~sqlspec.observability.CloudLogFormatter` protocol:

.. code-block:: python

   from typing import Any

   class MyFormatter:
       def format(
           self,
           level: str,
           message: str,
           *,
           correlation_id: str | None = None,
           trace_id: str | None = None,
           span_id: str | None = None,
           duration_ms: float | None = None,
           extra: dict[str, Any] | None = None,
       ) -> dict[str, Any]:
           return {
               "level": level,
               "message": message,
               "correlation_id": correlation_id,
               # ... your custom format
           }

Configuring Cloud Formatter
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Add a cloud formatter directly to :class:`~sqlspec.observability.ObservabilityConfig`:

.. code-block:: python

   from sqlspec import ObservabilityConfig
   from sqlspec.observability import GCPLogFormatter

   config = ObservabilityConfig(
       cloud_formatter=GCPLogFormatter(project_id="my-project"),
       print_sql=True,
   )

The ``cloud_formatter`` field accepts any object implementing the :class:`~sqlspec.observability.CloudLogFormatter` protocol. When merged, the override formatter replaces the base formatter.

Configuration Merging
---------------------

Observability configs merge intelligently when combining base and override configurations:

.. code-block:: python

   from sqlspec import ObservabilityConfig
   from sqlspec.observability import SamplingConfig

   # Base config (e.g., from registry)
   base = ObservabilityConfig(
       sampling=SamplingConfig(
           sample_rate=0.5,
           force_sample_on_error=True,
       ),
   )

   # Override config (e.g., from adapter)
   override = ObservabilityConfig(
       sampling=SamplingConfig(
           sample_rate=0.1,  # Override rate
           # force_sample_on_error inherited from base
       ),
   )

   # Merged result
   merged = ObservabilityConfig.merge(base, override)
   # merged.sampling.sample_rate == 0.1
   # merged.sampling.force_sample_on_error == True

Best Practices
--------------

**1. Use Correlation IDs Everywhere**

Enable correlation middleware in your framework integration to automatically track requests through the database layer.

**2. Sample in Production**

Use sampling to control log volume while still capturing important events:

.. code-block:: python

   SamplingConfig(
       sample_rate=0.01,              # 1% normal traffic
       force_sample_on_error=True,    # 100% errors
       force_sample_slow_queries_ms=100.0,  # 100% slow queries
   )

**3. Use Deterministic Sampling**

Enable deterministic sampling to ensure all queries in a request are logged together:

.. code-block:: python

   SamplingConfig(
       sample_rate=0.1,
       deterministic=True,  # Consistent per correlation ID
   )

**4. Redact Sensitive Data**

Always enable redaction in production:

.. code-block:: python

   RedactionConfig(
       mask_parameters=True,
       mask_literals=True,
   )

**5. Use Cloud Formatters**

Use the appropriate cloud formatter for structured logging in your environment.

Next Steps
----------

- :doc:`framework_integrations` - Framework-specific setup
- :doc:`configuration` - Database configuration options

See Also
--------

- :class:`~sqlspec.observability.ObservabilityConfig` - Full configuration reference
- :class:`~sqlspec.observability.SamplingConfig` - Sampling configuration
- :class:`~sqlspec.observability.StatementEvent` - Event payload details
