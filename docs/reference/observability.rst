=============
Observability
=============

Telemetry, logging, and diagnostic infrastructure for monitoring SQL operations.
Supports OpenTelemetry, Prometheus, and structured cloud logging formats.

Configuration
=============

.. autoclass:: sqlspec.observability.ObservabilityConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.observability.TelemetryConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.observability.LoggingConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.observability.RedactionConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.observability.SamplingConfig
   :members:
   :show-inheritance:

Runtime
=======

.. autoclass:: sqlspec.observability.ObservabilityRuntime
   :members:
   :show-inheritance:

Statement Observer
==================

.. autoclass:: sqlspec.observability.StatementEvent
   :members:
   :show-inheritance:

.. autofunction:: sqlspec.observability.create_statement_observer

.. autofunction:: sqlspec.observability.default_statement_observer

.. autofunction:: sqlspec.observability.create_event

.. autofunction:: sqlspec.observability.format_statement_event

Span Management
===============

.. autoclass:: sqlspec.observability.SpanManager
   :members:
   :show-inheritance:

Diagnostics
===========

.. autoclass:: sqlspec.observability.TelemetryDiagnostics
   :members:
   :show-inheritance:

Lifecycle
=========

.. autoclass:: sqlspec.observability.LifecycleDispatcher
   :members:
   :show-inheritance:

Log Formatters
==============

.. autoclass:: sqlspec.observability.OTelConsoleFormatter
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.observability.OTelJSONFormatter
   :members:
   :show-inheritance:

Cloud Log Formatters
--------------------

.. autoclass:: sqlspec.observability.AWSLogFormatter
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.observability.GCPLogFormatter
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.observability.AzureLogFormatter
   :members:
   :show-inheritance:

Prometheus
==========

.. autoclass:: sqlspec.extensions.prometheus.PrometheusStatementObserver
   :members:
   :show-inheritance:

Helper Functions
================

.. autofunction:: sqlspec.observability.compute_sql_hash

.. autofunction:: sqlspec.observability.get_trace_context

.. autofunction:: sqlspec.observability.resolve_db_system
