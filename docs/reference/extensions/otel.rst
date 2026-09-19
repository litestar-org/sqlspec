==============
OpenTelemetry
==============

OpenTelemetry tracing extension for SQLSpec. Provides helpers for enabling
OpenTelemetry spans across database statement executions and connection lifecycles.

Configuration
=============

Use ``sqlspec.config.OpenTelemetryConfig`` in ``extension_config["otel"]``.
These settings apply across adapters; no adapter-specific subtype is needed.

.. autoclass:: sqlspec.config.OpenTelemetryConfig
   :members:
   :show-inheritance:
   :no-index:

Helpers
=======

.. autofunction:: sqlspec.extensions.otel.enable_tracing
