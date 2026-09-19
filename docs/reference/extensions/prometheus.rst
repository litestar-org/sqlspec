==========
Prometheus
==========

Prometheus metrics extension for SQLSpec. Provides a statement observer and
configuration helpers that record SQL execution counts, duration histograms,
and row count histograms.

Configuration
=============

Use ``sqlspec.config.PrometheusConfig`` in ``extension_config["prometheus"]``.
These settings apply across adapters; no adapter-specific subtype is needed.

.. autoclass:: sqlspec.config.PrometheusConfig
   :members:
   :show-inheritance:
   :no-index:

Observer
========

.. autoclass:: sqlspec.extensions.prometheus.PrometheusStatementObserver
   :members:
   :show-inheritance:

Helpers
=======

.. autofunction:: sqlspec.extensions.prometheus.enable_metrics
