==========
Prometheus
==========

Prometheus metrics extension for SQLSpec. Provides a statement observer and
configuration helpers that record SQL execution counts, duration histograms,
and row count histograms.

Observer
========

.. autoclass:: sqlspec.extensions.prometheus.PrometheusStatementObserver
   :members:
   :show-inheritance:

Helpers
=======

.. autofunction:: sqlspec.extensions.prometheus.enable_metrics
