Observability
=============

SQLSpec includes lightweight observability hooks for SQL logging, correlation IDs,
custom sampling, and cloud log formatting. This page highlights the core building blocks.

Correlation Tracking
--------------------

.. literalinclude:: /examples/patterns/observability/correlation_middleware.py
   :language: python
   :caption: ``correlation context``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Sampling
--------

.. literalinclude:: /examples/patterns/observability/sampling_config.py
   :language: python
   :caption: ``sampling config``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Cloud Log Formatters
--------------------

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
