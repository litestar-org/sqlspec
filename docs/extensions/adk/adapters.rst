========
Adapters
========

ADK stores are backed by SQLSpec adapters. Configure a backend with a standard
SQLSpec config class, then pass it to the ADK store implementation.

- :doc:`backends` lists supported adapters.
- :doc:`../usage/drivers_and_querying` covers adapter configuration patterns.

Example
=======

.. literalinclude:: /examples/extensions/adk/backend_config.py
   :language: python
   :caption: ``adk backend config``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:
