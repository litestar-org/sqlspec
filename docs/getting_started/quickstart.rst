==========
Quickstart
==========

Get running with SQLSpec in a few minutes using SQLite. These examples are short on
purpose so you can copy them into a scratch file and experiment.

Step 1: Connect
---------------

.. literalinclude:: /examples/quickstart/basic_connection.py
   :language: python
   :caption: ``connect to sqlite``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Step 2: Run Your First Query
----------------------------

.. literalinclude:: /examples/quickstart/first_query.py
   :language: python
   :caption: ``first query``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Step 3: Tweak Configuration
---------------------------

.. literalinclude:: /examples/quickstart/configuration.py
   :language: python
   :caption: ``statement configuration``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Next Steps
----------

- :doc:`../usage/drivers_and_querying` for driver-specific guidance and transaction patterns.
- :doc:`../usage/query_builder` if you want the fluent SQL builder.
- :doc:`../usage/sql_files` to load named SQL queries from files.
- :doc:`../usage/framework_integrations` to plug into Litestar, FastAPI, Flask, or Starlette.
- :doc:`/usage/index` for deeper configuration and driver guidance.
