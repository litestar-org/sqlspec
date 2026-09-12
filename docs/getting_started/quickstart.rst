==========
Quickstart
==========

.. image:: /_static/demos/quickstart.gif
   :alt: SQLSpec quickstart demo
   :class: demo-gif

Get running with SQLSpec in a few minutes using SQLite. These examples are short on
purpose so you can copy them into a scratch file and experiment.

Step 1: Connect
---------------

Create a ``SQLSpec`` registry, register an adapter configuration (using SQLite in-memory
for this walkthrough), and open a session with ``provide_session()``. Sessions manage
connection lifecycle and cleanup automatically:

.. literalinclude:: /examples/quickstart/basic_connection.py
   :language: python
   :caption: ``connect to sqlite``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Step 2: Run Your First Query
----------------------------

Execute statements with parameter placeholders (``?`` for SQLite or ``:param`` for named parameters).
SQLSpec binds parameters safely to prevent SQL injection and returns structured results accessible
via ``.one()``, ``.all()``, or ``.scalar()``:

.. literalinclude:: /examples/quickstart/first_query.py
   :language: python
   :caption: ``first query``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Step 3: Tweak Configuration
---------------------------

Fine-tune statement execution behavior with ``StatementConfig``. For example, you can
disable AST validation for vendor-specific extensions or customize dialect handling:

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
- :doc:`../recipes/index` for production patterns like DI containers and service layers.
- :doc:`/usage/index` for deeper configuration and driver guidance.
