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
connection lifecycle and transaction boundaries automatically. Choose sync or async:

.. tab-set::

   .. tab-item:: Sync

      .. literalinclude:: /examples/quickstart/basic_connection.py
         :language: python
         :caption: ``sync connection (sqlite)``
         :start-after: # start-example
         :end-before: # end-example
         :dedent: 4
         :no-upgrade:

   .. tab-item:: Async

      .. literalinclude:: /examples/quickstart/async_connection.py
         :language: python
         :caption: ``async connection (aiosqlite)``
         :start-after: # start-example
         :end-before: # end-example
         :dedent: 4
         :no-upgrade:

Step 2: Run Your First Query
----------------------------

Execute statements with parameter placeholders (``:param`` for named parameters or ``?`` for positional).
SQLSpec binds parameters safely to prevent SQL injection and returns structured results accessible
via ``.one()``, ``.all()``, or ``.scalar()``:

.. literalinclude:: /examples/quickstart/first_query.py
   :language: python
   :caption: ``named parameters and query execution``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Step 3: Map Rows to Typed Models
--------------------------------

SQLSpec is a type-safe query mapper. You can map query results directly into standard
library ``@dataclass``, ``msgspec.Struct``, or ``pydantic.BaseModel`` instances using
the ``schema_type`` parameter:

.. literalinclude:: /examples/quickstart/typed_mapping.py
   :language: python
   :caption: ``type-safe model mapping``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Next Steps
----------

- :doc:`../usage/data_flow` to understand how sessions, drivers, and results connect.
- :doc:`../usage/drivers_and_querying` for driver-specific guidance and transaction patterns.
- :doc:`../usage/query_builder` if you want the fluent SQL builder.
- :doc:`../usage/sql_files` to load named SQL queries from files.
- :doc:`../usage/framework_integrations` to plug into Litestar, FastAPI, Flask, or Starlette.
- :doc:`../recipes/index` for production patterns like DI containers and service layers.
