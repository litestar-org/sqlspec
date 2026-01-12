=====
Usage
=====

This section provides focused guides on core SQLSpec workflows. Each page highlights the
minimum you need to move fast, with links to deeper examples when you want more detail.

.. toctree::
   :maxdepth: 2

   data_flow
   configuration
   drivers_and_querying
   query_builder
   sql_files
   cli
   migrations
   framework_integrations
   observability

Overview
--------

SQLSpec provides a unified interface for database operations across multiple backends while
keeping SQL at the center. Start with configuration and drivers, then move into query
construction, migration tooling, and framework integrations.

Quick Reference
---------------

**Connect and run a query**

.. literalinclude:: /examples/quickstart/basic_connection.py
   :language: python
   :caption: ``basic connection``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

**Build a query programmatically**

.. literalinclude:: /examples/builder/select_query.py
   :language: python
   :caption: ``select query``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

**Load SQL from files**

.. literalinclude:: /examples/sql_files/load_sql_files.py
   :language: python
   :caption: ``load sql files``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Next Steps
----------

Start with :doc:`configuration` for connection options, then review :doc:`drivers_and_querying`
for driver-specific guidance and execution patterns.
