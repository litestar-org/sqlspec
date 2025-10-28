==========
Quickstart
==========

This quickstart guide will get you running your first SQLSpec query in under 5 minutes. We'll use SQLite since it requires no additional setup, but the same patterns apply to all supported databases.

Your First Query
----------------

Let's start with the simplest possible example - executing a query and getting results:

.. literalinclude:: /examples/quickstart/quickstart_1.py
   :language: python
   :caption: ``first sqlspec query``

What's happening here?

1. **Create SQLSpec instance**: This is your central registry for database configurations.
2. **Configure a database**: Each database gets a config object. Here we're using in-memory SQLite.
3. **Get a session**: The context manager provides a database session and handles cleanup.
4. **Execute SQL**: Write your SQL directly - no magic, no abstraction.

Working with Real Data
----------------------

Let's create a table, insert some data, and query it:

.. literalinclude:: /examples/quickstart/quickstart_2.py
   :language: python
   :caption: ``working with real data``

Session Methods Cheat Sheet
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

SQLSpec provides several convenient query methods:

- ``execute(sql, *params)``: Execute any SQL, returns full result
- ``select(sql, *params)``: Execute SELECT, returns list of rows
- ``select_one(sql, *params)``: Get single row, raises error if not found
- ``select_one_or_none(sql, *params)``: Get single row or None
- ``select_value(sql, *params)``: Get single scalar value
- ``execute_many(sql, params_list)``: Execute with multiple parameter sets
- ``execute_script(sql)``: Execute multiple SQL statements

Type-Safe Results
-----------------

The real power of SQLSpec comes from type-safe result mapping. Define your data models and SQLSpec automatically maps query results to them:

.. literalinclude:: /examples/quickstart/quickstart_3.py
   :language: python
   :caption: ``type-safe results``

.. note::

    SQLSpec supports multiple type libraries: Pydantic, msgspec, attrs, and standard library dataclasses. Choose the one that fits your project!

Async Support
-------------

SQLSpec supports async/await for non-blocking database operations. Here's the same example with async:

.. literalinclude:: /examples/quickstart/quickstart_4.py
   :language: python
   :caption: ``async support``

The API is identical - just add ``await`` and use async config/drivers!

Switching Databases
-------------------

One of SQLSpec's strengths is the consistent API across databases. Here's the same code using PostgreSQL:

.. literalinclude:: /examples/quickstart/quickstart_5.py
   :language: python
   :caption: ``switching databases``


.. tip::

    Each database has its own parameter style (``?`` for SQLite, ``$1`` for PostgreSQL, ``%s`` for MySQL, etc.). SQLSpec handles this automatically - you just need to use the correct style for your database.

    `PEP249`_ is worth a read on that

    .. _PEP249: https://peps.python.org/pep-0249/#paramstyle

Multiple Databases
------------------

Need to work with multiple databases? Register multiple configs:

.. literalinclude:: /examples/quickstart/quickstart_6.py
   :language: python
   :caption: ``multiple databases``


Transaction Support
-------------------

SQLSpec automatically manages transactions. By default, each session is a transaction:

.. literalinclude:: /examples/quickstart/quickstart_7.py
   :language: python
   :caption: ``transaction support``

.. note::

    Transaction behavior can be configured per session or globally. See the :doc:`../usage/drivers_and_querying` guide for details on transaction modes.

Query Builder (Experimental)
----------------------------

For those who prefer programmatic query construction, SQLSpec includes an experimental query builder:

.. literalinclude:: /examples/quickstart/quickstart_8.py
   :language: python
   :caption: ``query builder``

.. warning::

    The query builder API is experimental and will change significantly in future releases. Use raw SQL for production code.

Next Steps
----------

You've now seen the basics of SQLSpec! Here's where to go next:

**Usage Guides**

- :doc:`../usage/configuration` - Learn about configuration options and connection pooling
- :doc:`../usage/drivers_and_querying` - Deep dive into drivers, sessions, and query execution
- :doc:`../usage/data_flow` - Understand how SQLSpec processes queries internally
- :doc:`../usage/sql_files` - Load SQL queries from files
- :doc:`../usage/framework_integrations` - Integrate with Litestar, FastAPI, and Flask

**Examples**

- :doc:`../examples/index` - Gallery of practical examples for various databases

**API Reference**

- :doc:`../reference/index` - Complete API documentation

.. tip::

    SQLSpec is designed to be simple but powerful. Start with raw SQL and add features like type-safe mapping and the query builder only when you need them.
