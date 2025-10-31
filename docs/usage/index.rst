=====
Usage
=====

This section provides comprehensive guides on using SQLSpec for database operations. Whether you're new to SQLSpec or looking to leverage advanced features, these guides will help you get the most out of the library.

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

Overview
--------

SQLSpec provides a unified interface for database operations across multiple backends while maintaining a direct connection to SQL. The key concepts are:

**Data Flow**
   Understand how SQLSpec processes queries from input to result using its sophisticated pipeline architecture.

**Configuration**
   Learn how to configure database connections, connection pools, and statement processing options.

**Drivers and Querying**
   Discover the available database drivers and how to execute queries effectively.

**Query Builder**
   Explore the experimental fluent API for programmatically constructing SQL queries.

**SQL Files**
   Manage SQL statements from files using the aiosql-style loader.

**Command Line Interface**
   Use the SQLSpec CLI for migrations, with shell completion support for bash, zsh, and fish.

**Database Migrations**
   Manage database schema changes with support for hybrid versioning, automatic schema migration,
   and extension migrations.

**Framework Integrations**
   Integrate SQLSpec with Litestar, FastAPI, and other Python web frameworks.

Quick Reference
---------------

**Basic Query Execution**

.. literalinclude:: /docs/examples/usage/test_index_1.py
   :language: python
   :caption: ``basic query execution``
   :lines: 4-16
   :dedent: 2


**Using the Query Builder**

.. literalinclude:: /docs/examples/usage/test_index_2.py
   :language: python
   :caption: ``using the query builder``
   :lines: 19-21
   :dedent: 2


**Loading from SQL Files**

.. literalinclude:: /docs/examples/usage/test_index_3.py
   :language: python
   :caption: ``loading from sql files``
   :lines: 25-29
   :dedent: 2


Next Steps
----------

Start with :doc:`data_flow` to understand SQLSpec's execution pipeline, then move on to :doc:`configuration` to set up your database connections.
