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

**Query Stack**
   Learn how to batch heterogeneous SQL statements with immutable stacks, choose between fail-fast and continue-on-error execution, and monitor native vs. sequential paths in :doc:`/reference/query-stack`.

**SQL Files**
   Manage SQL statements from files using the SQL file loader.

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

.. literalinclude:: /examples/usage/usage_index_1.py
   :language: python
   :caption: ``basic query execution``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

**Using the Query Builder**

.. literalinclude:: /examples/usage/usage_index_2.py
   :language: python
   :caption: ``using the query builder``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

**Loading from SQL Files**

.. literalinclude:: /examples/usage/usage_index_3.py
   :language: python
   :caption: ``loading from sql files``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Next Steps
----------

Start with :doc:`data_flow` to understand SQLSpec's execution pipeline, then move on to :doc:`configuration` to set up your database connections.
