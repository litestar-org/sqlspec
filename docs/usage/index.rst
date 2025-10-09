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

**Framework Integrations**
   Integrate SQLSpec with Litestar, FastAPI, and other Python web frameworks.

Quick Reference
---------------

**Basic Query Execution**

.. code-block:: python

   from sqlspec import SQLSpec
   from sqlspec.adapters.sqlite import SqliteConfig

   spec = SQLSpec()
   db = spec.add_config(SqliteConfig())

   with spec.provide_session(db) as session:
       result = session.execute("SELECT * FROM users WHERE id = ?", 1)
       user = result.one()

**Using the Query Builder**

.. code-block:: python

   from sqlspec import sql

   query = sql.select("id", "name", "email").from_("users").where("active = ?")
   result = session.execute(query, True)
   users = result.all()

**Loading from SQL Files**

.. code-block:: python

   from sqlspec.loader import SQLFileLoader

   loader = SQLFileLoader()
   loader.load_sql("queries/users.sql")

   user_query = loader.get_sql("get_user_by_id", user_id=123)
   result = session.execute(user_query)

Next Steps
----------

Start with :doc:`data_flow` to understand SQLSpec's execution pipeline, then move on to :doc:`configuration` to set up your database connections.
