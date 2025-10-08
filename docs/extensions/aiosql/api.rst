=============
API Reference
=============

Complete API reference for the aiosql integration.

SQLFileLoader
=============

.. autoclass:: sqlspec.loader.SQLFileLoader
   :members:
   :undoc-members:
   :show-inheritance:

For complete SQLFileLoader documentation, see :doc:`/reference/base`.

aiosql Adapters
===============

AiosqlAsyncAdapter
------------------

.. autoclass:: sqlspec.extensions.aiosql.AiosqlAsyncAdapter
   :members:
   :undoc-members:
   :show-inheritance:

AiosqlSyncAdapter
-----------------

.. autoclass:: sqlspec.extensions.aiosql.AiosqlSyncAdapter
   :members:
   :undoc-members:
   :show-inheritance:

Query Operators
===============

The aiosql adapter supports all aiosql query operators:

.. list-table::
   :header-rows: 1
   :widths: 15 30 55

   * - Operator
     - Meaning
     - Returns
   * - (none)
     - Select many
     - List of rows
   * - ``^``
     - Select one
     - Single row or None
   * - ``$``
     - Select value
     - Single value or None
   * - ``!``
     - Insert/Update/Delete
     - Rows affected (sync) / None (async)
   * - ``*!``
     - Insert/Update/Delete many
     - Rows affected (sync) / None (async)
   * - ``#``
     - Script
     - None

Usage Examples
==============

SQLFileLoader Example
---------------------

Direct usage of SQLFileLoader (for advanced use cases):

.. code-block:: python

   from sqlspec.loader import SQLFileLoader

   # Create and load
   loader = SQLFileLoader()
   loader.load_sql("queries/")

   # Get query
   query = loader.get_sql("get_user")

   # Execute with parameters
   result = await session.execute(query, user_id=1)
   user = result.one()

Recommended usage via SQLSpec:

.. code-block:: python

   from sqlspec import SQLSpec

   spec = SQLSpec()
   spec.load_sql_files("queries/")

   # Get query
   query = spec.get_sql("get_user")

   # Execute with parameters
   async with spec.provide_session(config) as session:
       result = await session.execute(query, user_id=1)
       user = result.one()

aiosql Adapter Example (Async)
-------------------------------

.. code-block:: python

   import aiosql
   from sqlspec.extensions.aiosql import AiosqlAsyncAdapter

   # Create adapter
   adapter = AiosqlAsyncAdapter(driver)

   # Load queries
   queries = aiosql.from_path("queries.sql", adapter)

   # Execute
   user = await queries.get_user(conn, user_id=1)

aiosql Adapter Example (Sync)
------------------------------

.. code-block:: python

   import aiosql
   from sqlspec.extensions.aiosql import AiosqlSyncAdapter

   # Create adapter
   adapter = AiosqlSyncAdapter(driver)

   # Load queries
   queries = aiosql.from_path("queries.sql", adapter)

   # Execute
   user = queries.get_user(conn, user_id=1)

Type Aliases
============

Common imports:

.. code-block:: python

   # SQLFileLoader
   from sqlspec.loader import SQLFileLoader

   # aiosql adapters
   from sqlspec.extensions.aiosql import (
       AiosqlAsyncAdapter,
       AiosqlSyncAdapter
   )

   # For type hints
   from sqlspec.driver import (
       AsyncDriverAdapterBase,
       SyncDriverAdapterBase
   )

See Also
========

- :doc:`quickstart` - Get started guide
- :doc:`usage` - Advanced usage
- :doc:`migration` - Migration from aiosql
- :doc:`/usage/sql_files` - Complete SQL file guide
- :doc:`/reference/base` - Complete API reference
