============
mssql-python
============

Sync SQL Server adapter built on Microsoft's official
`mssql-python <https://pypi.org/project/mssql-python/>`_ driver. Ships a
T-SQL data dictionary, a Litestar session store, an events queue store,
and migrations tracker. The SQL splitter also gains ``GO`` batch-separator
handling so multi-batch T-SQL scripts execute correctly.

Configuration
=============

.. autoclass:: sqlspec.adapters.mssql_python.MssqlPythonConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.mssql_python.MssqlPythonConnectionParams
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.mssql_python.MssqlPythonPoolParams
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.mssql_python.MssqlPythonDriverFeatures
   :members:
   :show-inheritance:

Driver
======

.. autoclass:: sqlspec.adapters.mssql_python.MssqlPythonDriver
   :members:
   :show-inheritance:

Connection Pool
===============

.. autoclass:: sqlspec.adapters.mssql_python.MssqlPythonConnectionPool
   :members:
   :show-inheritance:

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.mssql_python.MssqlPythonSyncDataDictionary
   :members:
   :show-inheritance:

Migrations
==========

.. autoclass:: sqlspec.adapters.mssql_python.MssqlPythonSyncMigrationTracker
   :members:
   :show-inheritance:
