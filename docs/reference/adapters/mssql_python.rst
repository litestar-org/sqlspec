============
mssql-python
============

Sync + async SQL Server adapter built on Microsoft's official
`mssql-python <https://pypi.org/project/mssql-python/>`_ driver. Ships a
T-SQL data dictionary, a Litestar session store, an events queue store,
and migrations tracker. The SQL splitter also gains ``GO`` batch-separator
handling so multi-batch T-SQL scripts execute correctly.

Sync Configuration
==================

.. autoclass:: sqlspec.adapters.mssql_python.MssqlPythonConfig
   :members:
   :show-inheritance:

Async Configuration
===================

.. autoclass:: sqlspec.adapters.mssql_python.MssqlPythonAsyncConfig
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

Sync Driver
===========

.. autoclass:: sqlspec.adapters.mssql_python.MssqlPythonDriver
   :members:
   :show-inheritance:

Async Driver
============

.. autoclass:: sqlspec.adapters.mssql_python.MssqlPythonAsyncDriver
   :members:
   :show-inheritance:

Connection Pool
===============

.. autoclass:: sqlspec.adapters.mssql_python.MssqlPythonConnectionPool
   :members:
   :show-inheritance:

Data Dictionaries
=================

.. autoclass:: sqlspec.adapters.mssql_python.MssqlPythonSyncDataDictionary
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.mssql_python.MssqlPythonAsyncDataDictionary
   :members:
   :show-inheritance:

Migration Trackers
==================

.. autoclass:: sqlspec.adapters.mssql_python.MssqlPythonSyncMigrationTracker
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.mssql_python.MssqlPythonAsyncMigrationTracker
   :members:
   :show-inheritance:
