============
mssql-python
============

Sync-only SQL Server adapter built on Microsoft's official
`mssql-python <https://pypi.org/project/mssql-python/>`_ driver. Ships
native Arrow reads and BulkCopy, a T-SQL data dictionary, migrations tracker,
events queue store, Litestar session store, and ADK session and memory stores.
The SQL splitter provides ``GO`` batch-separator handling so multi-batch
T-SQL scripts execute correctly.

See :doc:`/recipes/sql_server` for end-to-end examples.

Configuration
=============

.. autoclass:: sqlspec.adapters.mssql_python.MssqlPythonConfig
   :members:
   :show-inheritance:

Connection Parameters
=====================

.. autoclass:: sqlspec.adapters.mssql_python.MssqlPythonConnectionParams
   :members:
   :show-inheritance:

Pool Parameters
===============

.. autoclass:: sqlspec.adapters.mssql_python.MssqlPythonPoolParams
   :members:
   :show-inheritance:

Driver Features
===============

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

Extensions
==========

.. autoclass:: sqlspec.adapters.mssql_python.events.MssqlPythonEventQueueStore
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.mssql_python.litestar.MssqlPythonStore
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.mssql_python.adk.MssqlPythonADKStore
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.mssql_python.adk.MssqlPythonADKMemoryStore
   :members:
   :show-inheritance:

Extension Settings
==================

Use these types inside ``extension_config["adk"]``.

.. autoclass:: sqlspec.adapters.mssql_python.adk.MssqlPythonADKConfig
   :members:
   :show-inheritance:

Native Arrow loading
--------------------

``load_from_arrow()`` accepts tables, record batches, record batch readers, and
Arrow C stream sources. It forwards ``batch_size``, ``timeout``, ``table_lock``,
``check_constraints``, ``fire_triggers``, ``keep_identity``, ``keep_nulls``,
``use_internal_transaction``, and ``column_mappings`` to native BulkCopy.
Field names supply default mappings; sources without schema metadata require
explicit mappings. Native internal transactions apply per batch, not to the
caller connection transaction. ``overwrite=True`` retains DELETE semantics;
stream consumption failures can leave a partial load.
