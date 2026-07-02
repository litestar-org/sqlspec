========
pymssql
========

Sync SQL Server adapter using `pymssql <https://pypi.org/project/pymssql/>`_
and FreeTDS. It uses pyformat parameters (``%s`` and ``%(name)s``) and exposes
sync SQLSpec config, driver, pooling, data dictionary, migration, and extension
store integrations.

Configuration
=============

.. autoclass:: sqlspec.adapters.pymssql.PymssqlConfig
   :members:
   :show-inheritance:

Connection Parameters
=====================

.. autoclass:: sqlspec.adapters.pymssql.config.PymssqlConnectionParams
   :members:
   :show-inheritance:

Driver Features
===============

.. autoclass:: sqlspec.adapters.pymssql.config.PymssqlDriverFeatures
   :members:
   :show-inheritance:

Driver
======

.. autoclass:: sqlspec.adapters.pymssql.PymssqlDriver
   :members:
   :show-inheritance:

Connection Pool
===============

.. autoclass:: sqlspec.adapters.pymssql.PymssqlConnectionPool
   :members:
   :show-inheritance:

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.pymssql.data_dictionary.PymssqlSyncDataDictionary
   :members:
   :show-inheritance:

Migrations
==========

.. autoclass:: sqlspec.adapters.pymssql.migrations.PymssqlSyncMigrationTracker
   :members:
   :show-inheritance:
