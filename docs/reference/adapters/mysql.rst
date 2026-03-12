=====
MySQL
=====

SQLSpec provides three MySQL adapters covering sync and async use cases.

mysql-connector-python
======================

Official MySQL driver with both sync and async support.

Sync Configuration
------------------

.. autoclass:: sqlspec.adapters.mysqlconnector.MysqlConnectorSyncConfig
   :members:
   :show-inheritance:

Async Configuration
-------------------

.. autoclass:: sqlspec.adapters.mysqlconnector.MysqlConnectorAsyncConfig
   :members:
   :show-inheritance:

Sync Driver
-----------

.. autoclass:: sqlspec.adapters.mysqlconnector.MysqlConnectorSyncDriver
   :members:
   :show-inheritance:

Async Driver
------------

.. autoclass:: sqlspec.adapters.mysqlconnector.MysqlConnectorAsyncDriver
   :members:
   :show-inheritance:

PyMySQL
=======

Pure-Python MySQL driver for sync usage.

Configuration
-------------

.. autoclass:: sqlspec.adapters.pymysql.PyMysqlConfig
   :members:
   :show-inheritance:

Driver
------

.. autoclass:: sqlspec.adapters.pymysql.PyMysqlDriver
   :members:
   :show-inheritance:

asyncmy
=======

Async MySQL driver.

Configuration
-------------

.. autoclass:: sqlspec.adapters.asyncmy.AsyncmyConfig
   :members:
   :show-inheritance:

Driver
------

.. autoclass:: sqlspec.adapters.asyncmy.AsyncmyDriver
   :members:
   :show-inheritance:
