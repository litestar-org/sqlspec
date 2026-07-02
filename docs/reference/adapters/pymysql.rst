=======
PyMySQL
=======

Pure-Python MySQL driver for sync usage.

Configuration
=============

.. autoclass:: sqlspec.adapters.pymysql.PyMysqlConfig
   :members:
   :show-inheritance:

Cloud SQL Connector
===================

PyMySQL configs can use the in-process Google Cloud SQL Python Connector by
installing the ``cloud-sql`` extra and enabling the connector in
``driver_features``:

.. code-block:: python

   from sqlspec.adapters.pymysql import PyMysqlConfig

   config = PyMysqlConfig(
       connection_config={
           "user": "app-user",
           "password": "secret",
           "database": "app",
       },
       driver_features={
           "enable_cloud_sql": True,
           "cloud_sql_instance": "project:region:instance",
           "cloud_sql_ip_type": "PRIVATE",
       },
   )

When ``enable_cloud_sql`` is true, ``cloud_sql_instance`` is required and must
use ``project:region:instance`` format. Host, port, socket, and direct auth
connection values are passed through the connector rather than opened directly
by PyMySQL.

Driver
======

.. autoclass:: sqlspec.adapters.pymysql.PyMysqlDriver
   :members:
   :show-inheritance:

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.pymysql.data_dictionary.PyMysqlDataDictionary
   :members:
   :show-inheritance:
