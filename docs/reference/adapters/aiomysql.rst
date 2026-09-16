========
aiomysql
========

Async MySQL driver (PyMySQL-compatible wire protocol, asyncio-native).

Configuration
=============

.. autoclass:: sqlspec.adapters.aiomysql.AiomysqlConfig
   :members:
   :show-inheritance:

Connection Parameters
=====================

.. autoclass:: sqlspec.adapters.aiomysql.AiomysqlConnectionParams
   :members:
   :show-inheritance:

Pool Parameters
===============

.. autoclass:: sqlspec.adapters.aiomysql.AiomysqlPoolParams
   :members:
   :show-inheritance:

Driver Features
===============

.. autoclass:: sqlspec.adapters.aiomysql.AiomysqlDriverFeatures
   :members:
   :show-inheritance:

Driver
======

.. autoclass:: sqlspec.adapters.aiomysql.AiomysqlDriver
   :members:
   :show-inheritance:

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.aiomysql.data_dictionary.AiomysqlDataDictionary
   :members:
   :show-inheritance:

Extension Settings
==================

Use the configuration types below in their corresponding ``extension_config``
namespace: ``"litestar"``, ``"events"``, or ``"adk"`` as supported by this adapter.

.. autoclass:: sqlspec.adapters.aiomysql.litestar.AiomysqlLitestarConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.aiomysql.adk.AiomysqlADKConfig
   :members:
   :show-inheritance:
