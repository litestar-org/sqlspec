=======
asyncmy
=======

Async MySQL adapter using `asyncmy <https://github.com/long2ice/asyncmy>`_, an
asyncio-native MySQL driver written in Cython. Provides asynchronous execution,
connection pooling, and PyMySQL-compatible wire protocol handling.

Configuration
=============

.. autoclass:: sqlspec.adapters.asyncmy.AsyncmyConfig
   :members:
   :show-inheritance:

Connection Parameters
=====================

.. autoclass:: sqlspec.adapters.asyncmy.AsyncmyConnectionParams
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.asyncmy.AsyncmySSLParams
   :members:
   :show-inheritance:

Pool Parameters
===============

.. autoclass:: sqlspec.adapters.asyncmy.AsyncmyPoolParams
   :members:
   :show-inheritance:

Driver Features
===============

.. autoclass:: sqlspec.adapters.asyncmy.AsyncmyDriverFeatures
   :members:
   :show-inheritance:

Driver
======

.. autoclass:: sqlspec.adapters.asyncmy.AsyncmyDriver
   :members:
   :show-inheritance:

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.asyncmy.data_dictionary.AsyncmyDataDictionary
   :members:
   :show-inheritance:

Extension Settings
==================

Use the configuration types below in their corresponding ``extension_config``
namespace: ``"litestar"``, ``"events"``, or ``"adk"`` as supported by this adapter.

.. autoclass:: sqlspec.adapters.asyncmy.litestar.AsyncmyLitestarConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.asyncmy.adk.AsyncmyADKConfig
   :members:
   :show-inheritance:
