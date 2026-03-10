=======
AsyncPG
=======

High-performance async PostgreSQL adapter using `asyncpg <https://github.com/MagicStack/asyncpg>`_.
Supports native pipelines, Arrow export, and Cloud SQL / AlloyDB connectors.

Configuration
=============

.. autoclass:: sqlspec.adapters.asyncpg.AsyncpgConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.asyncpg.config.AsyncpgPoolConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.asyncpg.config.AsyncpgConnectionConfig
   :members:
   :show-inheritance:

Driver
======

.. autoclass:: sqlspec.adapters.asyncpg.AsyncpgDriver
   :members:
   :show-inheritance:

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.asyncpg.data_dictionary.AsyncpgDataDictionary
   :members:
   :show-inheritance:
