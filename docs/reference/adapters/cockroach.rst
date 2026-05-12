===========
CockroachDB
===========

CockroachDB adapters with automatic transaction retry logic. Available in
asyncpg and psycopg variants.

CockroachDB + AsyncPG
=====================

Configuration
-------------

.. autoclass:: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgPoolConfig
   :members:
   :show-inheritance:

Driver
------

.. autoclass:: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgDriver
   :members:
   :show-inheritance:

Retry Configuration
-------------------

.. autoclass:: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgRetryConfig
   :members:
   :show-inheritance:

CockroachDB + Psycopg
=====================

Sync Configuration
------------------

.. autoclass:: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgSyncConfig
   :members:
   :show-inheritance:

Async Configuration
-------------------

.. autoclass:: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgAsyncConfig
   :members:
   :show-inheritance:

Sync Driver
-----------

.. autoclass:: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgSyncDriver
   :members:
   :show-inheritance:

Async Driver
------------

.. autoclass:: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgAsyncDriver
   :members:
   :show-inheritance:

Retry Configuration
-------------------

.. autoclass:: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgRetryConfig
   :members:
   :show-inheritance:

Data Dictionaries
=================

.. autoclass:: sqlspec.adapters.cockroach_asyncpg.data_dictionary.CockroachAsyncpgDataDictionary
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.cockroach_psycopg.data_dictionary.CockroachPsycopgSyncDataDictionary
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.cockroach_psycopg.data_dictionary.CockroachPsycopgAsyncDataDictionary
   :members:
   :show-inheritance:
