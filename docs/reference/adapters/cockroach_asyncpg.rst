=====================
CockroachDB + AsyncPG
=====================

CockroachDB adapter using asyncpg with automatic transaction retry logic.

Configuration
=============

.. autoclass:: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgPoolConfig
   :members:
   :show-inheritance:

Driver
======

.. autoclass:: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgDriver
   :members:
   :show-inheritance:

Retry Configuration
===================

.. autoclass:: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgRetryConfig
   :members:
   :show-inheritance:

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.cockroach_asyncpg.data_dictionary.CockroachAsyncpgDataDictionary
   :members:
   :show-inheritance:
