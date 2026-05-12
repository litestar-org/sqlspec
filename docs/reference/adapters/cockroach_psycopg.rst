=====================
CockroachDB + Psycopg
=====================

CockroachDB adapter using psycopg with automatic transaction retry logic.
Provides both sync and async support.

Sync Configuration
==================

.. autoclass:: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgSyncConfig
   :members:
   :show-inheritance:

Async Configuration
===================

.. autoclass:: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgAsyncConfig
   :members:
   :show-inheritance:

Sync Driver
===========

.. autoclass:: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgSyncDriver
   :members:
   :show-inheritance:

Async Driver
============

.. autoclass:: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgAsyncDriver
   :members:
   :show-inheritance:

Retry Configuration
===================

.. autoclass:: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgRetryConfig
   :members:
   :show-inheritance:

Sync Data Dictionary
====================

.. autoclass:: sqlspec.adapters.cockroach_psycopg.data_dictionary.CockroachPsycopgSyncDataDictionary
   :members:
   :show-inheritance:

Async Data Dictionary
=====================

.. autoclass:: sqlspec.adapters.cockroach_psycopg.data_dictionary.CockroachPsycopgAsyncDataDictionary
   :members:
   :show-inheritance:
