=======
Psycopg
=======

PostgreSQL adapter using `psycopg 3 <https://www.psycopg.org/psycopg3/>`_ with both
sync and async support. Features native pipeline mode for multi-statement batching.

Sync Configuration
==================

.. autoclass:: sqlspec.adapters.psycopg.PsycopgSyncConfig
   :members:
   :show-inheritance:

Async Configuration
===================

.. autoclass:: sqlspec.adapters.psycopg.PsycopgAsyncConfig
   :members:
   :show-inheritance:

Shared Configuration
====================

.. autoclass:: sqlspec.adapters.psycopg.config.PsycopgConnectionParams
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.psycopg.config.PsycopgPoolParams
   :members:
   :show-inheritance:

Sync Driver
===========

.. autoclass:: sqlspec.adapters.psycopg.PsycopgSyncDriver
   :members:
   :show-inheritance:

Async Driver
============

.. autoclass:: sqlspec.adapters.psycopg.PsycopgAsyncDriver
   :members:
   :show-inheritance:

Extension Dialects
==================

Psycopg supports the :doc:`pgvector and ParadeDB dialects <../dialects>` for vector
similarity search and full-text search operators. See the :doc:`Dialects <../dialects>`
reference for operator details.

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.psycopg.data_dictionary.PsycopgSyncDataDictionary
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.psycopg.data_dictionary.PsycopgAsyncDataDictionary
   :members:
   :show-inheritance:
