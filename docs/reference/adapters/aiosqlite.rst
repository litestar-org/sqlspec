=========
aiosqlite
=========

Async SQLite adapter using `aiosqlite <https://github.com/omnilib/aiosqlite>`_.
Supports URI-based in-memory databases with per-config instance isolation.

Configuration
=============

.. autoclass:: sqlspec.adapters.aiosqlite.AiosqliteConfig
   :members:
   :show-inheritance:

Connection Parameters
=====================

.. autoclass:: sqlspec.adapters.aiosqlite.AiosqliteConnectionParams
   :members:
   :show-inheritance:

Pool Parameters
===============

.. autoclass:: sqlspec.adapters.aiosqlite.AiosqlitePoolParams
   :members:
   :show-inheritance:

Driver Features
===============

.. autoclass:: sqlspec.adapters.aiosqlite.AiosqliteDriverFeatures
   :members:
   :show-inheritance:

User-Defined Functions and Extensions
=====================================

.. autoclass:: sqlspec.adapters.aiosqlite.AiosqliteFunctionConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.aiosqlite.AiosqliteCollationConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.aiosqlite.AiosqliteAggregateConfig
   :members:
   :show-inheritance:

Driver
======

.. autoclass:: sqlspec.adapters.aiosqlite.AiosqliteDriver
   :members:
   :show-inheritance:

Connection Pool
===============

.. autoclass:: sqlspec.adapters.aiosqlite.AiosqliteConnectionPool
   :members:
   :show-inheritance:

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.aiosqlite.data_dictionary.AiosqliteDataDictionary
   :members:
   :show-inheritance:

Extension Settings
==================

Use these adapter-specific types inside ``extension_config["litestar"]``
and ``extension_config["events"]``.

.. autoclass:: sqlspec.adapters.aiosqlite.litestar.AiosqliteLitestarConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.aiosqlite.events.AiosqliteEventsConfig
   :members:
   :show-inheritance:
