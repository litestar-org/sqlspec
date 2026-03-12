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

Driver
======

.. autoclass:: sqlspec.adapters.aiosqlite.AiosqliteDriver
   :members:
   :show-inheritance:

Connection Pool
===============

.. autoclass:: sqlspec.adapters.aiosqlite.pool.AiosqliteConnectionPool
   :members:
   :show-inheritance:
