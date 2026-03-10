========
OracleDB
========

Sync and async Oracle adapter using `python-oracledb <https://python-oracledb.readthedocs.io/>`_.
Features native pipeline mode for multi-statement batching, BLOB support, and
LOB coercion with byte-length thresholds.

Sync Configuration
==================

.. autoclass:: sqlspec.adapters.oracledb.OracleSyncConfig
   :members:
   :show-inheritance:

Async Configuration
===================

.. autoclass:: sqlspec.adapters.oracledb.OracleAsyncConfig
   :members:
   :show-inheritance:

Sync Driver
===========

.. autoclass:: sqlspec.adapters.oracledb.OracleSyncDriver
   :members:
   :show-inheritance:

Async Driver
============

.. autoclass:: sqlspec.adapters.oracledb.OracleAsyncDriver
   :members:
   :show-inheritance:
