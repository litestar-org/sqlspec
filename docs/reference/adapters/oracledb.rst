========
OracleDB
========

Sync and async Oracle adapter using `python-oracledb <https://python-oracledb.readthedocs.io/>`_.
Features native pipeline mode for multi-statement batching, BLOB support, and
LOB coercion with byte-length thresholds.

LOB And JSON Fetching
=====================

Oracle configurations default ``fetch_lobs`` to ``False``. With modern
``python-oracledb`` this returns supported LOB values under Oracle's 1 GB
direct-fetch ceiling directly as ``str`` or ``bytes`` for normal SELECTs,
streaming reads, and Arrow exports. SQLSpec still materializes readable locators
when Oracle returns one, so buffered results and schema hydration do not expose
driver handles by default.

Pass ``fetch_lobs=True`` on a query when application code needs native Oracle
LOB locators, for example in a streaming workflow that wants to control when a
large value is read.

JSON fetch conversion is metadata-driven:

* native ``JSON`` columns are returned by ``python-oracledb``;
* ``IS JSON`` CLOB/BLOB/VARCHAR2 columns are decoded through Oracle fetch
  metadata;
* OSON BLOB values are decoded through Oracle's OSON support when the server and
  driver expose it.

Unconstrained CLOB or BLOB columns are returned as text or bytes even when their
contents look like JSON. Add an Oracle JSON type or ``IS JSON`` constraint when
you want automatic JSON decoding.

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

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.oracledb.data_dictionary.OracleVersionInfo
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.oracledb.data_dictionary.OracledbSyncDataDictionary
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.oracledb.data_dictionary.OracledbAsyncDataDictionary
   :members:
   :show-inheritance:
