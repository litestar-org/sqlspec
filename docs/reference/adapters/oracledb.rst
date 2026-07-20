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

.. _oracledb-extension-storage-options:

Extension Table Storage Options
===============================

Oracle ADK, durable event, and Litestar session tables support the same
opt-in storage concepts under their extension configuration: ``in_memory``,
``compression``, ``partitioning``, and table options. For example, an events
queue can use Advanced Compression and monthly interval partitions::

    extension_config = {
        "events": {
            "compression": {"enabled": True, "algorithm": "advanced"},
            "partitioning": {
                "strategy": "range",
                "partition_key": "available_at",
                "interval": "month",
            },
            "table_options": "TABLESPACE event_data",
        }
    }

Use the same keys under ``litestar``; range partitioning defaults to
``expires_at``. Under ``adk``, per-table options use names such as
``session_table_options``, ``events_table_options``, and
``memory_table_options``. ADK partition settings can likewise override a
specific table key with ``session_partition_key``, ``events_partition_key``,
or the corresponding state or memory key.

SQLSpec resolves Oracle Partitioning, Advanced Compression, Basic Compression,
and Database In-Memory availability once per connection pool through the data
dictionary. If the option catalog is inaccessible or a requested feature is not
available, SQLSpec logs a structured warning and creates the table without that
optimization. User-provided table options are still emitted because they are
application DDL rather than a capability-detected Oracle option.

SQLSpec does not automatically add ``SECUREFILE`` LOB compression. Its safety
also depends on tablespace segment-space management and database-level
``DB_SECUREFILE`` policy, which cannot be established from the option catalog
alone. Add a reviewed LOB clause through the table-options setting when the
deployment guarantees those prerequisites.

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
