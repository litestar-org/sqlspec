====
ADBC
====

Arrow Database Connectivity adapter providing native Arrow result handling
without conversion overhead. Supports PostgreSQL, SQLite, DuckDB, BigQuery,
and Snowflake with automatic driver detection and loading.

Configuration
=============

.. autoclass:: sqlspec.adapters.adbc.AdbcConfig
   :members:
   :show-inheritance:

Driver Features
===============

.. autoclass:: sqlspec.adapters.adbc.config.AdbcDriverFeatures
   :members:
   :no-index:

Driver
======

.. autoclass:: sqlspec.adapters.adbc.AdbcDriver
   :members:
   :show-inheritance:

PostgreSQL Extension Dialects
=============================

When targeting PostgreSQL, ADBC automatically detects installed extensions on the
first connection and upgrades the SQL dialect accordingly:

- **pgvector** — If the ``vector`` extension is installed, switches to the ``pgvector``
  dialect which supports distance operators (``<->``, ``<=>``, ``<#>``, ``<+>``, ``<~>``, ``<%>``).
- **ParadeDB** — If the ``pg_search`` extension is installed (alongside ``vector``),
  switches to the ``paradedb`` dialect which adds BM25 search operators (``@@@``, ``&&&``,
  ``|||``, ``===``) on top of pgvector operators.

Detection is controlled by two driver feature flags:

- ``enable_pgvector`` — Defaults to ``True`` when the ``pgvector`` Python package is installed.
- ``enable_paradedb`` — Defaults to ``True``.

Detection runs once per config instance and caches the result. Non-PostgreSQL backends
(SQLite, DuckDB, BigQuery, Snowflake) skip detection entirely.

.. note::

   ADBC returns vector data as strings (e.g. ``"[0.1,0.2,0.3]"``).
   The ``pgvector`` Python package is not required for querying vector data.
   It only affects the *default* value of ``enable_pgvector`` — when the package
   is installed, detection is enabled automatically. You can always set
   ``enable_pgvector=True`` explicitly in ``driver_features`` to enable
   detection without the package installed.

See the :doc:`Dialects <../dialects>` reference for full operator details.

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.adbc.data_dictionary.AdbcDataDictionary
   :members:
   :show-inheritance:
