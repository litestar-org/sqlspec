======
DuckDB
======

Sync DuckDB adapter with full Arrow integration, extension management, and
secret configuration. DuckDB excels at analytical workloads and can query
Parquet, CSV, and JSON files directly.

Configuration
=============

.. autoclass:: sqlspec.adapters.duckdb.DuckDBConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.duckdb.config.DuckDBExtensionConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.duckdb.config.DuckDBSecretConfig
   :members:
   :show-inheritance:

Driver
======

.. autoclass:: sqlspec.adapters.duckdb.DuckDBDriver
   :members:
   :show-inheritance:

Connection Pool
===============

.. autoclass:: sqlspec.adapters.duckdb.DuckDBConnectionPool
   :members:
   :show-inheritance:
