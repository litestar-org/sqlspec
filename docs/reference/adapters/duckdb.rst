======
DuckDB
======

Sync DuckDB adapter with full Arrow integration, extension management, and
secret configuration. DuckDB excels at analytical workloads and can query
Parquet, CSV, and JSON files directly.

Lifecycle Model
===============

DuckDB setup work is intentionally split across four lifecycle classes. Keep
expensive or network-capable work out of per-session and per-call paths.

.. list-table::
   :header-rows: 1

   * - Class
     - When it runs
     - DuckDB work
     - Cost
   * - Config and pool setup
     - ``DuckDBConfig(...)`` and the first ``provide_pool()``
     - Build connection configuration, create ``DuckDBConnectionPool``, and
       initialize the extension-install signature set and connection registry.
     - Once per config/pool
   * - Physical connection setup
     - New thread-local connection creation
     - Call ``duckdb.connect()``, apply extension flags, perform explicit
       extension installs once per pool/signature, load configured extensions,
       create secrets, and run ``on_connection_create``.
     - Per physical connection
   * - Session acquisition
     - ``provide_session()`` enter
     - Return the thread-local connection. File-backed connections are reused
       by default and close per session only with
       ``connection_lifetime="session"``.
     - Per session
   * - Per-call work
     - Driver methods such as ``execute()`` and ``select_many()``
     - Compile/cache the statement and execute it.
     - Per call

Extension and Secret Semantics
==============================

``DuckDBExtensionConfig`` distinguishes loading from installation:

* ``{"name": "json"}`` is load-only. SQLSpec calls
  ``load_extension("json")`` for each physical connection and relies on DuckDB
  autoloading known core extensions.
* ``install=True``, ``version``, ``repository``, ``repository_url``, or
  ``force_install=True`` request an explicit ``install_extension()`` call.
  Explicit installs are bounded once per pool/signature unless
  ``force_install=True`` is set.
* ``required=True`` raises install/load failures. The default is best-effort:
  optional failures are logged as warnings so missing optional extensions do
  not abort connection setup.

``DuckDBSecretConfig`` follows the same required-vs-best-effort model.
Secret creation failures raise only when ``required=True``; otherwise SQLSpec
logs a warning and continues connection setup.

Connection Lifetime
===================

File-backed DuckDB databases keep their thread-local connection alive until
``DuckDBConfig.close_pool()`` by default. This avoids repeated connection
bootstrap for short sessions and keeps extension work bounded at the physical
connection lifecycle.

Use ``connection_lifetime="session"`` in ``connection_config`` to restore the
legacy behavior where successful file-backed sessions close their connection on
exit. That opt-in is useful when multiple configs must open the same file with
different settings without explicitly closing the first pool.

When using the default pool lifetime, reconfiguring the same DuckDB file path
requires closing the previous config's pool first:

.. code-block:: python

   config = DuckDBConfig(connection_config={"database": "isolated.duckdb"})

   try:
       with config.provide_session() as db:
           db.execute("SELECT 1")
   finally:
       config.close_pool()

See :doc:`/recipes/duckdb_lifecycle` for an isolated-database pattern that
bundles extensions and closes the pool at each run boundary.

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

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.duckdb.data_dictionary.DuckDBDataDictionary
   :members:
   :show-inheritance:
