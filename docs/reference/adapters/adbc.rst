====
ADBC
====

The Arrow Database Connectivity (ADBC) adapter connects SQLSpec to 11 relational,
analytical, cloud, and Arrow Flight SQL backends through a single
:class:`~sqlspec.adapters.adbc.AdbcConfig` interface.

- **Arrow-first data path**: :meth:`~sqlspec.adapters.adbc.AdbcDriver.select_to_arrow`
  and :meth:`~sqlspec.adapters.adbc.AdbcDriver.load_from_arrow` transfer Arrow
  ``Table`` and ``RecordBatchReader`` payloads without intermediate Python tuple
  allocation.
- **Standard row & model APIs**: :meth:`~sqlspec.adapters.adbc.AdbcDriver.select`,
  :meth:`~sqlspec.adapters.adbc.AdbcDriver.select_one`, and
  :meth:`~sqlspec.adapters.adbc.AdbcDriver.select_stream` materialize dictionaries or
  typed models (`msgspec`, Pydantic, dataclasses) from Arrow batches.
- **Automatic dialect & parameter routing**: SQLSpec detects the connected engine,
  configures SQLGlot dialect compilation, and normalizes parameter placeholders,
  exception classes, savepoint syntax, and column casing per backend.

Installation
============

Install SQLSpec with the ``adbc`` extra (which installs ``adbc-driver-manager`` and
``pyarrow``), then install the ADBC driver package for your target database:

.. tab-set::

   .. tab-item:: Enterprise (Oracle, Db2, SQL Server)

      .. code-block:: bash

         # Oracle (pure-Rust ADBC driver wheel from GizmoData)
         uv add "sqlspec[adbc]" adbc-driver-oracle

         # IBM Db2 (requires IBM ODBC/CLI driver header/library at runtime)
         uv add "sqlspec[adbc]" adbc-driver-db2

         # Microsoft SQL Server via ADBC Driver Foundry (installs native Go TDS driver)
         uv add "sqlspec[adbc]"
         dbc install mssql

   .. tab-item:: Relational & Local (PostgreSQL, SQLite, DuckDB, Quack)

      .. code-block:: bash

         # PostgreSQL
         uv add "sqlspec[adbc]" adbc-driver-postgresql

         # SQLite
         uv add "sqlspec[adbc]" adbc-driver-sqlite

         # DuckDB (bundled driver or standalone pure-Rust Quack driver)
         uv add "sqlspec[adbc]" adbc-driver-duckdb
         uv add "sqlspec[adbc]" adbc-driver-quack

   .. tab-item:: Cloud & Flight SQL (BigQuery, Snowflake, GizmoSQL)

      .. code-block:: bash

         # Google BigQuery & Snowflake
         uv add "sqlspec[adbc]" adbc-driver-bigquery adbc-driver-snowflake

         # GizmoSQL (native driver with OAuth/OIDC support, or Flight SQL fallback)
         uv add "sqlspec[adbc]" adbc-driver-gizmosql
         uv add "sqlspec[adbc]" adbc-driver-flightsql

Supported Backends
==================

SQLSpec resolves the ADBC driver from either ``driver_name`` (or ``driver``) or the
connection ``uri`` scheme:

.. list-table::
   :header-rows: 1
   :widths: 15 20 18 22 25

   * - Backend
     - ``driver_name`` aliases
     - URI schemes
     - Driver package / binary
     - Dialect & parameter behavior
   * - PostgreSQL
     - ``postgres``, ``postgresql``, ``pg``
     - ``postgresql://``, ``postgres://``
     - ``adbc_driver_postgresql``
     - ``postgres`` dialect; numeric (``$1``) parameters; auto-detects ``pgvector``, ``pg_textsearch``, and ``paradedb``.
   * - Oracle
     - ``oracle``, ``oracledb``
     - ``oracle://``
     - ``adbc_driver_oracle``
     - ``oracle`` dialect; positional colon (``:1``) execution parameters; lowercases implicit uppercase column names.
   * - IBM Db2
     - ``db2``, ``ibmdb2``, ``ibm_db``
     - ``db2://``
     - ``adbc_driver_db2``
     - ``db2`` dialect; positional ``?`` parameters; lowercases implicit uppercase column names.
   * - SQL Server
     - ``mssql``, ``sqlserver``, ``tsql``
     - ``mssql://``, ``sqlserver://``
     - ``adbc_driver_mssql`` or ``adbc_driver_manager`` (``driver="mssql"``)
     - ``tsql`` compilation dialect; positional ``?`` execution parameters; ``SAVE TRANSACTION`` savepoints.
   * - SQLite
     - ``sqlite``, ``sqlite3``
     - ``sqlite://``
     - ``adbc_driver_sqlite``
     - ``sqlite`` dialect; positional ``?`` parameters; ``sqlite://`` prefixes are normalized to file paths.
   * - DuckDB
     - ``duckdb``
     - ``duckdb://``
     - ``adbc_driver_duckdb``
     - ``duckdb`` dialect; ``?`` and ``$1`` parameters; ``COPY ... TO/FROM`` parameter inlining.
   * - DuckDB (Quack)
     - ``quack``
     - ``quack://``, ``quack+tls://``
     - ``adbc_driver_quack``
     - ``duckdb`` dialect; supports local files, ``:memory:``, and MotherDuck (`quack:///md:db` + ``token``).
   * - BigQuery
     - ``bigquery``, ``bq``
     - ``bigquery://``
     - ``adbc_driver_bigquery``
     - ``bigquery`` dialect; named ``@param`` parameters; lifts ``project_id``, ``dataset_id``, and ``token`` into ``db_kwargs``.
   * - Snowflake
     - ``snowflake``, ``sf``
     - ``snowflake://``
     - ``adbc_driver_snowflake``
     - ``snowflake`` dialect; ``?`` and ``$1`` parameters.
   * - Flight SQL
     - ``flightsql``, ``grpc``
     - ``grpc://``, ``grpc+tls://``
     - ``adbc_driver_flightsql``
     - Arrow Flight SQL transport; defaults to ``sqlite`` dialect unless overridden.
   * - GizmoSQL
     - ``gizmosql``, ``gizmo``
     - ``gizmosql://``, ``gizmo://``
     - ``adbc_driver_gizmosql`` (falls back to ``adbc_driver_flightsql``)
     - ``duckdb`` dialect by default (or ``sqlite`` via ``gizmosql_backend="sqlite"``); supports ``auth_type="external"`` SSO.

.. tip::

   You can also pass a filesystem path to any ADBC shared library (``.so``,
   ``.dylib``, or ``.dll``) as ``driver_name``. SQLSpec automatically loads it
   through ``adbc_driver_manager.dbapi.connect`` and forwards ``uri``,
   ``username``, ``password``, and ``entrypoint`` into ``db_kwargs``.

Connection Examples
===================

.. tab-set::

   .. tab-item:: Enterprise (Oracle, Db2, SQL Server)

      .. code-block:: python

         from sqlspec.adapters.adbc import AdbcConfig

         # Oracle (EasyConnect URI; discrete username/password are lifted to db_kwargs)
         oracle_config = AdbcConfig(
             connection_config={
                 "driver_name": "oracle",
                 "uri": "oracle://localhost:1521/FREEPDB1",
                 "username": "system",
                 "password": "secret",
             }
         )

         # IBM Db2 (DSN string or db2:// URI)
         db2_config = AdbcConfig(
             connection_config={
                 "driver_name": "db2",
                 "uri": "DATABASE=testdb;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=secret;",
             }
         )

         # Microsoft SQL Server (loaded via ADBC driver manager or adbc_driver_mssql)
         mssql_config = AdbcConfig(
             connection_config={
                 "driver_name": "mssql",
                 "uri": "sqlserver://sa:SecretPassword123!@localhost:1433?database=app_db&trustServerCertificate=true",
             }
         )

   .. tab-item:: Relational & Local (PostgreSQL, SQLite, DuckDB, Quack)

      .. code-block:: python

         from sqlspec.adapters.adbc import AdbcConfig

         postgres_config = AdbcConfig(
             connection_config={
                 "uri": "postgresql://user:password@localhost:5432/app",
             }
         )

         sqlite_config = AdbcConfig(
             connection_config={
                 "uri": "sqlite:///tmp/app.db",
             }
         )

         duckdb_config = AdbcConfig(
             connection_config={
                 "driver_name": "duckdb",
                 "path": "/tmp/app.duckdb",
             }
         )

         # DuckDB / MotherDuck via adbc-driver-quack
         quack_config = AdbcConfig(
             connection_config={
                 "driver_name": "quack",
                 "uri": "quack:///md:analytics_db",
                 "token": "md_eyJ...",
             }
         )

   .. tab-item:: Cloud Data Warehouses (BigQuery, Snowflake)

      .. code-block:: python

         from sqlspec.adapters.adbc import AdbcConfig

         bigquery_config = AdbcConfig(
             connection_config={
                 "driver_name": "bigquery",
                 "project_id": "analytics-project",
                 "dataset_id": "events",
             }
         )

         snowflake_config = AdbcConfig(
             connection_config={
                 "driver_name": "snowflake",
                 "uri": "snowflake://user:password@account/db/schema?warehouse=compute_wh",
             }
         )

   .. tab-item:: Flight SQL & GizmoSQL

      .. code-block:: python

         from sqlspec.adapters.adbc import AdbcConfig

         # GizmoSQL with username/password or browser-based OAuth/OIDC ("external")
         gizmosql_config = AdbcConfig(
             connection_config={
                 "driver_name": "gizmosql",
                 "uri": "grpc+tls://localhost:31337",
                 "username": "admin",
                 "password": "secret",
                 "tls_skip_verify": True,
                 "query_timeout": 60,
             }
         )

         gizmosql_oauth_config = AdbcConfig(
             connection_config={
                 "driver_name": "gizmosql",
                 "uri": "grpc+tls://gizmosql.example.com:31337",
                 "auth_type": "external",
                 "tls_root_certs": "/etc/certs/ca.pem",
             }
         )

Flight SQL & GizmoSQL Options
-----------------------------

SQLSpec accepts high-level shortcut keys in ``connection_config`` for Flight SQL and
GizmoSQL connections and maps them into the underlying ADBC ``db_kwargs`` options:

- ``username`` and ``password``: Basic authentication credentials.
- ``auth_type``: Set to ``"external"`` on GizmoSQL to trigger browser-based OAuth/OIDC
  authentication (mapped to ``adbc.gizmosql.auth_type`` when using ``adbc_driver_flightsql``
  or passed directly to ``adbc_driver_gizmosql``).
- ``authorization_header``: Custom ``Authorization`` header value.
- ``tls_skip_verify``: Boolean flag to disable TLS certificate verification for local development.
- ``tls_root_certs``, ``mtls_cert_chain``, and ``mtls_private_key``: PEM certificate paths for TLS and mutual TLS.
- ``grpc_max_message_size``: Maximum gRPC message size in bytes.
- ``query_timeout`` and ``fetch_timeout``: RPC timeout values in seconds.
- ``with_cookie_middleware``: Enable Flight SQL cookie middleware (boolean).
- ``headers``: Dictionary of custom RPC call headers (mapped to ``adbc.flight.sql.rpc.call_header.<name>``).
- ``gizmosql_backend``: Set to ``"sqlite"`` when targeting a SQLite-backed GizmoSQL server (defaults to ``"duckdb"``).

Transactions, Savepoints, and Dialect Behavior
==============================================

Savepoints and Nested Transactions
----------------------------------

ADBC connections manage transactions through ``session.begin()``, ``session.commit()``,
``session.rollback()``, and ``session.transaction()``. Nested ``session.transaction()``
blocks use SQL savepoints with dialect-specific syntax:

.. list-table::
   :header-rows: 1
   :widths: 22 28 25 25

   * - Backend Dialect
     - ``create_savepoint(name)``
     - ``rollback_to_savepoint(name)``
     - ``release_savepoint(name)``
   * - PostgreSQL, SQLite, MySQL
     - ``SAVEPOINT <name>``
     - ``ROLLBACK TO SAVEPOINT <name>``
     - ``RELEASE SAVEPOINT <name>``
   * - Oracle
     - ``SAVEPOINT <name>``
     - ``ROLLBACK TO SAVEPOINT <name>``
     - No-op (Oracle does not support ``RELEASE SAVEPOINT``)
   * - IBM Db2
     - ``SAVEPOINT <name> ON ROLLBACK RETAIN CURSORS``
     - ``ROLLBACK TO SAVEPOINT <name>``
     - ``RELEASE SAVEPOINT <name>``
   * - SQL Server (``mssql`` / ``tsql``)
     - ``SAVE TRANSACTION <name>``
     - ``ROLLBACK TRANSACTION <name>``
     - No-op (T-SQL does not release individual savepoints)
   * - DuckDB, BigQuery, Snowflake
     - Raises ``NotImplementedError``
     - Raises ``NotImplementedError``
     - Raises ``NotImplementedError``

Oracle and Db2 Column Name Normalization
----------------------------------------

Oracle and IBM Db2 fold unquoted SQL identifiers to uppercase (`SELECT id, name FROM users`
returns columns named ``ID`` and ``NAME``). When the resolved dialect is ``oracle`` or
``db2``, SQLSpec defaults ``enable_lowercase_column_names=True`` in ``driver_features`` so
implicit-uppercase column names are normalized to lowercase (``row["id"]``, ``row["name"]``)
while preserving quoted mixed-case identifiers (such as ``"MixedCase"``).

To preserve raw uppercase column names from Oracle or Db2, disable the feature in
``driver_features``:

.. code-block:: python

   from sqlspec.adapters.adbc import AdbcConfig

   oracle_raw_columns = AdbcConfig(
       connection_config={"uri": "oracle://system:secret@localhost:1521/FREEPDB1"},
       driver_features={"enable_lowercase_column_names": False},
   )

PostgreSQL UUID and Extension Dialects
--------------------------------------

- **UUID Decoding & Binding**: PostgreSQL ADBC drivers expose ``UUID`` columns as Arrow
  opaque binary extension types. SQLSpec automatically decodes scalar ``UUID`` and
  ``UUID[]`` values to :class:`uuid.UUID` on row APIs (`select`, `select_one`,
  `select_stream`) and adds explicit ``::uuid`` / ``::uuid[]`` casts when binding Python
  :class:`uuid.UUID` parameters. Set ``driver_features={"enable_arrow_extension_types": False}``
  to keep raw bytes on row APIs.
- **Extension Dialect Auto-Upgrade**: On the first PostgreSQL connection, SQLSpec checks
  ``pg_extension`` once and upgrades the dialect to ``pgvector`` (when ``vector`` is
  installed), ``pg_textsearch`` (when ``pg_textsearch`` is installed), or ``paradedb``
  (when ``pg_search`` is installed). Control detection with ``enable_pgvector``,
  ``enable_pg_textsearch``, and ``enable_paradedb`` in ``driver_features``.

Schema Introspection and Statistics
===================================

``AdbcDataDictionary`` combines ADBC's native C-API metadata methods
(``adbc_get_objects``, ``adbc_get_table_schema``) with SQLSpec's central SQL catalog
queries as an automatic fallback:

- When a driver returns complete table, column, and foreign-key payloads via
  ``adbc_get_objects``, SQLSpec normalizes them directly without issuing catalog SQL.
- When native metadata is unsupported, incomplete, or omits index/DDL details,
  ``AdbcDataDictionary`` falls back to the dialect's SQL catalog queries (`postgres`,
  `oracle`, `db2`, `mssql`, `sqlite`, `duckdb`, `bigquery`).
- For Oracle, ``get_optimal_type(driver, "json")`` inspects the server version and
  selects native ``JSON`` on Oracle 21c+, ``BLOB`` on Oracle 12c–19c, and ``CLOB`` on
  older versions.
- ``get_statistics`` wraps ``adbc_get_statistics`` and maps standard ADBC statistic keys
  (`0`–`6`) to canonical ``adbc.statistic.*`` names; drivers without statistics support
  raise :exc:`~sqlspec.exceptions.OperationalError`.

Framework and AI Extension Stores
=================================

``AdbcConfig`` integrates with all SQLSpec synchronous extension stores across every
supported relational and analytical dialect (`postgres`, `oracle`, `db2`, `mssql`,
`sqlite`, `duckdb`, `bigquery`, `snowflake`):

- **Litestar Sessions**: :class:`~sqlspec.adapters.adbc.litestar.store.ADBCStore`
- **Durable Event Queue**: :class:`~sqlspec.adapters.adbc.events.store.AdbcEventQueueStore`
- **Google ADK Sessions & Events**: :class:`~sqlspec.adapters.adbc.adk.store.AdbcADKStore`
- **Google ADK Memory**: :class:`~sqlspec.adapters.adbc.adk.store.AdbcADKMemoryStore`

API Reference
=============

Configuration
-------------

.. autoclass:: sqlspec.adapters.adbc.AdbcConfig
   :members:
   :show-inheritance:

Connection Parameters
---------------------

.. autoclass:: sqlspec.adapters.adbc.AdbcConnectionParams
   :members:
   :show-inheritance:

Driver Features
---------------

.. autoclass:: sqlspec.adapters.adbc.AdbcDriverFeatures
   :members:
   :show-inheritance:

Driver
------

.. autoclass:: sqlspec.adapters.adbc.AdbcDriver
   :members:
   :show-inheritance:

Data Dictionary
---------------

.. autoclass:: sqlspec.adapters.adbc.data_dictionary.AdbcDataDictionary
   :members:
   :show-inheritance:
