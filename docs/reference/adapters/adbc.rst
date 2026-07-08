====
ADBC
====

Arrow Database Connectivity adapter providing native Arrow result handling
without conversion overhead. SQLSpec can load the ADBC drivers for PostgreSQL,
SQLite, DuckDB, BigQuery, Snowflake, Flight SQL, and GizmoSQL from one
``AdbcConfig`` surface.

Connection Configuration
========================

ADBC configuration is driver-specific at the transport layer, so
``connection_config`` accepts both SQLSpec aliases and the keyword arguments
expected by the selected ADBC driver. SQLSpec normalizes common aliases and URI
schemes before calling the driver's ``dbapi.connect`` function.

Supported Backends
------------------

.. list-table::
   :header-rows: 1
   :widths: 20 24 28 28

   * - Backend
     - ``driver_name`` aliases
     - ADBC driver
     - Notes
   * - PostgreSQL
     - ``postgres``, ``postgresql``, ``pg``
     - ``adbc_driver_postgresql``
     - Numeric parameters; optional pgvector and ParadeDB dialect detection.
   * - SQLite
     - ``sqlite``, ``sqlite3``
     - ``adbc_driver_sqlite``
     - Qmark parameters; ``sqlite://`` URIs are normalized to file paths.
   * - DuckDB
     - ``duckdb``
     - ``adbc_driver_duckdb``
     - Qmark and numeric parameters; ``duckdb://`` URIs are normalized to paths.
   * - BigQuery
     - ``bigquery``, ``bq``
     - ``adbc_driver_bigquery``
     - Named ``@`` parameters; ``project_id``, ``dataset_id``, and ``token``
       are lifted into ``db_kwargs``.
   * - Snowflake
     - ``snowflake``, ``sf``
     - ``adbc_driver_snowflake``
     - Qmark and numeric parameters.
   * - Flight SQL
     - ``flightsql``, ``grpc``
     - ``adbc_driver_flightsql``
     - Generic Flight SQL connections default to SQLite-style statement handling.
   * - GizmoSQL
     - ``gizmosql``, ``gizmo``
     - ``adbc_driver_flightsql``
     - Flight SQL transport with DuckDB dialect by default, or SQLite when
       ``gizmosql_backend="sqlite"``.

Examples
--------

PostgreSQL:

.. code-block:: python

   from sqlspec.adapters.adbc import AdbcConfig

   postgres = AdbcConfig(
       connection_config={
           "driver_name": "postgres",
           "uri": "postgresql://user:password@localhost:5432/app",
       }
   )

SQLite:

.. code-block:: python

   from sqlspec.adapters.adbc import AdbcConfig

   sqlite = AdbcConfig(
       connection_config={
           "driver_name": "sqlite",
           "uri": "sqlite:///tmp/app.db",
       }
   )

DuckDB:

.. code-block:: python

   from sqlspec.adapters.adbc import AdbcConfig

   duckdb = AdbcConfig(
       connection_config={
           "driver_name": "duckdb",
           "path": "/tmp/app.duckdb",
       }
   )

BigQuery:

.. code-block:: python

   from sqlspec.adapters.adbc import AdbcConfig

   bigquery = AdbcConfig(
       connection_config={
           "driver_name": "bigquery",
           "project_id": "analytics-project",
           "dataset_id": "events",
       }
   )

Flight SQL:

.. code-block:: python

   from sqlspec.adapters.adbc import AdbcConfig

   flightsql = AdbcConfig(
       connection_config={
           "driver_name": "flightsql",
           "uri": "grpc+tls://flightsql.example.com:31337",
       }
   )

GizmoSQL:

.. code-block:: python

   from sqlspec.adapters.adbc import AdbcConfig

   gizmosql = AdbcConfig(
       connection_config={
           "driver_name": "gizmosql",
           "uri": "grpc+tls://localhost:31337",
           "username": "admin",
           "password": "secret",
           "tls_skip_verify": True,
           "gizmosql_backend": "duckdb",
       }
   )

GizmoSQL Notes
--------------

GizmoSQL runs over the Flight SQL ADBC driver. SQLSpec maps the common
``username``, ``password``, ``tls_skip_verify``, and ``authorization_header``
shortcuts into Flight SQL ``db_kwargs`` before opening the connection. For
lower-level Flight SQL options, pass ``db_kwargs`` directly:

.. code-block:: python

   from sqlspec.adapters.adbc import AdbcConfig

   secure_gizmosql = AdbcConfig(
       connection_config={
           "driver_name": "gizmosql",
           "uri": "grpc+tls://gizmosql.example.com:31337",
           "db_kwargs": {
               "username": "admin",
               "password": "secret",
               "adbc.flight.sql.client_option.tls_root_certs": "/etc/certs/ca.pem",
               "adbc.flight.sql.client_option.mtls_cert_chain": "/etc/certs/client.pem",
               "adbc.flight.sql.client_option.mtls_private_key": "/etc/certs/client.key",
           },
       }
   )

Use ``gizmosql_backend="sqlite"`` only when the target GizmoSQL server was
started with SQLite as its database backend. DuckDB remains the default dialect
for GizmoSQL.

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
(SQLite, DuckDB, BigQuery, Snowflake, GizmoSQL) skip detection entirely.

.. note::

   ADBC returns vector data as strings (e.g. ``"[0.1,0.2,0.3]"``).
   The ``pgvector`` Python package is not required for querying vector data.
   It only affects the *default* value of ``enable_pgvector`` — when the package
   is installed, detection is enabled automatically. You can always set
   ``enable_pgvector=True`` explicitly in ``driver_features`` to enable
   detection without the package installed.

See the :doc:`Dialects <../dialects>` reference for full operator details.

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

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.adbc.data_dictionary.AdbcDataDictionary
   :members:
   :show-inheritance:

Native Metadata And Statistics
==============================

``AdbcDataDictionary`` keeps SQLSpec's central dialect data-dictionary queries
as the canonical fallback. It detects the database behind the ADBC connection
and uses the same dialect query registry as the native adapter when ADBC
metadata is unsupported, incomplete, or too broad. The standardized ADBC
metadata APIs (``adbc_get_objects``, ``adbc_get_table_schema``) are an optional
overlay when the driver returns complete table, column, and foreign-key
payloads that can be normalized to SQLSpec's public metadata types.

``get_statistics`` is separate from the shared data dictionary surface because
SQLSpec does not define a portable SQL statistics contract. It wraps
``adbc_get_statistics`` directly; unsupported drivers raise
:exc:`sqlspec.exceptions.OperationalError`.

In the replacement data dictionary, ADBC statistics are also exposed through the
opt-in system metadata namespace as transport metadata. This does not make ADBC
a lossless DDL or dependency source; dialect query packs remain canonical for
DDL-grade metadata.

.. list-table:: ADBC native metadata support (driver manager 1.11.0)
   :header-rows: 1

   * - Backend
     - Metadata behavior
     - Statistics behavior
   * - PostgreSQL
     - Native overlay when available; central PostgreSQL SQL fallback
     - Native (approximate; run ``ANALYZE`` for fresh estimates)
   * - SQLite
     - Native overlay when available; central SQLite SQL fallback
       (type names populated; nullability unreliable)
     - Unsupported (raises ``OperationalError``)
   * - DuckDB
     - Native overlay for single tables when schema enrichment succeeds;
       schema-wide column listings use central DuckDB SQL
     - Unsupported (raises ``OperationalError``)
   * - Flight SQL / GizmoSQL
     - Native overlay is server dependent; central dialect fallback applies
       when the backend dialect is mapped. DuckDB-backed GizmoSQL exposes
       catalogs, schemas, tables, columns, and constraints through
       ``GetObjects`` when the server returns a complete payload; incomplete
       constraint or nullability metadata falls back to SQLSpec's central
       dialect SQL.
     - Server dependent
   * - BigQuery
     - Central BigQuery SQL fallback
     - Unverified

Precision limits:

- ADBC native name filters are SQL ``LIKE`` patterns; SQLSpec post-filters
  native results by exact table name, but schema filters containing ``_`` or
  ``%`` may match more broadly on the server side before fallback.
- The SQLite driver reports ``xdbc_is_nullable`` as ``YES`` even for
  ``NOT NULL`` columns.
- Index metadata always uses SQL introspection; ADBC GetObjects has no
  portable index representation.
- ``get_statistics`` maps the standard ADBC statistic keys 0-6 to their
  canonical names (``adbc.statistic.row_count`` and friends); driver-specific
  keys are reported numerically.
