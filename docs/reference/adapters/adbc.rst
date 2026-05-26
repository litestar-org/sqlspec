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

Connecting to GizmoSQL
======================

GizmoSQL uses the Flight SQL ADBC driver. SQLSpec accepts ``driver_name="gizmosql"``
as a shorthand for ``adbc_driver_flightsql.dbapi.connect`` and keeps the default
parameter style at qmark placeholders.

.. code-block:: python

   from sqlspec.adapters.adbc import AdbcConfig

   config = AdbcConfig(
       connection_config={
           "driver_name": "gizmosql",
           "uri": "grpc+tls://localhost:31337",
           "username": "admin",
           "password": "secret",
           "tls_skip_verify": True,
           "gizmosql_backend": "duckdb",
       }
   )

``username``, ``password``, ``tls_skip_verify``, and ``authorization_header`` are
translated into Flight SQL ``db_kwargs``. Explicit values in ``db_kwargs`` take
precedence, which lets production deployments pass certificate material without
SQLSpec rewriting it:

.. code-block:: python

   secure_config = AdbcConfig(
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

Set ``gizmosql_backend="sqlite"`` when connecting to a SQLite-backed GizmoSQL
server so runtime dialect detection resolves to SQLite instead of DuckDB. Local
integration tests can use the ``pytest-databases`` GizmoSQL fixture; see
``tests/integration/adapters/adbc/conftest.py`` for the canonical fixture wiring.

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
