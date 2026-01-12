============
Installation
============

Install SQLSpec with the extras that match your databases and tooling.

Install bundles
---------------

.. tab-set::

   .. tab-item:: Core

      .. code-block:: bash

         uv pip install sqlspec
         # or
         pip install sqlspec

   .. tab-item:: PostgreSQL

      .. code-block:: bash

         uv pip install "sqlspec[asyncpg]"
         uv pip install "sqlspec[psycopg]"
         uv pip install "sqlspec[psqlpy]"

   .. tab-item:: SQLite

      .. code-block:: bash

         uv pip install "sqlspec[aiosqlite]"

   .. tab-item:: MySQL

      .. code-block:: bash

         uv pip install "sqlspec[asyncmy]"
         uv pip install "sqlspec[mysql-connector]"
         uv pip install "sqlspec[pymysql]"

   .. tab-item:: Oracle

      .. code-block:: bash

         uv pip install "sqlspec[oracledb]"

   .. tab-item:: Analytics

      .. code-block:: bash

         uv pip install "sqlspec[duckdb]"
         uv pip install "sqlspec[bigquery]"
         uv pip install "sqlspec[spanner]"
         uv pip install "sqlspec[adbc]"

   .. tab-item:: Frameworks

      .. code-block:: bash

         uv pip install "sqlspec[litestar]"
         uv pip install "sqlspec[fastapi]"
         uv pip install "sqlspec[flask]"

   .. tab-item:: Observability

      .. code-block:: bash

         uv pip install "sqlspec[opentelemetry]"
         uv pip install "sqlspec[prometheus]"

   .. tab-item:: Data Export

      .. code-block:: bash

         uv pip install "sqlspec[pandas]"
         uv pip install "sqlspec[polars]"
         uv pip install "sqlspec[fsspec]"
         uv pip install "sqlspec[obstore]"

   .. tab-item:: Types & Serialization

      .. code-block:: bash

         uv pip install "sqlspec[msgspec]"
         uv pip install "sqlspec[pydantic]"
         uv pip install "sqlspec[attrs]"
         uv pip install "sqlspec[orjson]"

   .. tab-item:: Performance

      .. code-block:: bash

         uv pip install "sqlspec[performance]"

Package groups
--------------

.. list-table::
   :header-rows: 1
   :widths: 18 42 40

   * - Extra
     - Includes
     - What it is for
   * - ``adbc``
     - ``adbc_driver_manager``, ``pyarrow``
     - Arrow Database Connectivity drivers.
   * - ``adk``
     - ``google-adk``
     - Google ADK storage extension.
   * - ``aioodbc``
     - ``aioodbc``
     - Async ODBC connections.
   * - ``aiosqlite``
     - ``aiosqlite``
     - Async SQLite driver.
   * - ``alloydb``
     - ``google-cloud-alloydb-connector``
     - AlloyDB connector.
   * - ``asyncmy``
     - ``asyncmy``
     - Async MySQL driver.
   * - ``asyncpg``
     - ``asyncpg``
     - Async PostgreSQL driver.
   * - ``attrs``
     - ``attrs``, ``cattrs``
     - Result mapping with attrs models.
   * - ``bigquery``
     - ``google-cloud-bigquery``, ``google-cloud-storage``
     - BigQuery adapter dependencies.
   * - ``cli``
     - ``rich-click``, ``tomli`` (Py<3.11)
     - CLI enhancements.
   * - ``cloud-sql``
     - ``cloud-sql-python-connector``
     - Google Cloud SQL connector.
   * - ``cockroachdb``
     - ``psycopg[binary,pool]``, ``asyncpg``
     - CockroachDB drivers.
   * - ``duckdb``
     - ``duckdb``
     - DuckDB adapter.
   * - ``fastapi``
     - ``fastapi``
     - FastAPI integration helpers.
   * - ``flask``
     - ``flask``
     - Flask integration helpers.
   * - ``fsspec``
     - ``fsspec``
     - Storage helpers using fsspec.
   * - ``litestar``
     - ``litestar``
     - Litestar integration helpers.
   * - ``msgspec``
     - ``msgspec``
     - High-performance result mapping.
   * - ``mysql-connector``
     - ``mysql-connector-python``
     - MySQL connector driver.
   * - ``nanoid``
     - ``fastnanoid``
     - NanoID utilities.
   * - ``obstore``
     - ``obstore``
     - Object storage helpers.
   * - ``opentelemetry``
     - ``opentelemetry-instrumentation``
     - OpenTelemetry instrumentation.
   * - ``oracledb``
     - ``oracledb``
     - Oracle Database adapter.
   * - ``orjson``
     - ``orjson``
     - Fast JSON serialization.
   * - ``pandas``
     - ``pandas``, ``pyarrow``
     - Pandas data export.
   * - ``performance``
     - ``sqlglot[rs]``, ``msgspec``
     - Rust-based SQL parsing + msgspec.
   * - ``polars``
     - ``polars``, ``pyarrow``
     - Polars data export.
   * - ``prometheus``
     - ``prometheus-client``
     - Prometheus metrics.
   * - ``psqlpy``
     - ``psqlpy``
     - Async PostgreSQL (Rust).
   * - ``psycopg``
     - ``psycopg[binary,pool]``
     - Sync/async PostgreSQL driver.
   * - ``pydantic``
     - ``pydantic``, ``pydantic-extra-types``
     - Result mapping with Pydantic models.
   * - ``pymssql``
     - ``pymssql``
     - MSSQL driver.
   * - ``pymysql``
     - ``pymysql``
     - MySQL driver.
   * - ``spanner``
     - ``google-cloud-spanner``
     - Cloud Spanner adapter.
   * - ``uuid``
     - ``uuid-utils``
     - UUID helpers.

Multiple extras
---------------

.. code-block:: bash

   uv pip install "sqlspec[asyncpg,msgspec,litestar]"
   # or
   pip install "sqlspec[asyncpg,msgspec,litestar]"

Next steps
----------

Head to :doc:`quickstart` to run your first query.
