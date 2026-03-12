============
Installation
============

Install SQLSpec with the extras that match your databases and tooling.

Install bundles
---------------

.. tab-set::

   .. tab-item:: uv

      .. code-block:: bash

         uv add sqlspec

   .. tab-item:: pip

      .. code-block:: bash

         pip install sqlspec

   .. tab-item:: Poetry

      .. code-block:: bash

         poetry add sqlspec

   .. tab-item:: PDM

      .. code-block:: bash

         pdm add sqlspec

Performance bundle
~~~~~~~~~~~~~~~~~~

For production deployments, install the ``performance`` extra for Rust-based SQL
parsing and high-speed serialization, or the ``mypyc`` extra for C-compiled internals:

.. tab-set::

   .. tab-item:: uv

      .. code-block:: bash

         uv add "sqlspec[performance]"

   .. tab-item:: pip

      .. code-block:: bash

         pip install "sqlspec[performance]"

   .. tab-item:: Poetry

      .. code-block:: bash

         poetry add "sqlspec[performance]"

   .. tab-item:: PDM

      .. code-block:: bash

         pdm add "sqlspec[performance]"

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
   * - ``mypyc``
     - ``sqlglot[c]``
     - C-compiled sqlglot internals for faster SQL parsing.
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

.. tab-set::

   .. tab-item:: uv

      .. code-block:: bash

         uv add "sqlspec[asyncpg,msgspec,litestar]"

   .. tab-item:: pip

      .. code-block:: bash

         pip install "sqlspec[asyncpg,msgspec,litestar]"

   .. tab-item:: Poetry

      .. code-block:: bash

         poetry add "sqlspec[asyncpg,msgspec,litestar]"

   .. tab-item:: PDM

      .. code-block:: bash

         pdm add "sqlspec[asyncpg,msgspec,litestar]"

Next steps
----------

Head to :doc:`quickstart` to run your first query.
