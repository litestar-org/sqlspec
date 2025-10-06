============
Installation
============

Installing SQLSpec
------------------

SQLSpec can be installed using pip or uv. The base package includes support for SQLite and the core query processing infrastructure.

Using pip
^^^^^^^^^

.. code-block:: bash

    pip install sqlspec

Using uv (recommended)
^^^^^^^^^^^^^^^^^^^^^^

`uv <https://docs.astral.sh/uv/>`_ is a fast Python package installer and resolver written in Rust. It's significantly faster than pip and provides better dependency resolution.

.. code-block:: bash

    uv pip install sqlspec

Database-Specific Dependencies
-------------------------------

SQLSpec uses optional dependencies to keep the base installation lightweight. Install only the drivers you need for your databases.

PostgreSQL
^^^^^^^^^^

SQLSpec supports multiple PostgreSQL drivers. Choose the one that best fits your needs:

.. tab-set::

    .. tab-item:: asyncpg (recommended for async)

        Fast, async-native PostgreSQL driver with connection pooling.

        .. code-block:: bash

            pip install sqlspec[asyncpg]
            # or
            uv pip install sqlspec[asyncpg]

    .. tab-item:: psycopg (sync and async)

        Modern PostgreSQL adapter with both sync and async support.

        .. code-block:: bash

            pip install sqlspec[psycopg]
            # or
            uv pip install sqlspec[psycopg]

    .. tab-item:: psqlpy (high-performance async)

        High-performance async PostgreSQL driver built with Rust.

        .. code-block:: bash

            pip install sqlspec[psqlpy]
            # or
            uv pip install sqlspec[psqlpy]

SQLite
^^^^^^

SQLite is included in Python's standard library. For async support:

.. code-block:: bash

    pip install sqlspec[aiosqlite]
    # or
    uv pip install sqlspec[aiosqlite]

DuckDB
^^^^^^

DuckDB is an embedded analytical database perfect for OLAP workloads:

.. code-block:: bash

    pip install sqlspec[duckdb]
    # or
    uv pip install sqlspec[duckdb]

MySQL
^^^^^

For async MySQL support:

.. code-block:: bash

    pip install sqlspec[asyncmy]
    # or
    uv pip install sqlspec[asyncmy]

Oracle
^^^^^^

For Oracle Database support (both sync and async):

.. code-block:: bash

    pip install sqlspec[oracledb]
    # or
    uv pip install sqlspec[oracledb]

BigQuery
^^^^^^^^

For Google BigQuery support:

.. code-block:: bash

    pip install sqlspec[bigquery]
    # or
    uv pip install sqlspec[bigquery]

ADBC (Arrow Database Connectivity)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

ADBC provides Arrow-native database access for multiple databases:

.. code-block:: bash

    pip install sqlspec[adbc]
    # or
    uv pip install sqlspec[adbc]

Type-Safe Result Mapping
-------------------------

SQLSpec supports automatic mapping to typed models. Install the library you prefer:

Pydantic
^^^^^^^^

.. code-block:: bash

    pip install sqlspec[pydantic]
    # or
    uv pip install sqlspec[pydantic]

msgspec (recommended for performance)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

    pip install sqlspec[msgspec]
    # or
    uv pip install sqlspec[msgspec]

attrs
^^^^^

.. code-block:: bash

    pip install sqlspec[attrs]
    # or
    uv pip install sqlspec[attrs]

Framework Integrations
----------------------

Litestar
^^^^^^^^

For Litestar web framework integration:

.. code-block:: bash

    pip install sqlspec[litestar]
    # or
    uv pip install sqlspec[litestar]

FastAPI
^^^^^^^

For FastAPI integration:

.. code-block:: bash

    pip install sqlspec[fastapi]
    # or
    uv pip install sqlspec[fastapi]

Flask
^^^^^

For Flask integration:

.. code-block:: bash

    pip install sqlspec[flask]
    # or
    uv pip install sqlspec[flask]

Additional Features
-------------------

SQL File Loading
^^^^^^^^^^^^^^^^

For loading SQL queries from files (aiosql-style):

.. code-block:: bash

    pip install sqlspec[aiosql]
    # or
    uv pip install sqlspec[aiosql]

Observability
^^^^^^^^^^^^^

For OpenTelemetry instrumentation:

.. code-block:: bash

    pip install sqlspec[opentelemetry]
    # or
    uv pip install sqlspec[opentelemetry]

For Prometheus metrics:

.. code-block:: bash

    pip install sqlspec[prometheus]
    # or
    uv pip install sqlspec[prometheus]

Data Export
^^^^^^^^^^^

For Pandas and Polars support:

.. code-block:: bash

    pip install sqlspec[pandas]
    # or
    uv pip install sqlspec[polars]

For storage operations with fsspec or obstore:

.. code-block:: bash

    pip install sqlspec[fsspec]
    # or
    uv pip install sqlspec[obstore]

Performance Optimizations
^^^^^^^^^^^^^^^^^^^^^^^^^

For maximum performance with Rust-based SQL parsing and msgspec:

.. code-block:: bash

    pip install sqlspec[performance]
    # or
    uv pip install sqlspec[performance]

Installing Multiple Extras
---------------------------

You can install multiple optional dependencies at once:

.. code-block:: bash

    pip install sqlspec[asyncpg,pydantic,litestar]
    # or
    uv pip install sqlspec[asyncpg,pydantic,litestar]

Development Installation
------------------------

If you want to contribute to SQLSpec or run the examples:

.. code-block:: bash

    git clone https://github.com/litestar-org/sqlspec.git
    cd sqlspec
    make install
    # or
    uv sync --all-extras --dev

Next Steps
----------

Now that SQLSpec is installed, head over to the :doc:`quickstart` to run your first query!
