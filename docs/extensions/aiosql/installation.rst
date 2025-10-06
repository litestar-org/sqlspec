============
Installation
============

Requirements
============

Python Version
--------------

SQLSpec aiosql integration requires:

- **Python 3.10 or higher**
- **SQLSpec** with a supported database adapter

Optional Dependencies
---------------------

- **aiosql** - Required only if using the aiosql adapter (not needed for SQLFileLoader)
- **fsspec** - For cloud storage support with SQLFileLoader

Installing SQLFileLoader
=========================

The SQLFileLoader is included in the base SQLSpec package (no additional dependencies needed):

.. code-block:: bash

   # Base installation (local files only)
   pip install sqlspec[asyncpg]

   # With cloud storage support
   pip install sqlspec[asyncpg,fsspec]

Installing aiosql Adapter
==========================

If you have existing aiosql code or need aiosql operators:

.. code-block:: bash

   # Install SQLSpec with aiosql
   pip install sqlspec[asyncpg] aiosql

   # Or with uv
   uv pip install sqlspec[asyncpg] aiosql

Database Adapters
=================

Install with your preferred database adapter:

PostgreSQL
----------

.. code-block:: bash

   # AsyncPG (recommended)
   pip install sqlspec[asyncpg]

   # Psycopg
   pip install sqlspec[psycopg]

   # Psqlpy
   pip install sqlspec[psqlpy]

SQLite
------

.. code-block:: bash

   # Sync (included in Python)
   pip install sqlspec

   # Async
   pip install sqlspec[aiosqlite]

MySQL / MariaDB
---------------

.. code-block:: bash

   pip install sqlspec[asyncmy]

Other Databases
---------------

.. code-block:: bash

   # Oracle
   pip install sqlspec[oracledb]

   # DuckDB
   pip install sqlspec[duckdb]

Cloud Storage Support
=====================

For loading SQL files from cloud storage (SQLFileLoader only):

.. code-block:: bash

   # S3, GCS, Azure, HTTP
   pip install sqlspec[asyncpg,fsspec]

   # With S3 credentials
   pip install sqlspec[asyncpg,fsspec,s3fs]

   # With Google Cloud Storage
   pip install sqlspec[asyncpg,fsspec,gcsfs]

Verification
============

Verify SQLFileLoader installation:

.. code-block:: python

   from sqlspec.loader import SQLFileLoader

   loader = SQLFileLoader()
   print("✅ SQLFileLoader installed successfully")

Verify aiosql adapter installation:

.. code-block:: python

   try:
       import aiosql
       from sqlspec.extensions.aiosql import AiosqlAsyncAdapter, AiosqlSyncAdapter
       print("✅ aiosql adapter installed successfully")
   except ImportError as e:
       print(f"❌ aiosql not installed: {e}")
       print("Run: pip install aiosql")

Development Installation
========================

For contributing to SQLSpec:

.. code-block:: bash

   git clone https://github.com/litestar-org/sqlspec.git
   cd sqlspec
   make install
   # or
   uv sync --all-extras --dev

Running Tests
-------------

Run aiosql integration tests:

.. code-block:: bash

   # Run all tests
   uv run pytest tests/integration/extensions/test_aiosql/ -v

   # Run specific test file
   uv run pytest tests/integration/test_loader.py -v

Next Steps
==========

Now that the aiosql integration is installed, proceed to the :doc:`quickstart` guide!

See Also
========

- :doc:`quickstart` - Get started in 5 minutes
- :doc:`usage` - Learn about SQLFileLoader features
- :doc:`/getting_started/installation` - General SQLSpec installation
