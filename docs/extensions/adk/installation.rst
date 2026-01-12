============
Installation
============

Install SQLSpec with the adapter you plan to use plus Google ADK dependencies.

.. code-block:: console

   uv pip install "sqlspec[asyncpg]" google-genai

Replace ``asyncpg`` with ``psycopg``, ``psqlpy``, ``asyncmy``, ``sqlite``,
``aiosqlite``, ``duckdb``, ``bigquery``, ``oracledb``, or ``adbc`` as needed.
