============
Installation
============

Install SQLSpec with a Litestar-compatible adapter and the Litestar extras.

.. code-block:: console

   uv pip install "sqlspec[asyncpg,litestar]" litestar

Replace ``asyncpg`` with your preferred adapter (``psycopg``, ``aiosqlite``,
``sqlite``, etc.).
