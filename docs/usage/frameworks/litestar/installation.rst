============
Installation
============

Install SQLSpec with the Litestar extra and a database adapter.

.. tab-set::

   .. tab-item:: PostgreSQL (asyncpg)

      .. code-block:: bash

         pip install "sqlspec[asyncpg,litestar]"

   .. tab-item:: PostgreSQL (psycopg)

      .. code-block:: bash

         pip install "sqlspec[psycopg,litestar]"

   .. tab-item:: SQLite (async)

      .. code-block:: bash

         pip install "sqlspec[aiosqlite,litestar]"

   .. tab-item:: MySQL

      .. code-block:: bash

         pip install "sqlspec[asyncmy,litestar]"

Requirements
------------

- **Python 3.9+**
- **Litestar 2.0+**
- A compatible async database adapter

Next Steps
----------

Proceed to :doc:`quickstart` to wire the plugin into your Litestar app.
