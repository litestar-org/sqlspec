============
Installation
============

Install SQLSpec with the Litestar extra and a database adapter.

.. tab-set::

   .. tab-item:: PostgreSQL (asyncpg)

      .. tab-set::

         .. tab-item:: uv

            .. code-block:: bash

               uv add "sqlspec[asyncpg,litestar]"

         .. tab-item:: pip

            .. code-block:: bash

               pip install "sqlspec[asyncpg,litestar]"

         .. tab-item:: Poetry

            .. code-block:: bash

               poetry add "sqlspec[asyncpg,litestar]"

         .. tab-item:: PDM

            .. code-block:: bash

               pdm add "sqlspec[asyncpg,litestar]"

   .. tab-item:: PostgreSQL (psycopg)

      .. tab-set::

         .. tab-item:: uv

            .. code-block:: bash

               uv add "sqlspec[psycopg,litestar]"

         .. tab-item:: pip

            .. code-block:: bash

               pip install "sqlspec[psycopg,litestar]"

         .. tab-item:: Poetry

            .. code-block:: bash

               poetry add "sqlspec[psycopg,litestar]"

         .. tab-item:: PDM

            .. code-block:: bash

               pdm add "sqlspec[psycopg,litestar]"

   .. tab-item:: SQLite (async)

      .. tab-set::

         .. tab-item:: uv

            .. code-block:: bash

               uv add "sqlspec[aiosqlite,litestar]"

         .. tab-item:: pip

            .. code-block:: bash

               pip install "sqlspec[aiosqlite,litestar]"

         .. tab-item:: Poetry

            .. code-block:: bash

               poetry add "sqlspec[aiosqlite,litestar]"

         .. tab-item:: PDM

            .. code-block:: bash

               pdm add "sqlspec[aiosqlite,litestar]"

   .. tab-item:: MySQL

      .. tab-set::

         .. tab-item:: uv

            .. code-block:: bash

               uv add "sqlspec[asyncmy,litestar]"

         .. tab-item:: pip

            .. code-block:: bash

               pip install "sqlspec[asyncmy,litestar]"

         .. tab-item:: Poetry

            .. code-block:: bash

               poetry add "sqlspec[asyncmy,litestar]"

         .. tab-item:: PDM

            .. code-block:: bash

               pdm add "sqlspec[asyncmy,litestar]"

Requirements
------------

- **Python 3.9+**
- **Litestar 2.0+**
- A compatible async database adapter

Next Steps
----------

Proceed to :doc:`quickstart` to wire the plugin into your Litestar app.
