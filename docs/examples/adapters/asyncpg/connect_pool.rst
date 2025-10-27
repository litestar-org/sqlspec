AsyncPG Connection Pool
=======================

Configure SQLSpec with the AsyncPG adapter and verify the server version. The DSN defaults to
``postgresql://postgres:postgres@localhost:5432/postgres`` and can be overridden via the
``SQLSPEC_ASYNCPG_DSN`` environment variable.

.. code-block:: console

   SQLSPEC_ASYNCPG_DSN=postgresql://user:pass@host/db \
     uv run python docs/examples/adapters/asyncpg/connect_pool.py

Source
------

.. literalinclude:: connect_pool.py
   :language: python
   :linenos:
