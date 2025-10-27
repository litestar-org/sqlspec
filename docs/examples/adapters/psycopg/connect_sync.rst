Psycopg Synchronous Connection
==============================

Demonstrate a synchronous PostgreSQL workflow with SQLSpec's Psycopg adapter. Update the DSN via
``SQLSPEC_PSYCOPG_DSN`` to point at your database.

.. code-block:: console

   SQLSPEC_PSYCOPG_DSN=postgresql://user:pass@host/db \
     uv run python docs/examples/adapters/psycopg/connect_sync.py

Source
------

.. literalinclude:: connect_sync.py
   :language: python
   :linenos:
