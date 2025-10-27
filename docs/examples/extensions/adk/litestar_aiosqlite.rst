ADK + Litestar Endpoint
=======================

Initialize ``SQLSpecSessionService`` inside Litestar and expose a ``/sessions`` endpoint backed by
AioSQLite.

.. code-block:: console

   uv run python docs/examples/extensions/adk/litestar_aiosqlite.py

Source
------

.. literalinclude:: litestar_aiosqlite.py
   :language: python
   :linenos:
