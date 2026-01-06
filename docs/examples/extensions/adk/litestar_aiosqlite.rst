ADK + Litestar Endpoint
=======================

Initialize ``SQLSpecSessionService`` and ``SQLSpecMemoryService`` inside Litestar and expose
``/sessions`` plus ``/memories`` endpoints backed by AioSQLite.

.. code-block:: console

   uv run python docs/examples/extensions/adk/litestar_aiosqlite.py

Source
------

.. literalinclude:: litestar_aiosqlite.py
   :language: python
   :linenos:
