ADK Runner with Memory (AioSQLite)
==================================

Run an ADK ``Runner`` with SQLSpec-backed session and memory services, then
persist memories from the completed session.

This example requires Google ADK credentials (for example, a configured API key)
and network access to the model provider.

.. code-block:: console

   uv run python docs/examples/extensions/adk/runner_memory_aiosqlite.py

Source
------

.. literalinclude:: runner_memory_aiosqlite.py
   :language: python
   :linenos:
