==========
Quickstart
==========

Wire SQLSpec stores into your ADK agent to persist sessions and memory across restarts.

How It Works
============

1. Create a SQLSpec database config.
2. Initialize ADK stores (session, memory, event) backed by that config.
3. Pass the stores to your ADK agent.

Session Store
=============

The session store persists agent state between conversations. When a user returns,
the agent can resume from where it left off.

.. literalinclude:: /examples/extensions/adk/memory_store.py
   :language: python
   :caption: ``adk session store``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Memory Store Integration
========================

The memory store retains context that the agent can reference later. This enables
long-term memory across sessions.

.. literalinclude:: /examples/extensions/adk/tool_integration.py
   :language: python
   :caption: ``adk memory integration``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Schema Setup
============

Stores create their tables automatically on first use. For production, run migrations
ahead of time:

.. code-block:: python

   await session_store.create_tables()
   await memory_store.create_tables()

Next Steps
==========

- :doc:`backends` for adapter-specific configuration.
- :doc:`schema` for table layouts and indexes.
