====================
Google ADK Extension
====================

SQLSpec provides an ADK extension for session, event, and memory storage with
SQL-backed persistence.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   quickstart
   api
   adapters
   backends
   migrations
   schema

Example
=======

.. literalinclude:: /examples/extensions/adk/memory_store.py
   :language: python
   :caption: ``adk memory store``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Highlights
==========

- Session and memory persistence for Google ADK.
- Multiple database backends via SQLSpec adapters.
- Async and sync stores with the same API surface.
- Typed records with configurable table names.
