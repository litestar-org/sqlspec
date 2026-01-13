=========
Starlette
=========

SQLSpec provides a Starlette extension that hooks database sessions into the ASGI
lifecycle. The extension uses Starlette's lifespan context to manage connection pools
and provides middleware for request-scoped sessions.

Installation
============

Install SQLSpec with the Starlette extra:

.. code-block:: bash

   pip install "sqlspec[starlette]"

Basic Setup
===========

Create a SQLSpec instance, register your database config, and attach the plugin to your
Starlette app. The plugin adds middleware that makes sessions available during each request.

.. literalinclude:: /examples/frameworks/starlette/basic_setup.py
   :language: python
   :caption: ``starlette basic setup``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Key Concepts
============

**Lifespan Management**
   The plugin registers startup and shutdown handlers to initialize and close connection
   pools. Pools are created when the application starts and released when it stops.

**Async Sessions**
   Starlette is async-first. Use async adapters like ``AiosqliteConfig``, ``AsyncpgConfig``,
   or ``PsycopgAsyncConfig`` for optimal performance.

**Request State**
   Sessions are stored in ``request.state`` and can be accessed in route handlers or
   middleware that runs after the SQLSpec middleware.

Related Guides
==============

- :doc:`/usage/configuration` for detailed config options.
- :doc:`/reference/adapters` for adapter-specific settings.
- :doc:`fastapi` shares the same underlying integration patterns.
