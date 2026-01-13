=====
Flask
=====

SQLSpec provides a Flask extension that manages database connections within the Flask
request lifecycle. The extension registers connection pool setup and teardown with
Flask's application context hooks.

Installation
============

Install SQLSpec with the Flask extra:

.. code-block:: bash

   pip install "sqlspec[flask]"

Basic Setup
===========

Create a SQLSpec instance, register your database config, and attach the plugin to your
Flask app. Use ``plugin.get_session()`` inside request handlers to obtain a session.

.. literalinclude:: /examples/frameworks/flask/basic_setup.py
   :language: python
   :caption: ``flask basic setup``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Key Concepts
============

**Request-Scoped Sessions**
   Sessions obtained via ``get_session()`` are bound to the current request context.
   They are automatically closed when the request finishes.

**Application Factory Pattern**
   When using the factory pattern, initialize the plugin in your ``create_app`` function:

   .. code-block:: python

      def create_app():
          app = Flask(__name__)
          sqlspec = SQLSpec()
          sqlspec.add_config(SqliteConfig(...))
          plugin = SQLSpecPlugin(sqlspec, app)
          return app

**Sync Execution**
   Flask operates synchronously by default. Use sync adapters like ``SqliteConfig`` or
   ``PsycopgSyncConfig`` for straightforward integration.

Related Guides
==============

- :doc:`/usage/configuration` for detailed config options.
- :doc:`/reference/adapters` for adapter-specific settings.
