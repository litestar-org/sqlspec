=====
Flask
=====

SQLSpec provides a Flask extension that manages database connections within the Flask
request lifecycle. The extension registers connection pool setup and teardown with
Flask's application context hooks, supporting both synchronous and asynchronous adapters.

Installation
============

Install SQLSpec with the Flask extra:

.. tab-set::

   .. tab-item:: uv

      .. code-block:: bash

         uv add "sqlspec[flask]"

   .. tab-item:: pip

      .. code-block:: bash

         pip install "sqlspec[flask]"

   .. tab-item:: Poetry

      .. code-block:: bash

         poetry add "sqlspec[flask]"

   .. tab-item:: PDM

      .. code-block:: bash

         pdm add "sqlspec[flask]"

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

Transaction Modes
=================

Configure the commit mode under ``extension_config["flask"]``:

``manual`` (default)
   SQLSpec manages connection lifecycle within the request context. Your route handler
   commits or rolls back explicitly.

``autocommit``
   SQLSpec automatically commits transactions for 2xx responses and rolls back for 4xx/5xx
   responses or unhandled exceptions.

``autocommit_include_redirect``
   Extends autocommit to also commit on redirect responses (2xx and 3xx).

.. code-block:: python

   from sqlspec.adapters.sqlite import SqliteConfig

   config = SqliteConfig(
       connection_config={"database": "app.db"},
       extension_config={
           "flask": {
               "commit_mode": "autocommit",
               "extra_rollback_statuses": {409},
               "session_key": "db",
           }
       },
   )

Multiple Databases
==================

For multiple databases, assign unique ``session_key``, ``connection_key``, and ``pool_key``
settings under ``extension_config["flask"]``. Retrieve sessions by passing the key to
``plugin.get_session()``:

.. literalinclude:: /examples/frameworks/flask/multi_database.py
   :language: python
   :caption: ``flask multi database``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Async Adapter Support
=====================

While Flask operates synchronously by default, SQLSpec provides seamless support for
asynchronous database adapters (such as ``AsyncpgConfig`` or ``AiosqliteConfig``) via
an internal AnyIO portal runner. In async configurations, ``get_session()`` handles the
event loop bridge transparently.

Related Guides
==============

- :doc:`/usage/configuration` for detailed config options.
- :doc:`/reference/adapters` for adapter-specific settings.
