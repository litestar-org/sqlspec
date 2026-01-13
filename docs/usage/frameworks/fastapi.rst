=======
FastAPI
=======

SQLSpec provides a FastAPI extension that wires database sessions into the request lifecycle
using standard dependency injection. The extension manages connection pools and ensures proper
cleanup between requests.

Installation
============

Install SQLSpec with the FastAPI extra:

.. code-block:: bash

   pip install "sqlspec[fastapi]"

Basic Setup
===========

Create a SQLSpec instance, register your database config, and attach the plugin to your
FastAPI app. The plugin provides a ``provide_session`` dependency that yields a session
for each request.

.. literalinclude:: /examples/frameworks/fastapi/basic_setup.py
   :language: python
   :caption: ``fastapi basic setup``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Key Concepts
============

**Dependency Injection**
   Use ``Depends(db_ext.provide_session())`` to inject a session into your route handlers.
   The session is scoped to the request and automatically cleaned up afterward.

**Session Lifecycle**
   Sessions are created at the start of each request and closed when the request completes.
   For long-running operations, consider using background tasks with their own session.

**Multiple Databases**
   Register multiple configs with different names and create separate dependencies for each:

   .. code-block:: python

      sqlspec.add_config(primary_config, name="primary")
      sqlspec.add_config(analytics_config, name="analytics")

      primary_dep = db_ext.provide_session("primary")
      analytics_dep = db_ext.provide_session("analytics")

Related Guides
==============

- :doc:`/usage/configuration` for detailed config options.
- :doc:`/reference/adapters` for adapter-specific settings.
