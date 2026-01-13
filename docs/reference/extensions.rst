==========
Extensions
==========

SQLSpec extensions integrate the registry with frameworks and services such as
Litestar and Google ADK. Use these extensions for dependency injection, lifecycle
hooks, and application-facing helpers.

.. currentmodule:: sqlspec.extensions

Overview
========

- **Google ADK**: Session, event, and memory storage.
- **Litestar**: Plugin-based integration with dependency injection and lifecycle management.
- **FastAPI/Flask/Starlette/Sanic**: Framework helpers (see usage docs).

ADK Example
===========

.. literalinclude:: /examples/extensions/adk/memory_store.py
   :language: python
   :caption: ``adk memory store``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Litestar Example
================

.. literalinclude:: /examples/extensions/litestar/plugin_setup.py
   :language: python
   :caption: ``litestar plugin``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Reference Modules
=================

.. currentmodule:: sqlspec.extensions.adk

.. autoclass:: SQLSpecSessionService
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

.. currentmodule:: sqlspec.extensions.litestar

.. autoclass:: SQLSpecPlugin
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

See Also
========

- :doc:`/extensions/adk/index`
- :doc:`/usage/frameworks/litestar/index`
- :doc:`/usage/framework_integrations`
