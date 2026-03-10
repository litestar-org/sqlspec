======
FastAPI
======

FastAPI integration extending the Starlette plugin with dependency injection
helpers for FastAPI's ``Depends()`` system, including filter dependency builders.

Plugin
======

.. autoclass:: sqlspec.extensions.fastapi.SQLSpecPlugin
   :members:
   :show-inheritance:

Dependency Helpers
==================

.. autofunction:: sqlspec.extensions.fastapi.provide_filters

.. autoclass:: sqlspec.extensions.fastapi.DependencyDefaults
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.extensions.fastapi.FilterConfig
   :members:
   :show-inheritance:

Middleware
=========

.. autoclass:: sqlspec.extensions.fastapi.SQLSpecAutocommitMiddleware
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.extensions.fastapi.SQLSpecManualMiddleware
   :members:
   :show-inheritance:
