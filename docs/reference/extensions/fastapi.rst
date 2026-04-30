======
FastAPI
======

FastAPI integration with dependency injection helpers for FastAPI's ``Depends()``
system, including filter dependency builders.

Plugin
======

.. autoclass:: sqlspec.extensions.fastapi.SQLSpecPlugin
   :members:
   :show-inheritance:

Dependency Helpers
==================

``provide_filters()`` supports the same ``orderBy`` alias contract as the
Litestar provider. Camel-case query values are accepted by default for
configured ``sort_field`` values. Use ``sort_field_aliases`` for explicit API
names, or set ``sort_field_camelize=False`` to accept only raw configured
values. Alias values normalize to fields from ``sort_field`` before
``OrderByFilter`` is created, so the SQL-facing sort allowlist remains strict.

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
   :no-index:

.. autoclass:: sqlspec.extensions.fastapi.SQLSpecManualMiddleware
   :members:
   :show-inheritance:
   :no-index:
