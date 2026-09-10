=======
Service
=======

Base service classes that wrap a driver session and add pagination, single-row
fetching, existence checks, and transaction helpers.

The five web-framework extensions — ``litestar``, ``fastapi``, ``flask``,
``starlette``, and ``sanic`` — each re-export these two objects, so
``from sqlspec.extensions.litestar import SQLSpecAsyncService`` gives the
identical class. See :doc:`/recipes/service_layer` for usage.

.. currentmodule:: sqlspec.service

SQLSpecAsyncService
===================

.. autoclass:: SQLSpecAsyncService
   :members:
   :show-inheritance:

SQLSpecSyncService
==================

.. autoclass:: SQLSpecSyncService
   :members:
   :show-inheritance:
