=======
Service
=======

Base service classes provide pagination, single-row fetching, existence checks,
and transactions. Construct them with a caller-owned driver session or a database
``config=``. Config-built helpers acquire short sessions; ``begin_transaction()``
holds one session across calls and releases it on exit.

Each query helper accepts a borrowed ``session=`` override. ``provide_session()``
yields a driver for explicit reuse. The optional ``loader=`` is exposed without
resolving named queries. See the recipe for ownership and concurrency rules.

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
