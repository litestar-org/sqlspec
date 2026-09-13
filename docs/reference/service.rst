=======
Service
=======

Base service classes provide pagination, single-row fetching, existence checks,
and transactions. Construct them with a caller-owned driver session or a database
``config=``. Config-built helpers acquire short sessions; ``begin_transaction()``
holds one session across calls and releases it on exit.

Each query helper accepts a borrowed ``session=`` override. ``provide_session()``
yields a driver for explicit reuse. The optional ``loader=`` is exposed without
resolving named queries. ``config`` returns the database configuration a service
was built from, or ``None`` for a service built from a session, so one service can
construct another from the same configuration. See the recipe for ownership and
concurrency rules.

``begin_transaction()`` blocks can nest on every service. A nested block runs
inside a savepoint on the outer block's session and never acquires a second
session. It releases the savepoint when it succeeds and rolls back to the
savepoint when it raises, then re-raises, so only the inner work is undone and the
outer block can continue and commit. An adapter without savepoint support raises
``ImproperConfigurationError`` when a nested block is entered.

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
