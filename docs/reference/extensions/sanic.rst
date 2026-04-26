=====
Sanic
=====

Sanic extension providing app/request context integration, request-scoped
session management, transaction handling, correlation IDs, SQLCommenter, and
connection pool lifecycle management.

Plugin
======

.. autoclass:: sqlspec.extensions.sanic.SQLSpecPlugin
   :members:
   :show-inheritance:

State
=====

.. autoclass:: sqlspec.extensions.sanic.SanicConfigState
   :members:
   :show-inheritance:

Helpers
=======

.. autofunction:: sqlspec.extensions.sanic.get_connection_from_request

.. autofunction:: sqlspec.extensions.sanic.get_or_create_session
