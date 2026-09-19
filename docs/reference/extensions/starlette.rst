=========
Starlette
=========

Starlette extension providing middleware-based session management, automatic
transaction handling, and connection pooling lifecycle management.

Configuration
=============

Use ``sqlspec.config.StarletteConfig`` in ``extension_config["starlette"]``.
These settings apply across adapters; no adapter-specific subtype is needed.

.. autoclass:: sqlspec.config.StarletteConfig
   :members:
   :show-inheritance:
   :no-index:

Plugin
======

.. autoclass:: sqlspec.extensions.starlette.SQLSpecPlugin
   :members:
   :show-inheritance:

Middleware
==========

.. autoclass:: sqlspec.extensions.starlette.SQLSpecAutocommitMiddleware
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.extensions.starlette.SQLSpecManualMiddleware
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.extensions.starlette.middleware.CorrelationMiddleware
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.extensions.starlette.middleware.SQLCommenterMiddleware
   :members:
   :show-inheritance:

State
=====

.. autoclass:: sqlspec.extensions.starlette.SQLSpecConfigState
   :members:
   :show-inheritance:

Helpers
=======

.. autofunction:: sqlspec.extensions.starlette.get_connection_from_request

.. autofunction:: sqlspec.extensions.starlette.get_or_create_session
