===
API
===

.. currentmodule:: sqlspec.extensions.litestar

Plugin
======

.. autoclass:: SQLSpecPlugin
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

Configuration
=============

.. autoclass:: LitestarConfig
   :members:
   :undoc-members:
   :no-index:

Correlation Middleware
======================

.. autoclass:: CorrelationMiddleware
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

.. py:data:: TRACE_CONTEXT_FALLBACK_HEADERS
   :no-index:

Exception Handlers
==================

.. autofunction:: sqlspec.extensions.litestar.plugin.not_found_error_handler
   :no-index:

.. autofunction:: sqlspec.extensions.litestar.plugin.integrity_error_handler
   :no-index:

Session Stores
==============

.. autoclass:: BaseSQLSpecStore
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

.. currentmodule:: sqlspec.adapters.aiosqlite.litestar

.. autoclass:: AiosqliteStore
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

Channels Backend
================

.. currentmodule:: sqlspec.extensions.litestar

.. autoclass:: SQLSpecChannelsBackend
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:
