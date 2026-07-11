=================
Utility Functions
=================

SQLSpec exposes small, reusable helpers for runtime type narrowing, configuration,
identifier generation, optional dependency loading, sync/async interoperation,
deprecation, and fixture files. These modules are supported public APIs; import
helpers from their defining ``sqlspec.utils`` module.

Type Guards
===========

Runtime predicates in :mod:`sqlspec.utils.type_guards` narrow schema, mapping,
SQLGlot expression, filter, cursor, and optional-library values.

.. automodule:: sqlspec.utils.type_guards
   :members:

Environment and Configuration
=============================

.. automodule:: sqlspec.utils.env
   :members: get_env, get_env_with_aliases, get_config_val, get_config_val_with_aliases, is_env_set

UUIDs and Compact Identifiers
=============================

.. automodule:: sqlspec.utils.uuids
   :members:

Module and Optional Dependency Loading
======================================

.. automodule:: sqlspec.utils.module_loader
   :members:

Sync and Async Interoperation
============================

The executor controls are process-wide knobs. Use them when adapting a synchronous
driver or callback to an async application; ordinary application code should
prefer its framework's native concurrency primitives.

.. automodule:: sqlspec.utils.sync_tools
   :members: ASYNC_THREAD_LIMIT_ENV, DEFAULT_ASYNC_THREAD_LIMIT, CapacityLimiter, async_, await_, enable_default_async_thread_pool, ensure_async_, get_default_async_executor, get_next, run_, set_default_async_executor, shutdown_default_async_executor, with_ensure_async_

Deprecation Helpers
===================

.. automodule:: sqlspec.utils.deprecation
   :members: deprecated, warn_deprecation

Fixture Files
=============

.. automodule:: sqlspec.utils.fixtures
   :members: open_fixture_sync, open_fixture_async, write_fixture_sync, write_fixture_async
