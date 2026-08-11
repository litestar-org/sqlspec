========
Litestar
========

Full Litestar integration with plugin lifecycle, dependency injection,
CLI commands, channels backend, and key-value store.

Plugin
======

.. autoclass:: sqlspec.extensions.litestar.SQLSpecPlugin
   :members:
   :show-inheritance:

Configuration
=============

.. autoclass:: sqlspec.extensions.litestar.LitestarConfig
   :members:
   :show-inheritance:

Channels Backend
================

``SQLSpecChannelsBackend`` buffers decoded output from every asynchronous
``EventChannel`` transport. Pass ``output_queue_capacity`` to bound that buffer;
the default ``None`` remains unbounded. When full, the backend discards the
oldest decoded message before acknowledging and retaining the newest one.
``output_queue_depth`` reports the current pending count and
``dropped_message_count`` reports cumulative overflow drops for the backend
instance. Malformed payloads are acknowledged and logged without increasing the
overflow count. Shutdown clears pending output while preserving the cumulative
drop diagnostic for lifecycle reuse.

.. autoclass:: sqlspec.extensions.litestar.SQLSpecChannelsBackend
   :members:
   :show-inheritance:

Store
=====

.. autoclass:: sqlspec.extensions.litestar.BaseSQLSpecStore
   :members:
   :show-inheritance:

Providers
=========

``create_filter_dependencies()`` accepts camel-case aliases for configured
``orderBy`` fields by default. Use ``sort_field_aliases`` to map explicit API
names to configured SQL-facing fields, or set ``sort_field_camelize=False`` when
an endpoint must accept only raw configured values. Alias values are normalized
before ``OrderByFilter`` is created, and unknown aliases cannot bypass the
``sort_field`` allowlist.

.. autoclass:: sqlspec.extensions.litestar.providers.DependencyDefaults
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.extensions.litestar.providers.FilterConfig
   :members:
   :show-inheritance:

CLI
===

.. py:data:: sqlspec.extensions.litestar.database_group

   Click command group for managing SQLSpec database components (migrations, etc.).

   :type: click.Group
