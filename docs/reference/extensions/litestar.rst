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

Correlation Middleware
======================

``SQLSpecPlugin`` installs ``CorrelationMiddleware`` while
``enable_correlation_middleware`` is enabled, which is the default. Add it
yourself only when you build the middleware stack without the plugin.

When the application's middleware stack already contains ``CorrelationMiddleware``
or a subclass, ``SQLSpecPlugin`` automatically skips adding a duplicate instance.
It logs at ``DEBUG`` when the existing middleware honors the configured headers,
or logs a ``WARNING`` when configured correlation headers would remain unapplied.

.. code-block:: python

   from litestar.middleware import DefineMiddleware

   from sqlspec.extensions.litestar import TRACE_CONTEXT_FALLBACK_HEADERS, CorrelationMiddleware

   middleware = [DefineMiddleware(CorrelationMiddleware, headers=TRACE_CONTEXT_FALLBACK_HEADERS)]

.. autoclass:: sqlspec.extensions.litestar.CorrelationMiddleware
   :members:
   :show-inheritance:

.. py:data:: sqlspec.extensions.litestar.TRACE_CONTEXT_FALLBACK_HEADERS

   Ordered trace-context header names that the plugin checks after
   ``correlation_header`` and ``correlation_headers`` while
   ``auto_trace_headers`` is enabled, which is the default. Import the constant
   to inspect the names or pass them to ``CorrelationMiddleware``.

   :type: tuple[str, ...]

Error Handling
==============

``SQLSpecPlugin`` automatically registers default exception handlers on Litestar's
application configuration for database errors:

* :class:`~sqlspec.exceptions.NotFoundError`: translated to HTTP 404, with the error message as detail.
* :class:`~sqlspec.exceptions.IntegrityError` (and subclasses such as :class:`~sqlspec.exceptions.UniqueViolationError`): translated to HTTP 409 Conflict with the generic detail ``"Conflict"``, preventing internal database constraint or schema text from leaking to clients.

The plugin renders these responses within the route's middleware stack, keeping headers
added by application middleware and running ``after_exception`` hooks once. Handlers
explicitly registered on the application or router for :class:`~sqlspec.exceptions.IntegrityError`
or its subclasses take precedence; handlers for broader exceptions (such as
:class:`~sqlspec.exceptions.SQLSpecError`, :class:`Exception`, or status 500) receive the original
exception, while handlers for status 409 or :class:`litestar.exceptions.HTTPException` render the HTTP response.

.. autofunction:: sqlspec.extensions.litestar.plugin.not_found_error_handler

.. autofunction:: sqlspec.extensions.litestar.plugin.integrity_error_handler

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

``metrics_snapshot()`` returns every observability metric recorded for the
event channel's database configuration, including loader, migration, storage,
and other event channels on that configuration, merged with
``channels.output_queue_depth`` and ``channels.dropped_message_count``. Those
two keys are unprefixed and describe this backend instance only; they mirror
the ``output_queue_depth`` and ``dropped_message_count`` properties. All values
are floats.

Payload budget
--------------

Each published payload is base64-wrapped as ``{"data_b64": ...}``. The
PostgreSQL ``notify`` event backend sends that payload inside a notification
envelope and rejects an envelope larger than
:data:`~sqlspec.extensions.events.MAX_NOTIFY_BYTES`. ``measure(data)`` returns
the encoded size of the ``notify`` envelope that wraps ``data``, and
``fits(data)`` reports whether it is within ``notify_budget``.
``notify_budget`` is ``None`` for every other backend kind (``notify_queue``,
``poll_queue``, ``aq``, ``txeventq``), so ``fits()`` always returns ``True``
there. Check a payload before publishing and send oversized data as smaller
messages or as a compact reference the subscriber resolves:

.. code-block:: python

    backend = SQLSpecChannelsBackend(spec.event_channel(config))

    if backend.fits(payload):
        await backend.publish(payload, ["updates"])
    else:
        await backend.publish(b'{"object_key": "updates/123.json"}', ["updates"])

API
---

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

.. autofunction:: sqlspec.extensions.litestar.providers.create_filter_dependencies

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
