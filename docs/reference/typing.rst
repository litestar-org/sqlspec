======
Typing
======

Public type aliases, metadata types, and protocol definitions used throughout
SQLSpec.

.. currentmodule:: sqlspec.typing

Optional dependency exports
===========================

The :mod:`sqlspec.typing` module and its private ``sqlspec._typing`` backing module resolve heavy
optional-dependency symbols lazily. Importing ``sqlspec`` does not import
Pydantic, Litestar, PyArrow, pandas, Polars, OpenTelemetry, or Prometheus.
Accessing one of their exported symbols imports its dependency on first use and
caches the resolved object::

   import sqlspec.typing as sqlspec_typing

   arrow_table_type = sqlspec_typing.ArrowTable  # Imports PyArrow here.

When an optional dependency is not installed, the same access returns a stable
typed shim. Repeated access returns the identical real object or shim, preserving
annotation and runtime identity behavior. Features that require the dependency
still raise the normal missing-dependency error when enabled.

``msgspec`` remains eager because its core types and conversion functions are
used throughout result mapping and serialization. ``orjson`` also remains on
the existing eager path; both have a small measured import cost compared with
the deferred integrations.

Metadata Types
==============

Schema metadata types (such as ``TableMetadata``, ``ColumnMetadata``, ``ForeignKeyMetadata``,
and ``IndexMetadata``) are defined in :doc:`data_dictionary`.

Protocols
=========

.. currentmodule:: sqlspec.typing

SQLSpec defines focused protocols for reading dictionary-like rows, inspecting
dataclasses, and interfacing with Apache Arrow record batch readers and schemas.

.. note::
   SQLSpec does not define standalone ``DriverProtocol``, ``AsyncDriverProtocol``,
   or ``SessionProtocol`` protocols. Driver instances are implementations of
   :class:`~sqlspec.driver.SyncDriverAdapterBase` or
   :class:`~sqlspec.driver.AsyncDriverAdapterBase`, typed with
   :data:`~sqlspec.driver.DriverAdapterProtocol`. Database configurations conform
   to :class:`~sqlspec.config.DatabaseConfigProtocol`. Runtime statement protocols
   reside in :mod:`sqlspec.protocols`.

.. autoclass:: DictLike
   :members:
   :show-inheritance:

.. autoclass:: DataclassProtocol
   :members:
   :show-inheritance:

.. autoclass:: ArrowRecordBatchReaderProtocol
   :members:
   :show-inheritance:

.. autoclass:: ArrowSchemaProtocol
   :members:
   :show-inheritance:

Type Variables
==============

.. autodata:: ConnectionT

.. autodata:: PoolT

.. autodata:: SchemaT

Type Aliases
============

.. autodata:: SupportedSchemaModel

.. autodata:: StatementParameters

.. autodata:: ArrowReturnFormat

Sentinels
=========

.. autodata:: Empty

.. autodata:: UNSET

Helper Functions
================

.. autofunction:: get_type_adapter
