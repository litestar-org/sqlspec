=================
Creating Adapters
=================

This guide explains how to build and contribute a database adapter for SQLSpec. SQLSpec provides a type-safe SQL query execution and result-mapping layer designed for minimal abstraction without ORM semantics. Adapters connect SQLSpec's core statement processing pipeline—including SQLGlot AST transformation, dialect conversions, and parameter extraction—to a concrete database driver.

Overview
========

An adapter in SQLSpec bridges two layers:

1. **The SQLSpec Pipeline**: Transforms user queries, optimizes AST representations, normalizes parameters, and maps results to dictionaries, tuples, dataclasses, msgspec Structs, Pydantic models, or Arrow tables.
2. **The Database Driver**: Manages physical connections, cursor lifecycles, statement execution, and transaction boundaries.

Adapters are modular and live under ``sqlspec/adapters/<adapter_name>/``.

Sync vs. Async Architecture
===========================

SQLSpec supports both synchronous and asynchronous database drivers through dedicated base classes:

- **Synchronous Adapters**:
  - Configuration subclasses :class:`~sqlspec.config.SyncDatabaseConfig`.
  - Driver subclasses :class:`~sqlspec.driver.SyncDriverAdapterBase`.
  - Data dictionary subclasses :class:`~sqlspec.driver.SyncDataDictionaryBase`.
  - Exception handler subclasses :class:`~sqlspec.driver.BaseSyncExceptionHandler`.
- **Asynchronous Adapters**:
  - Configuration subclasses :class:`~sqlspec.config.AsyncDatabaseConfig`.
  - Driver subclasses :class:`~sqlspec.driver.AsyncDriverAdapterBase`.
  - Data dictionary subclasses :class:`~sqlspec.driver.AsyncDataDictionaryBase`.
  - Exception handler subclasses :class:`~sqlspec.driver.BaseAsyncExceptionHandler`.

Both modes expose the same high-level query operations (:meth:`execute`, :meth:`select`, :meth:`select_value`, :meth:`select_to_arrow`, :meth:`select_stream`, :meth:`execute_many`), with asynchronous operations defined as awaitable coroutines and async context managers.

Adapter Module Layout
=====================

Every database adapter is organized into a standardized directory structure:

.. code-block:: text

   sqlspec/adapters/<name>/
   ├── __init__.py           # Re-exports the public adapter interface
   ├── _typing.py            # Local type aliases for connection and cursor types
   ├── config.py             # Typed configuration, connection params, and driver features
   ├── core.py               # Statement config, apply_driver_features, exception mapping
   ├── driver.py             # Driver adapter implementation and exception handling
   ├── pool.py               # Connection pool implementation or driver pool wrapper
   ├── data_dictionary.py    # Database schema introspection (tables, columns, indexes, FKs)
   └── type_converter.py     # Optional database-specific type conversion and serialization

Module Responsibilities
-----------------------

``config.py``
+++++++++++++

Defines the adapter configuration and connection parameters:

- Subclasses :class:`~sqlspec.config.SyncDatabaseConfig` or :class:`~sqlspec.config.AsyncDatabaseConfig`.
- Declares a ``<Name>ConnectionParams`` :class:`~typing.TypedDict` for driver-specific connection arguments.
- Declares a ``<Name>DriverFeatures`` :class:`~typing.TypedDict` for optional feature flags.
- Implements ``provide_connection()``, ``provide_pool()``, and ``provide_session()``.

``core.py``
+++++++++++

Houses compiled and core helpers for statement execution:

- ``default_statement_config``: Configures the SQLGlot dialect, parameter style (``named``, ``qmark``, ``numeric``, or ``pyformat``), and type coercions.
- ``apply_driver_features(statement_config, driver_features)``: Merges feature defaults and applies parameter serializer customizations, returning a ``tuple[StatementConfig, dict[str, Any]]``.
- ``create_mapped_exception(error, *, logger=None)``: Translates native database driver exceptions into the unified :mod:`sqlspec.exceptions` hierarchy.

``driver.py``
+++++++++++++

Implements the driver adapter by subclassing :class:`~sqlspec.driver.SyncDriverAdapterBase` or :class:`~sqlspec.driver.AsyncDriverAdapterBase`. The driver must implement:

- :meth:`dispatch_execute`: Executes a single SQL statement with parameters and returns an :class:`~sqlspec.driver.ExecutionResult`.
- :meth:`dispatch_execute_many`: Executes a statement across multiple parameter batches.
- :meth:`dispatch_execute_script`: Executes raw SQL scripts or DDL.
- :meth:`begin`, :meth:`commit`, :meth:`rollback`: Manages transaction boundaries.
- :meth:`with_cursor`: Context manager yielding a cursor for database operations.
- :meth:`handle_database_exceptions`: Context manager wrapping operations in the adapter's exception handler.
- :meth:`_connection_in_transaction`: Inspects whether the active connection is inside a transaction.
- :attr:`data_dictionary`: Returns the data dictionary instance for schema introspection.

``data_dictionary.py``
++++++++++++++++++++++

Provides schema introspection capabilities by subclassing :class:`~sqlspec.driver.SyncDataDictionaryBase` or :class:`~sqlspec.driver.AsyncDataDictionaryBase`:

- :meth:`get_tables`: Retrieves table metadata for a given schema.
- :meth:`get_columns`: Retrieves column metadata for a table or schema.
- :meth:`get_indexes`: Retrieves index metadata.
- :meth:`get_foreign_keys`: Retrieves foreign key relationships.
- :meth:`get_version`: Queries database version information.
- :meth:`get_feature_flag`: Checks feature availability on the target database engine.

``pool.py``
+++++++++++

Manages connection pooling, wrapping either the driver's native connection pool or implementing SQLSpec's connection pool protocols.

``__init__.py``
+++++++++++++++

Re-exports the public API of the adapter, including configuration classes, connection parameters, driver classes, and the default statement configuration.

The Driver Features Pattern
===========================

SQLSpec standardizes adapter capabilities through driver feature dictionaries. Features allow callers to toggle adapter behaviors such as custom type adapters, JSON serialization, UUID conversion, or native bulk-loading optimizations:

.. code-block:: python

   from typing import Any, TypedDict
   from typing_extensions import NotRequired


   class ExampleDriverFeatures(TypedDict):
       enable_custom_adapters: NotRequired[bool]
       enable_uuid_conversion: NotRequired[bool]
       json_serializer: NotRequired[Any]
       json_deserializer: NotRequired[Any]

In ``core.py``, the :func:`apply_driver_features` helper normalizes these options and returns an updated statement configuration:

.. code-block:: python

   def apply_driver_features(
       statement_config: StatementConfig,
       driver_features: Mapping[str, Any] | None,
   ) -> tuple[StatementConfig, dict[str, Any]]:
       features = dict(driver_features) if driver_features else {}
       features.setdefault("enable_custom_adapters", False)
       features.setdefault("enable_uuid_conversion", True)

       # Update statement_config with any custom serializers or settings
       return statement_config, features

Exception Mapping
=================

Adapters must never leak raw driver exceptions to application callers. Every database exception is mapped to a subclass of :class:`~sqlspec.exceptions.SQLSpecError` via :func:`create_mapped_exception`:

.. code-block:: python

   from sqlspec.exceptions import (
       CheckViolationError,
       DatabaseConnectionError,
       DataError,
       DeadlockError,
       ForeignKeyViolationError,
       IntegrityError,
       NotNullViolationError,
       OperationalError,
       SQLParsingError,
       SQLSpecError,
       UniqueViolationError,
   )


   def create_mapped_exception(
       error: Exception,
       *,
       logger: Any = None,
   ) -> SQLSpecError:
       """Map driver-specific exception to a SQLSpec exception."""
       # Inspect driver error codes or error hierarchy and return the matching SQLSpecError
       return OperationalError(str(error))

Adapter Skeleton
================

Below is a minimal synchronous driver implementation illustrating the required dispatch and transaction methods:

.. literalinclude:: /examples/contributing/new_adapter.py
   :language: python
   :caption: Adapter Driver Skeleton
   :start-after: # start-example
   :end-before: # end-example

Testing Guidelines
==================

All adapter contributions must include comprehensive test coverage following the repository's test placement structure:

1. **Unit Tests** (``tests/unit/adapters/<adapter>/``):
   - Test configuration instantiation, parameter normalization, statement compilation, and feature flag merging.
   - Unit tests must run without requiring live database services.
2. **Contract Tests** (``tests/integration/adapters/<family>/test_shared.py``):
   - Reusable test contracts verify multi-driver behavioral parity across database families (e.g. SQLite, PostgreSQL, MySQL, Oracle, MSSQL).
3. **Integration Tests** (``tests/integration/adapters/<family>/<driver>/``):
   - Live database tests using ``pytest-databases`` fixtures.
   - Test connection pooling, transactional commit and rollback, savepoints, batch execution, and Arrow table streaming.

Quality Verification
--------------------

Before opening a pull request for a new adapter, ensure all quality gates pass locally:

.. code-block:: console

   # Run linters, formatters, Prek hooks, and slotscheck
   make lint

   # Run static type checking (mypy + pyright)
   make type-check

   # Run tests
   make test

   # Verify test coverage (changed code must maintain >= 90% coverage)
   make coverage

See Also
========

- :doc:`/reference/driver` for the complete driver protocol and base class references.
- :doc:`/reference/adapters` for existing adapter configuration reference.
- :doc:`/contribution-guide` for repository setup, commit standards, and PR workflows.
