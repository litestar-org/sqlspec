======
Driver
======

The driver module provides base classes and mixins for database operations. It defines the foundation for both synchronous and asynchronous database drivers.

.. currentmodule:: sqlspec.driver

Overview
========

The driver system consists of:

- **Base Drivers** - Abstract base classes for sync/async drivers
- **Mixins** - Reusable functionality for query operations
- **Transaction Management** - Context managers for transactions
- **Result Processing** - Standard result handling
- **Connection Lifecycle** - Connection pooling and cleanup

Base Driver Classes
===================

Synchronous Driver
------------------

.. currentmodule:: sqlspec.driver

.. autoclass:: SyncDriverAdapterBase
   :members:
   :undoc-members:
   :show-inheritance:

   Base class for synchronous database drivers.

   **Abstract methods to implement:**

   - ``execute()`` - Execute SQL and return results
   - ``execute_many()`` - Execute SQL for multiple parameter sets
   - ``begin_transaction()`` - Start a transaction
   - ``commit_transaction()`` - Commit a transaction
   - ``rollback_transaction()`` - Rollback a transaction
   - ``close()`` - Close the connection

Asynchronous Driver
-------------------

.. currentmodule:: sqlspec.driver

.. autoclass:: AsyncDriverAdapterBase
   :members:
   :undoc-members:
   :show-inheritance:

   Base class for asynchronous database drivers.

   **Abstract methods to implement:**

   - ``execute()`` - Execute SQL and return results (async)
   - ``execute_many()`` - Execute SQL for multiple parameter sets (async)
   - ``begin_transaction()`` - Start a transaction (async)
   - ``commit_transaction()`` - Commit a transaction (async)
   - ``rollback_transaction()`` - Rollback a transaction (async)
   - ``close()`` - Close the connection (async)

Driver Mixins
=============

.. currentmodule:: sqlspec.driver.mixins

.. automodule:: sqlspec.driver.mixins.query
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: sqlspec.driver.mixins.result
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: sqlspec.driver.mixins.parameters
   :members:
   :undoc-members:
   :show-inheritance:

Transaction Management
======================

Both sync and async drivers support transaction context managers:

.. code-block:: python

   # Async transactions
          await driver.execute("INSERT INTO users VALUES (?, ?)", "Bob", 25)
       await driver.execute("UPDATE accounts SET balance = balance - 50 WHERE user = ?", "Bob")

Connection Pooling
==================

.. currentmodule:: sqlspec.driver

.. automodule:: sqlspec.driver
   :members:
   :undoc-members:
   :show-inheritance:

Data Dictionary
===============

The Data Dictionary API provides standardized introspection capabilities across all supported databases.

.. currentmodule:: sqlspec.driver

.. autoclass:: DataDictionaryMixin
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: AsyncDataDictionaryBase
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: SyncDataDictionaryBase
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: ForeignKeyMetadata
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: ColumnMetadata
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: IndexMetadata
   :members:
   :undoc-members:
   :show-inheritance:

Feature Flag Types
------------------

.. currentmodule:: sqlspec.data_dictionary

.. autoclass:: FeatureFlags
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: FeatureVersions
   :members:
   :undoc-members:
   :show-inheritance:

Driver Protocols
================

.. currentmodule:: sqlspec.protocols

.. autoclass:: DriverProtocol
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: AsyncDriverProtocol
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: SessionProtocol
   :members:
   :undoc-members:
   :show-inheritance:

Adapter Implementation Contract
===============================

Each adapter's ``core.py`` module must export standardized helper functions:

.. list-table:: Standardized core.py Functions
   :header-rows: 1
   :widths: 30 50 20

   * - Function
     - Purpose
     - Return Type
   * - ``collect_rows``
     - Extract rows from cursor result
     - ``tuple[list[dict], list[str]]``
   * - ``resolve_rowcount``
     - Get affected row count (handles negative values)
     - ``int``
   * - ``normalize_execute_parameters``
     - Prepare parameters for single execution
     - ``Any``
   * - ``normalize_execute_many_parameters``
     - Prepare parameters for batch execution
     - ``Any``
   * - ``build_connection_config``
     - Transform raw config to driver format
     - ``dict``
   * - ``raise_exception``
     - Map driver errors to SQLSpec exceptions
     - ``NoReturn``

**Why standardized names matter:**

- Consistent naming across all adapters reduces cognitive load
- Enables mypyc optimization of hot-path functions
- Clear contract for new adapter implementations

Reference implementations: ``sqlspec.adapters.asyncpg.core``, ``sqlspec.adapters.sqlite.core``

See Also
========

- :doc:`adapters` - Database adapters built on driver system
- :doc:`base` - SQLSpec configuration
- :doc:`core` - Core SQL processing
- :doc:`/contributing/creating_adapters` - Adapter creation guide
