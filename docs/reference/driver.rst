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

.. currentmodule:: sqlspec.driver._sync

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

.. currentmodule:: sqlspec.driver._async

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

.. currentmodule:: sqlspec.driver._common

.. automodule:: sqlspec.driver._common
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

See Also
========

- :doc:`adapters` - Database adapters built on driver system
- :doc:`base` - SQLSpec configuration
- :doc:`core` - Core SQL processing
- :doc:`/contributing/creating_adapters` - Adapter creation guide
