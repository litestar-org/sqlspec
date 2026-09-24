===
Db2
===

Sync IBM Db2 adapter using `ibm_db <https://pypi.org/project/ibm_db/>`_.
It uses positional parameter binding (``?``) and exposes sync SQLSpec config,
driver, connection pooling, data dictionary, and Apache Arrow integrations.

Overview
========

The Db2 adapter provides comprehensive integration with IBM Db2 (LUW, z/OS, and IBM i):

* **Dialect**: Full SQLGlot dialect compilation targeting Db2 conventions, including ``FETCH FIRST n ROWS ONLY`` / ``OFFSET m ROWS FETCH NEXT n ROWS ONLY`` and dual dummy table transforms (``SYSIBM.SYSDUMMY1``).
* **Connection Pooling**: Dedicated thread-local pool with configurable liveness checks (``SELECT 1 FROM SYSIBM.SYSDUMMY1``) and maximum connection lifetime recycling.
* **Arrow Analytics**: In-memory Arrow table export via ``select_to_arrow``, as well as native block cursor streaming via ``sqlspec.adapters.arrow_odbc``.
* **Data Dictionary**: Schema reflection for tables, columns, primary keys, foreign keys, and indexes using Db2 catalog views (``SYSCAT.TABLES``, ``SYSCAT.COLUMNS``, etc.).

Installation
============

.. code-block:: bash

   pip install sqlspec[db2]

Platform Requirements
=====================

* **Supported Platforms**: Pre-compiled wheels are available for Linux x86_64, Windows, and macOS ARM64.
* **Linux ARM64**: IBM clidriver is not available natively for Linux aarch64; use x86_64 container emulation or ODBC bridging.
* **Mainframe / IBM i**: Connecting to Db2 on z/OS or IBM i requires an active IBM Db2 Connect license (`db2conpe.lic`).

Configuration
=============

.. autoclass:: sqlspec.adapters.db2.Db2SyncConfig
   :members:
   :show-inheritance:

Connection Parameters
=====================

.. autoclass:: sqlspec.adapters.db2.Db2ConnectionParams
   :members:
   :show-inheritance:

Pool Parameters
===============

.. autoclass:: sqlspec.adapters.db2.Db2PoolParams
   :members:
   :show-inheritance:

Driver Features
===============

.. autoclass:: sqlspec.adapters.db2.Db2DriverFeatures
   :members:
   :show-inheritance:

Driver
======

.. autoclass:: sqlspec.adapters.db2.Db2SyncDriver
   :members:
   :show-inheritance:

Connection Pool
===============

.. autoclass:: sqlspec.adapters.db2.Db2SyncConnectionPool
   :members:
   :show-inheritance:

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.db2.data_dictionary.Db2SyncDataDictionary
   :members:
   :show-inheritance:
