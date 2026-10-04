=======
Spanner
=======

Google Cloud Spanner adapter using the Spanner client library with sync and
async session pool management.

Request And Session Controls
============================

Spanner request behavior stays on the existing execution APIs. SQLSpec does not
expose public ``execute_with_options()``, ``execute_partitioned_dml()``,
``apply_mutations()``, or ``provide_batch_snapshot()`` methods.

Default request controls can be configured through
``SpannerSyncConfig.driver_features`` or ``SpannerAsyncConfig.driver_features``:

``request_options``
    Forwarded to Spanner ``execute_sql()``, ``execute_update()``, and
    ``batch_update()`` calls. Use this for request tags, transaction tags, and
    priority options supported by the Google Cloud Spanner client.

``directed_read_options``
    Forwarded only to read calls that use ``execute_sql()``. Directed reads are
    not forwarded to DML calls.

``retry`` and ``timeout``
    Forwarded to Spanner statement execution calls when provided.

``query_options``
    Forwarded to ``execute_sql()`` and ``execute_update()``. Batch DML does not
    accept query options.

Per-call overrides use the existing ``execute()``, ``execute_many()``, and
``execute_script()`` methods:

.. code-block:: python

   result = driver.execute(
       "SELECT id FROM users WHERE id = @id",
       id="u-1",
       request_options={"request_tag": "users.lookup"},
       directed_read_options=directed_read_options,
       timeout=10.0,
   )

``directed_read_options`` only applies to read statements. The driver accepts
the argument for a DML statement so call sites can share option plumbing, but it
does not forward directed-read options to ``execute_update()`` or
``batch_update()``.

Pass ``last_statement=True`` to mark the final DML request in a transaction.
For scripts, SQLSpec forwards it only when the final statement is DML. This
option does not commit the transaction; commit through the normal transaction
context or driver API.

Session-Scoped Controls
=======================

``SpannerSyncConfig.provide_session()`` and ``SpannerAsyncConfig.provide_session()``
also accept explicit Spanner controls for the returned session context:

.. code-block:: python

   with config.provide_session(
       request_options={"transaction_tag": "orders.write"},
       retry=retry,
       timeout=20.0,
   ) as driver:
       driver.execute("UPDATE orders SET status = @status WHERE id = @id", status="paid", id="o-1")

   async with async_config.provide_session(
       request_options={"transaction_tag": "orders.write"},
       retry=retry,
       timeout=20.0,
   ) as driver:
       await driver.execute("UPDATE orders SET status = @status WHERE id = @id", status="paid", id="o-1")

The explicit ``provide_session()`` arguments are copied into the returned
driver's feature set and do not mutate ``config.driver_features``. They also do
not hide a ``database_provider`` feature for unrelated database-level methods.

``provide_read_session()`` is the read-only helper for single-use snapshot
reads. For DDL, DML, and write-capable transactions, use ``provide_session()``
or ``provide_write_session()``.

For read-write transactions with automatic ``Aborted`` retry semantics, use
``run_in_transaction()`` on either configuration or driver:

.. code-block:: python

   async def transfer(driver: SpannerAsyncDriver, amount: int) -> None:
       await driver.execute(
           "UPDATE accounts SET balance = balance - @amount WHERE id = @id",
           amount=amount,
           id="a-1",
       )

   await async_config.run_in_transaction(
       transfer,
       100,
       transaction_tag="accounts.transfer",
   )

Sync Configuration
==================

With ``google-cloud-spanner==3.71.0``, closing a database that used multiplexed
sessions can wait up to ten minutes for the SDK's maintenance thread. The
`upstream shutdown fix <https://github.com/googleapis/google-cloud-python/pull/18317>`_
is merged but has not yet been released. Allow for this delay during application
shutdown until a fixed SDK is available.

.. autoclass:: sqlspec.adapters.spanner.SpannerSyncConfig
   :members:
   :show-inheritance:

Async Configuration
===================

.. autoclass:: sqlspec.adapters.spanner.SpannerAsyncConfig
   :members:
   :show-inheritance:

Connection Parameters
=====================

.. autoclass:: sqlspec.adapters.spanner.SpannerConnectionParams
   :members:
   :show-inheritance:

Pool Parameters
===============

.. autoclass:: sqlspec.adapters.spanner.SpannerPoolParams
   :members:
   :show-inheritance:

Driver Features
===============

.. autoclass:: sqlspec.adapters.spanner.SpannerDriverFeatures
   :members:
   :show-inheritance:

Custom Dialects
================

Spanner uses the :doc:`Spanner and Spangres dialects <../dialects>` for SQL compilation.
See the :doc:`Dialects <../dialects>` reference for details.

Sync Driver
===========

.. autoclass:: sqlspec.adapters.spanner.SpannerSyncDriver
   :members:
   :show-inheritance:

Async Driver
============

.. autoclass:: sqlspec.adapters.spanner.SpannerAsyncDriver
   :members:
   :show-inheritance:

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.spanner.data_dictionary.SpannerDataDictionary
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.spanner.data_dictionary.SpannerAsyncDataDictionary
   :members:
   :show-inheritance:

Extension Settings
==================

Use the configuration types below in their corresponding ``extension_config``
namespace: ``"litestar"``, ``"events"``, or ``"adk"`` as supported by this adapter.

.. autoclass:: sqlspec.adapters.spanner.litestar.SpannerLitestarConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.spanner.adk.SpannerADKConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.spanner.adk.SpannerADKRetentionConfig
   :members:
   :show-inheritance:

Extension Stores
================

.. autoclass:: sqlspec.adapters.spanner.adk.SpannerSyncADKStore
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.spanner.adk.SpannerAsyncADKStore
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.spanner.adk.SpannerSyncADKMemoryStore
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.spanner.adk.SpannerAsyncADKMemoryStore
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.spanner.litestar.SpannerSyncStore
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.spanner.litestar.SpannerAsyncStore
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.spanner.events.SpannerSyncEventQueueStore
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.spanner.events.SpannerAsyncEventQueueStore
   :members:
   :show-inheritance:

Native execution controls
-------------------------

``query_options`` can be configured on the driver, supplied when opening a
session, or overridden per call. They apply to queries and single DML operations;
native batch DML does not accept them. ``last_statement=True`` marks final
transaction DML, including only the final statement of a script.

Opt-in Arrow Batch Write ingestion works from database-backed read sessions.
Mutation groups commit independently. Arrow overwrite retains transactional
delete-and-insert behavior without partitioned DML or Batch Write.

