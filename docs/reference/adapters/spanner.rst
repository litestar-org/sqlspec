=======
Spanner
=======

Google Cloud Spanner adapter using the Spanner client library with session
pool management.

Request And Session Controls
============================

Spanner request behavior stays on the existing execution APIs. SQLSpec does not
expose public ``execute_with_options()``, ``execute_partitioned_dml()``,
``apply_mutations()``, or ``provide_batch_snapshot()`` methods.

Default request controls can be configured through
``SpannerSyncConfig.driver_features``:

``request_options``
    Forwarded to Spanner ``execute_sql()``, ``execute_update()``, and
    ``batch_update()`` calls. Use this for request tags, transaction tags, and
    priority options supported by the Google Cloud Spanner client.

``directed_read_options``
    Forwarded only to read calls that use ``execute_sql()``. Directed reads are
    not forwarded to DML calls.

``retry`` and ``timeout``
    Forwarded to Spanner statement execution calls when provided.

Per-call overrides use the existing ``execute()``, ``execute_many()``, and
``execute_script()`` methods:

.. code-block:: python

   result = driver.execute(
       "SELECT id FROM users WHERE id = @id",
       {"id": "u-1"},
       request_options={"request_tag": "users.lookup"},
       directed_read_options=directed_read_options,
       timeout=10.0,
   )

``directed_read_options`` only applies to read statements. The driver accepts
the argument for a DML statement so call sites can share option plumbing, but it
does not forward directed-read options to ``execute_update()`` or
``batch_update()``.

Session-Scoped Controls
=======================

``SpannerSyncConfig.provide_session()`` also accepts explicit Spanner controls
for the returned session context:

.. code-block:: python

   with config.provide_session(
       request_options={"transaction_tag": "orders.write"},
       retry=retry,
       timeout=20.0,
   ) as driver:
       driver.execute("UPDATE orders SET status = @status WHERE id = @id", params)

The explicit ``provide_session()`` arguments are copied into the returned
driver's feature set and do not mutate ``config.driver_features``. They also do
not hide a ``database_provider`` feature for unrelated database-level methods.

``provide_read_session()`` is the read-only helper for single-use snapshot
reads. For DDL, DML, and write-capable transactions, use ``provide_session()``
or ``provide_write_session()``.

Configuration
=============

.. autoclass:: sqlspec.adapters.spanner.SpannerSyncConfig
   :members:
   :show-inheritance:

Custom Dialects
================

Spanner uses the :doc:`Spanner and Spangres dialects <../dialects>` for SQL compilation.
See the :doc:`Dialects <../dialects>` reference for details.

Driver
======

.. autoclass:: sqlspec.adapters.spanner.SpannerSyncDriver
   :members:
   :show-inheritance:

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.spanner.data_dictionary.SpannerDataDictionary
   :members:
   :show-inheritance:
