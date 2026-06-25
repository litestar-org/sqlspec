========
BigQuery
========

Google BigQuery adapter with Arrow result support.

Job Controls
============

BigQuery job behavior is configured on ``BigQueryConfig.driver_features`` and
then applied by the normal driver methods. SQLSpec does not expose a separate
``execute_with_job()`` method, and per-call job options are not threaded through
generic ``execute(**kwargs)``.

``job_retry_deadline`` controls retry construction for BigQuery query, load, and
export jobs. The default is ``60.0`` seconds. Values less than or equal to zero
disable the BigQuery job retry wrapper by passing ``job_retry=None`` to the
client call.

``job_result_timeout`` bounds waits on ``QueryJob.result()`` and load job
completion. ``request_timeout`` bounds the initial BigQuery API request; when it
is omitted, SQLSpec derives a request timeout from ``job_result_timeout`` when
that value is numeric.

``use_query_and_wait=True`` switches simple query execution from
``Client.query()`` plus ``QueryJob.result()`` to ``Client.query_and_wait()``.
The public SQLSpec call remains ``execute()``, ``select_*()``, or
``select_to_arrow()``; configured retry and timeout values are applied
transparently.

Load Jobs
=========

``load_from_arrow()`` and ``load_from_storage()`` use the configured BigQuery
job retry and timeout controls for the load request and load-job completion.
Use ``job_retry_deadline=0`` when running against emulators or other endpoints
where retrying invalid or unsupported jobs would only extend failures.

Result Exports
==============

BigQuery result export uses the existing ``select_to_storage()`` method. There
is no public ``export_table_to_storage()`` API. ``select_to_storage()`` runs the
query with the configured job controls, waits for the query job according to the
configured result timeout, and then writes through the selected storage bridge
destination.

Billing follows BigQuery query-job behavior: exporting query results still runs
the SQL statement, so the query can scan and bill data before SQLSpec writes the
result to local or object storage.

Configuration
=============

.. autoclass:: sqlspec.adapters.bigquery.BigQueryConfig
   :members:
   :show-inheritance:

Driver
======

.. autoclass:: sqlspec.adapters.bigquery.BigQueryDriver
   :members:
   :show-inheritance:

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.bigquery.data_dictionary.BigQueryDataDictionary
   :members:
   :show-inheritance:
