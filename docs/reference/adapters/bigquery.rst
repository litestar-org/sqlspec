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

``select_to_storage()`` exports eligible remote destinations through BigQuery
``EXPORT DATA``. Its destination becomes a wildcard result set, which may
contain several objects. The returned job's ``telemetry["destination"]`` gives
the actual wildcard URI, rather than a single downloadable object.

Destination rules
-----------------

SQLSpec inserts ``-*`` before the final filename suffix. A suffix does not
select the encoding: ``format_hint`` defaults to ``parquet``.

.. list-table:: Example destinations with the default Parquet format
   :header-rows: 1

   * - Requested destination
     - Native destination
   * - ``gs://reports/daily.parquet``
     - ``gs://reports/daily-*.parquet``
   * - ``gs://reports/daily``
     - ``gs://reports/daily-*``
   * - ``gs://reports/daily/``
     - ``gs://reports/daily/part-*.parquet``
   * - ``gs://reports``
     - ``gs://reports/part-*.parquet``
   * - ``gs://reports/daily-*.parquet``
     - ``gs://reports/daily-*.parquet``

One wildcard is allowed in the leaf filename. Wildcards in parent paths,
multiple wildcards, quotes, control characters, and query or fragment components
are rejected before submission. ``gcs://`` becomes ``gs://`` for export;
registered ``alias://`` destinations resolve through the storage registry.

Configuration and formats
-------------------------

``BigQueryConfig.driver_features["enable_native_storage"]`` defaults to
``True``. Set it to ``False`` to retain the client Arrow writer. Local emulator
endpoints, custom storage pipeline factories, local paths, and unsupported
destinations or formats also use the client path before query submission.

Google Cloud Storage needs no connection identifier. S3 and Azure require
``driver_features["native_export_connection"]`` containing an existing
``project.location.connection`` identifier. Missing connections select the
client path; malformed supplied identifiers raise a configuration error.
Azure destinations must include the account, as in
``azure://account.blob.core.windows.net/container/result.parquet``.
Accountless ``az://`` or ``abfss://`` addresses do not select native export.

The caller supplies the provider connection, permissions, compatible location,
and existing BigQuery Omni resources. See the provider's
`EXPORT DATA connection requirements <https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/export-statements>`_
and `Azure export setup <https://docs.cloud.google.com/bigquery/docs/omni-azure-export-results-to-azure-storage>`_.

Native formats are ``parquet``, ``csv``, ``json``, and ``jsonl``. Both JSON names
use BigQuery's newline-delimited JSON export; ``arrow-ipc`` uses the client path.
CSV includes a header. Native export overwrites matching shard names but never
deletes stale shards. Choose a fresh prefix when readers need an exact new
result set. There is no public ``overwrite`` or compression argument on
``select_to_storage()``; native exports use provider compression defaults.
Custom pipeline settings remain on the client path.

Query behavior and telemetry
----------------------------

Query parameters remain bound through the SDK's query-job configuration.
SQLSpec submits one export job and applies the configured retry, request timeout,
and result timeout. A submitted native job failure propagates through normal
exception mapping; it does not retry through the client writer.

BigQuery forbids ``INFORMATION_SCHEMA``, system-table, and wildcard-table queries
inside ``EXPORT DATA``. CSV cannot represent nested or repeated fields.
Query execution still incurs the provider's query costs and capacity limits;
see `export options and restrictions <https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/export-statements>`_.

Native telemetry includes the wildcard destination and a job identifier in
``extra``. Unknown exported rows, bytes, and file counts are omitted. Bytes
scanned or billed by a query are not exported-byte measurements.

Validation and measurement
--------------------------

Local tests cover URI rules, SDK parameter objects, job controls, errors, and
emulator client export/readback. Real GCS, S3, and Azure export/readback and
provider-side parameter execution remain unverified. No native service latency
or crossover claim follows from the local tests.

The contributor harness ``tools/scripts/bench_bigquery_storage.py`` compares
native and client export controls with explicit project, endpoint, and output
arguments. It reports unsupported native routes separately and retains exported
objects for inspection. Run its ``--help`` for options.

Its ``--mode ingest-local`` measures local Parquet encoding and temporary-file
writes at several sizes. Both direct-file upload and GCS landing require the
client to upload the encoded payload; landing also needs storage configuration
and cleanup. Real GCS landing throughput remains unverified.
``load_from_arrow()``, its optional Storage Write API route, and
``load_from_storage()`` retain their existing behavior.

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
