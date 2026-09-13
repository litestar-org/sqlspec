=====================
CockroachDB + Psycopg
=====================

CockroachDB adapter using psycopg with automatic transaction retry logic.
Provides both sync and async support.

Sync Configuration
==================

.. autoclass:: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgSyncConfig
   :members:
   :show-inheritance:

Async Configuration
===================

.. autoclass:: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgAsyncConfig
   :members:
   :show-inheritance:

Connection Parameters
=====================

.. autoclass:: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgConnectionConfig
   :members:
   :show-inheritance:

Pool Parameters
===============

.. autoclass:: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgPoolConfig
   :members:
   :show-inheritance:

Driver Features
===============

.. autoclass:: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgDriverFeatures
   :members:
   :show-inheritance:

Sync Driver
===========

.. autoclass:: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgSyncDriver
   :members:
   :show-inheritance:

Async Driver
============

.. autoclass:: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgAsyncDriver
   :members:
   :show-inheritance:

Retry Configuration
===================

.. autoclass:: sqlspec.adapters.cockroach_psycopg.CockroachPsycopgRetryConfig
   :members:
   :show-inheritance:

Sync Data Dictionary
====================

.. autoclass:: sqlspec.adapters.cockroach_psycopg.data_dictionary.CockroachPsycopgSyncDataDictionary
   :members:
   :show-inheritance:

Async Data Dictionary
=====================

.. autoclass:: sqlspec.adapters.cockroach_psycopg.data_dictionary.CockroachPsycopgAsyncDataDictionary
   :members:
   :show-inheritance:

Native object storage
=====================

Set ``driver_features={"enable_native_storage": True}`` to let CockroachDB
write or read CSV and Parquet directly. This changes the storage contract:
``select_to_storage`` writes generated files below a destination prefix;
``load_from_storage`` appends into an existing table and takes it offline.
Imports invalidate foreign keys, which must be validated afterward.
The feature is disabled by default.
Install ``sqlspec[cockroachdb,obstore]`` for both drivers and URI resolution.
The inherited CSV/Parquet paths additionally require ``pyarrow``.

Both psycopg configurations require ``connection_config={"autocommit": True}``
when native storage is enabled. SQLSpec never enables autocommit automatically.
The driver also checks the actual connection before each native call. Active
transactions, disabled features, unsupported formats/protocols, and
``overwrite=True`` imports use the inherited client-side path. Native execution
or result errors propagate once; SQLSpec does not retry them through that path.

Native export supports a single SELECT, including query builders, filters and
bound parameters that compile to positional driver parameters. Scripts,
executemany statements and other statement types use the inherited path.
Parquet is the default export format. Eligible resolved protocols are ``s3``,
``gs``, ``gcs`` and ``azure``; the URI itself must use a scheme and credentials
CockroachDB understands. Local paths and local aliases use client-side storage.
Aliases resolve through the storage registry before database execution.

The destination must be reachable by every CockroachDB node. Credentials stored
only in a Python storage backend are not transferred to the database. Supply a
server-readable URI or configure the server's external-storage identity.

Export and import filenames
---------------------------

This example assumes an existing ``archive_items(id INT8, label STRING)`` table
and a server-readable destination in ``COCKROACH_STORAGE_URI``. Use a fresh
prefix for each export. ``extra["files"]`` contains the generated relative
filenames; preserve URI query parameters when constructing import sources.

.. code-block:: python

    import os
    from urllib.parse import urlsplit, urlunsplit
    from uuid import uuid4

    from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgSyncConfig

    config = CockroachPsycopgSyncConfig(
        connection_config={
            "conninfo": os.environ["DATABASE_URL"],
            "autocommit": True,
        },
        driver_features={"enable_native_storage": True},
    )
    parts = urlsplit(os.environ["COCKROACH_STORAGE_URI"])
    destination = urlunsplit(
        parts._replace(path=parts.path.rstrip("/") + "/" + uuid4().hex)
    )
    try:
        with config.provide_session() as driver:
            exported = driver.select_to_storage(
                "SELECT :id::INT8 AS id, :label::STRING AS label",
                destination,
                id=7,
                label="O'Reilly",
                format_hint="parquet",
            )
            prefix = urlsplit(exported.telemetry["destination"])
            for filename in exported.telemetry["extra"]["files"]:
                source = urlunsplit(
                    prefix._replace(path=prefix.path.rstrip("/") + "/" + filename)
                )
                driver.load_from_storage("archive_items", source, file_format="parquet")
    finally:
        config.close_pool()

Export telemetry sums server-reported rows and file bytes. Import telemetry
reports rows and ``extra["job_id"]``; its logical job byte count is not reported
as an artifact size. Partition metadata and supplied export telemetry retain
the standard storage job merge behavior. Generated files, including partial
files after failure, remain the caller's responsibility to clean up.

CSV conventions
---------------

Native CSV exports have no header. For the non-NULL example above, select CSV
and configure ``native_storage_csv_options={"skip": 0}``; import the returned
files with ``file_format="csv"``. ``skip=1`` instead acknowledges one header
in an externally produced CSV file. Without an explicit ``skip``, CSV import
uses the inherited header-aware reader. A boolean is not a valid skip count.

``nullas`` sets an export NULL marker; ``nullif`` sets an import NULL marker.
Both accept strings, including an explicit empty string. No marker is inferred:
CSV export with NULL values and no ``nullas`` fails at the server. When using
markers, choose matching values that cannot occur as literal data. In particular,
using an empty marker can turn an empty string into NULL. Use Parquet to preserve
arbitrary NULL, empty-string and literal-marker values without this convention.
Only ``skip``, ``nullas`` and ``nullif`` are accepted; option values are bound.

Privileges and validation boundary
----------------------------------

Export requires SELECT on the source table; import requires INSERT and DROP on
the target. Custom S3 endpoints and implicit cloud credentials require the admin
role or ``EXTERNALIOIMPLICITACCESS``. Explicit credentials for standard cloud
storage have different privileges. Storage-provider permissions still apply.
Import cannot run during a rolling upgrade. Consult CockroachDB's
`EXPORT reference <https://docs.cockroachlabs.com/docs/stable/export>`_ and
`IMPORT INTO reference <https://docs.cockroachlabs.com/docs/stable/import-into>`_
for server-version restrictions and foreign-key revalidation.
The import reference documents Parquet support as subject to change.

Local validation used CockroachDB CCL v26.2.5 and RustFS. It covers all three
SQLSpec Cockroach drivers, CSV/Parquet roundtrips, bound values, transaction
refusal, NULL conventions and custom-endpoint privileges. CockroachDB Cloud
tiers, hosted privileges, cloud identity chains, GCS and Azure are unverified.
Local results do not establish support on every hosted tier.

Performance measurement
-----------------------

``tools/scripts/bench_cockroach_storage.py`` compares native and inherited
Parquet export/import with explicit local connection and storage configuration.
Run ``--help`` for environment variables, row counts, warmups and iterations.
It records per-sample timings, medians and ranges. A second connection samples
table availability; its polling interval and query timeout limit the resolution.
Zero failed probes does not prove the table was never offline. Each run removes
only its generated tables and random storage prefix. Native job startup can
outweigh avoided client transfers; benchmark the intended workload.

A local Python 3.14 run against the services above used an INT8 key and a
64-character string, one warmup and three measured samples per case. Times below
are median milliseconds (minimum–maximum). Native operations were slower at
all three sizes in this run; these small local samples do not predict cloud or
large distributed workloads.

.. list-table:: Local Parquet measurements
   :header-rows: 1

   * - Rows
     - Client export
     - Native export
     - Client import
     - Native import
   * - 100
     - 5.34 (4.89–5.36)
     - 12.78 (12.11–13.06)
     - 8.61 (7.83–9.08)
     - 107.03 (103.43–108.72)
   * - 1,000
     - 6.59 (6.24–6.73)
     - 13.55 (13.25–13.73)
     - 18.92 (18.75–21.98)
     - 112.92 (110.56–119.71)
   * - 10,000
     - 16.62 (16.50–17.99)
     - 23.18 (20.85–28.36)
     - 76.41 (75.57–92.64)
     - 129.86 (122.95–139.74)

Native imports produced two to four failed availability probes per sample, with
observed spans between 21.3 and 63.2 ms. Client imports produced none. Polling
was every 20 ms with a 250 ms query timeout. These spans are sampled
observations of one run, not exact table-offline durations, and repeated runs
move both the probe counts and the spans.
