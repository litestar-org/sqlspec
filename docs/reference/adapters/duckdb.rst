======
DuckDB
======

Sync DuckDB adapter with full Arrow integration, extension management, and
secret configuration. DuckDB excels at analytical workloads and can query
Parquet, CSV, and JSON files directly.

Native object-store transfers
=============================

``select_to_storage()`` can write a remote Parquet or CSV object with DuckDB
``COPY``. ``load_from_storage()`` can append a remote Parquet object with
``INSERT ... SELECT ... FROM read_parquet()``. Each successful native transfer
executes one statement on the driver's existing connection and avoids
materializing the data through Python Arrow buffers.

Configure the required extension and secret through ``driver_features`` before
opening a session. Native routing uses the settings successfully applied to that
connection. A failed optional extension or secret does not enable the route.
Pool recreation resets this record. Filesystems registered through
``on_connection_create`` are discovered after the callback runs and participate
without requiring a cloud extension.

.. code-block:: python

    import os

    from sqlspec.adapters.duckdb import DuckDBConfig

    config = DuckDBConfig(
        driver_features={
            "extensions": [{"name": "httpfs", "install": True, "required": True}],
            "secrets": [{
                "name": "reports",
                "secret_type": "s3",
                "provider": "config",
                "required": True,
                "value": {
                    "key_id": os.environ["AWS_ACCESS_KEY_ID"],
                    "secret": os.environ["AWS_SECRET_ACCESS_KEY"],
                    "region": "us-east-1",
                },
            }],
        },
    )
    try:
        with config.provide_session() as session:
            session.select_to_storage(
                "SELECT :day AS report_day",
                "s3://example-bucket/reports/daily.parquet",
                day="2026-01-01",
            )
            session.execute("CREATE TABLE reports (report_day VARCHAR)")
            session.load_from_storage(
                "reports", "s3://example-bucket/reports/daily.parquet",
                file_format="parquet",
            )
    finally:
        config.close_pool()

Declared secrets are created only when missing. DuckDB secret names are shared by
every connection to a database and matched case-insensitively, and persistent secrets
outlive the process, so an existing secret is reused and declared values are not
applied to it unless ``replace=True`` is set, for example after rotating credentials.
DuckDB redacts credential values, so an existing secret is compared on its type,
provider, scope, and unredacted values such as ``key_id``, ``region`` and ``endpoint``.
A difference raises for ``required=True`` secrets and logs a warning otherwise.

Query values and object addresses remain bound parameters. Native import accepts
one table identifier, optionally schema-qualified or quoted; SQL fragments are
rejected. Parquet import disables automatic Hive partition columns. The API
handles one object and does not expose reader projection or filter pushdown.

Providers and aliases
---------------------

.. list-table:: Native provider prerequisites
   :header-rows: 1
   :widths: 20 20 60

   * - URI schemes
     - Extension
     - Secret settings
   * - ``s3``
     - ``httpfs``
     - ``secret_type="s3"`` with static credentials or a configured credential chain.
   * - ``gs``, ``gcs``
     - ``httpfs``
     - ``secret_type="gcs"`` with HMAC ``key_id`` and ``secret``. GCS OAuth and
       service-account backend options are not interchangeable with HMAC keys.
   * - ``r2``
     - ``httpfs``
     - ``secret_type="r2"`` with ``key_id``, ``secret`` and ``account_id`` or
       the matching R2 endpoint.
       R2 uses the S3 API through DuckDB's ``httpfs`` extension.
   * - ``gcss``
     - Community ``gcs``
     - DuckDB's ``gcp`` secret configuration, including application default
       credentials. The original URI is passed to the extension unchanged.
   * - ``az``, ``azure``, ``abfss``
     - ``azure``
     - ``secret_type="azure"`` with a configured ``connection_string`` or
       supported identity provider. Direct URI resolution also needs an account
       name in the successful secret configuration.
   * - ``http``, ``https``
     - ``httpfs``
     - Parquet reads only, with default backend options.

Existing ``alias://name/path`` destinations use the storage registry. Native
routing requires a known backend whose credentials, endpoint, region and access
options match the configured DuckDB secret. Unknown options and credential
mismatches keep the Arrow route. Alias names are not provider names: an alias
named ``gcs`` can still refer to S3. Custom storage backend classes and custom
``storage_pipeline_factory`` implementations retain their existing behavior.

Additional filesystems
----------------------

DuckDB's Python client supports registered fsspec filesystems, including
``gcsfs``. Register an instance using ``connection.register_filesystem()`` in
``on_connection_create``. Its advertised protocols are discovered automatically;
eligible transfers use DuckDB's reader or ``COPY`` without materializing an
Arrow payload in SQLSpec. The filesystem itself can still perform Python I/O.

For additional DuckDB extensions, declare their URI schemes alongside the
existing extension configuration, for example
``{"name": "my_filesystem", "repository": "community",
"storage_protocols": ["myfs", "myfs+alias"]}``. These schemes become available
only after the extension loads successfully. SQLSpec passes direct addresses
unchanged; the configured DuckDB filesystem owns authentication and reports
transfer errors. No matching fsspec or obstore package is required for these
direct routes. Aliases still resolve through SQLSpec's registry, and backend
options that cannot be preserved keep the existing storage path.

See DuckDB's `fsspec integration
<https://duckdb.org/docs/current/guides/python/filesystems>`_ and the
`community GCS extension
<https://duckdb.org/community_extensions/extensions/gcs>`_.

Fallbacks and failures
----------------------

Local paths, unsupported formats, explicit Arrow conversion options such as
``arrow_schema``, and options that cannot be represented faithfully use Arrow.
Import with ``overwrite=True`` also uses the existing Arrow truncate-and-insert
path. CSV imports always use Arrow: DuckDB's CSV inference differs for values
such as leading-zero strings, quoted empty strings and null markers.

Names containing ``?`` or ``#`` retain the storage backend's interpretation
instead of becoming DuckDB URL settings. Parquet reader names containing glob
characters also use the single-object Arrow path. Repeated native exports replace
the same object; they do not add a new overwrite argument or produce partitioned
files. The ``partitioner`` argument continues to attach telemetry metadata.

Routing is decided before the transfer statement. Once native execution starts,
authentication, missing-object and database failures propagate through the
adapter's exception mapping; they are never retried through Arrow.

Native jobs report row counts, duration, format and ``backend="duckdb"``.
``bytes_processed`` is omitted when DuckDB does not provide it; no extra object
request is made solely to fill that field.

Verification and performance
----------------------------

S3-compatible CSV/Parquet export and Parquet append are tested against local
RustFS. Configured GCS, R2 and Azure routing is tested offline; transfers against
those cloud services are not verified by the local tests. Extension installation
may require access to DuckDB's official extension repository.

``tools/scripts/bench_duckdb_storage.py`` compares native and Arrow Parquet
export/import against the same local service. It verifies equal values and
records object sizes, median and spread, connection/extension setup time,
versions and source revision. Run the opt-in fixture-backed benchmark with:

.. code-block:: console

    SQLSPEC_DUCKDB_STORAGE_BENCHMARK=/tmp/duckdb-storage.json uv run pytest \
      tests/integration/adapters/duckdb/duckdb/test_native_storage.py \
      -k native_storage_benchmark

The benchmark uses 100, 1,000 and 10,000 rows with four warmups and eight measured
iterations. Results depend on object size, network and service configuration;
native transfer is not a universal speedup. CSV import is excluded from native
benchmarks because it retains Arrow inference.

Configuration
=============

.. autoclass:: sqlspec.adapters.duckdb.DuckDBConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.duckdb.config.DuckDBExtensionConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.duckdb.config.DuckDBSecretConfig
   :members:
   :show-inheritance:

Driver
======

.. autoclass:: sqlspec.adapters.duckdb.DuckDBDriver
   :members:
   :show-inheritance:

Connection Pool
===============

.. autoclass:: sqlspec.adapters.duckdb.DuckDBConnectionPool
   :members:
   :show-inheritance:

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.duckdb.data_dictionary.DuckDBDataDictionary
   :members:
   :show-inheritance:
