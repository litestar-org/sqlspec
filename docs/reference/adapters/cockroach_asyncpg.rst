=====================
CockroachDB + AsyncPG
=====================

CockroachDB adapter using asyncpg with automatic transaction retry logic.

Configuration
=============

.. autoclass:: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgPoolConfig
   :members:
   :show-inheritance:

Driver
======

.. autoclass:: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgDriver
   :members:
   :show-inheritance:

Retry Configuration
===================

.. autoclass:: sqlspec.adapters.cockroach_asyncpg.CockroachAsyncpgRetryConfig
   :members:
   :show-inheritance:

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.cockroach_asyncpg.data_dictionary.CockroachAsyncpgDataDictionary
   :members:
   :show-inheritance:

Native object storage
=====================

Set ``driver_features={"enable_native_storage": True}`` to use CockroachDB
EXPORT/IMPORT for remote CSV and Parquet. The feature is disabled by default.
Exports create generated files below a prefix; imports append into existing
tables, take them offline and invalidate foreign keys. Asyncpg requires no
additional autocommit setting. Calls made inside an active transaction use the
inherited client-side path.

.. code-block:: python

    import os
    from urllib.parse import urlsplit, urlunsplit
    from uuid import uuid4

    from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig

    config = CockroachAsyncpgConfig(
        connection_config={"dsn": os.environ["DATABASE_URL"]},
        driver_features={
            "enable_native_storage": True,
            "native_storage_csv_options": {"skip": 0},
        },
    )
    parts = urlsplit(os.environ["COCKROACH_STORAGE_URI"])
    destination = urlunsplit(
        parts._replace(path=parts.path.rstrip("/") + "/" + uuid4().hex)
    )
    try:
        async with config.provide_session() as driver:
            exported = await driver.select_to_storage(
                "SELECT :id::INT8 AS id, :label::STRING AS label",
                destination,
                {"id": 7, "label": "O'Reilly"},
                format_hint="csv",
            )
            prefix = urlsplit(exported.telemetry["destination"])
            for filename in exported.telemetry["extra"]["files"]:
                source = urlunsplit(
                    prefix._replace(path=prefix.path.rstrip("/") + "/" + filename)
                )
                await driver.load_from_storage("archive_items", source, file_format="csv")
    finally:
        await config.close_pool()

The example assumes an existing ``archive_items(id INT8, label STRING)`` table
and a server-readable storage URI. CSV export is headerless: ``skip=0`` imports
all rows. Use ``skip=1`` for an external file with one header. Without explicit
``skip``, CSV import uses the inherited header-aware reader. NULL markers
(``nullas`` for export, ``nullif`` for import) are optional explicit strings;
choose values absent from literal data. Unconfigured NULL export fails.
Parquet avoids marker collisions and is the default export format.

Remote aliases resolve through the storage registry. Disabled, local,
unsupported-format/protocol and ``overwrite=True`` calls use the inherited
path. Native failures propagate without an inherited retry. File metadata,
privileges, cleanup, supported query forms, server restrictions and benchmark
measurement details match :doc:`cockroach_psycopg`.

Local tests cover CockroachDB CCL v26.2.5 with RustFS. CockroachDB Cloud tiers,
hosted privileges, cloud credentials, GCS and Azure remain unverified.
