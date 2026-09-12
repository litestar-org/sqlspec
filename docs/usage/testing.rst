Testing
=======

SQLSpec provides tools for both unit and integration testing of database code.

Pytest Fixture Tips
-------------------

- Use ``tmp_path`` (pytest built-in) for SQLite file databases
- Use ``:memory:`` for fast in-memory tests
- Create factory functions for reusable test setup
- Use ``execute_many`` for bulk fixture data
- Use ``select_value`` for assertion checks on counts

.. code-block:: python

    import pytest
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    @pytest.fixture
    def db(tmp_path):
        spec = SQLSpec()
        config = spec.add_config(
            SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
        )
        with spec.provide_session(config) as session:
            session.execute("create table users (id integer primary key, name text)")
            yield session

Integration Test Patterns
-------------------------

For integration tests against real databases, use the standard ``SQLSpec`` +
adapter config pattern with temporary databases.

.. literalinclude:: /examples/patterns/integration_testing.py
   :language: python
   :caption: ``integration test fixtures``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Table Fixtures
--------------

``sqlspec.utils.fixtures`` loads and exports table data as one file per table.
A table reads from ``<table>.json`` (a JSON array of row objects) or
``<table>.jsonl`` (one row object per line), either optionally gzipped with a
``.gz`` suffix. Schema-qualified tables use the qualified name, such as
``app.users.json``. Every row in a file must have the same keys; they become the
inserted columns. Table and column names must be plain SQL identifiers, and row
values are always bound as parameters. Files and exported tables are read into
memory, so these helpers suit fixture-sized data rather than bulk transfers.

.. code-block:: python

    from pathlib import Path

    from sqlspec.adapters.psycopg import PsycopgSyncConfig
    from sqlspec.utils.fixtures import export_table_fixtures_sync, load_table_fixtures_sync

    config = PsycopgSyncConfig(connection_config={"conninfo": "postgresql://localhost/app"})
    fixtures = Path("tests/fixtures/tables")

    with config.provide_session() as session:
        counts = load_table_fixtures_sync(
            session,
            fixtures,
            table_order=["users", "posts"],
            conflict_keys={"users": ["id"]},
            resync_sequences=True,
        )
        session.commit()

        export_table_fixtures_sync(session, fixtures, ["users", "posts"], compress=True)

``load_table_fixtures_async`` and ``export_table_fixtures_async`` take the same
arguments with an async driver.

- **Which tables load:** every table fixture file in the directory, or only the
  names passed as ``tables``. Files whose names are not table identifiers are
  skipped, and a table with more than one fixture file (for example
  ``users.json`` and ``users.jsonl.gz``) raises ``ValueError``. A dot in a file
  name marks a schema-qualified table, so a copy such as ``users.backup.json``
  in the directory is loaded as table ``backup`` in schema ``users``; keep
  backups elsewhere or pass ``tables``. Discovered schema-qualified names are
  logged at debug level. Tables named in ``table_order`` load first in that
  order, and the rest follow alphabetically. The return value maps each table to
  its row count.
- **Exact names:** table and column names are quoted in every statement, so they
  must match the database spelling exactly, including case. Reserved words such
  as ``order`` work as table or column names. On PostgreSQL, unqualified tables
  resolve through the session's search path. The query builder renders Oracle
  names unquoted.
- **Column types:** before inserting, the loader reads the table's columns from
  the driver's data dictionary on PostgreSQL-family, MySQL, DuckDB, and SQLite
  drivers and converts these JSON values:

  - ISO 8601 strings in ``timestamp``/``timestamptz``/``datetime`` columns to
    ``datetime``, in ``date`` columns to ``date``, and in ``time`` columns, with
    or without a time zone, to ``time`` (not on MySQL);
  - ISO 8601 durations and numbers of seconds in ``interval`` columns to
    ``timedelta``, and DuckDB ``[months, days, nanoseconds]`` interval lists to
    interval text;
  - strings and numbers in ``numeric``/``decimal`` columns to ``Decimal``;
  - strings in ``uuid`` columns to ``UUID``;
  - base64 strings in ``bytea``, ``blob``, ``tinyblob``, ``mediumblob``,
    ``longblob``, ``binary``, and ``varbinary`` columns to bytes (the only
    conversion on SQLite);
  - values of PostgreSQL and MySQL ``json``/``jsonb`` columns to JSON text.

  Every other value is passed to the driver as decoded from JSON. That includes
  ``BIT`` columns (including MySQL ``BIT``),
  MySQL ``TIME``, DuckDB ``MAP``, and the elements of arrays, so arrays of dates,
  timestamps, or UUIDs are not converted. On other databases no column types are
  read, and only values the driver accepts as JSON-decoded strings, numbers,
  booleans, nulls, lists, and objects load.
- **Generated columns:** columns the database computes (``GENERATED ALWAYS AS``)
  are left out of exported files, and their values in a loaded file are ignored,
  so files that contain them still load.
- **Upserts:** ``conflict_keys`` maps a table to the columns of a unique
  constraint; every entry must name a table being loaded, spelled exactly. Rows
  for that table update the non-key columns of existing rows instead of failing;
  PostgreSQL ``GENERATED ALWAYS`` identity columns are never updated, and a table
  with nothing left to update skips the conflicting row. PostgreSQL-family,
  SQLite, and DuckDB drivers use ``ON CONFLICT``; MySQL uses
  ``ON DUPLICATE KEY UPDATE col = VALUES(col)``, which matches any unique key of
  the table. ``VALUES()`` is deprecated since MySQL 8.0.20 but kept because
  MariaDB does not support the row-alias form. Other dialects raise
  ``ValueError`` before any statement runs. Without conflict keys, a duplicate
  row raises the database's integrity error.
- **Identity columns:** on PostgreSQL, values for ``GENERATED ALWAYS`` identity
  columns are inserted with ``OVERRIDING SYSTEM VALUE``. CockroachDB does not
  accept explicit values for ``GENERATED ALWAYS`` columns; use
  ``GENERATED BY DEFAULT`` there.
- **Batching:** rows are inserted with ``execute_many`` in batches of
  ``batch_size`` (default 500).
- **Transactions:** the loader does not commit. Commit or roll back on the session
  yourself, or run the load inside a transaction.
- **Sequences:** with ``resync_sequences=True`` on a PostgreSQL-family driver, the
  sequence behind each serial or identity column is set so the next generated
  value follows the highest loaded value; for an empty table it restarts at the
  sequence's minimum value. Other databases skip the option and log a debug
  message. On CockroachDB, identity columns are resynced, while ``SERIAL``
  columns that default to ``unique_rowid()`` have no sequence and are left
  unchanged.
- **Exporting:** every row of each table is written to ``<table>.json``, or
  ``<table>.jsonl`` with ``jsonl=True``, gzipped by default (``compress=True``).
  Rows are ordered by the primary key, or by every column when there is none
  (PostgreSQL ``json``, ``xml``, and geometric columns, and arrays of them, are
  left out of the ordering), so repeated exports of unchanged data produce identical files.
  Dates and times are written as ISO 8601 strings, ``Decimal`` and ``UUID``
  values as strings, and bytes as base64 strings. Each file is written to a
  temporary file created with the process umask and moved into place; the
  table's other fixture files in the directory, such as an older ``users.json``
  next to a new ``users.json.gz``, are then removed so the next load reads the
  exported file. The directory is created when missing. Values of the column
  types listed above, and JSON-native values in other columns, load back
  unchanged. DuckDB ``MAP`` values and the offsets of DuckDB ``TIME WITH TIME
  ZONE`` values do not round-trip, and PostgreSQL interval months and years come
  back as days because the drivers return intervals as ``timedelta``.

Related Guides
--------------

- :doc:`configuration` for adapter configuration options.
- :doc:`drivers_and_querying` for the full query API.
