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
values are always bound as parameters.

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
  names passed as ``tables``. Tables named in ``table_order`` load first in that
  order, and the rest follow alphabetically. The return value maps each table to
  its row count.
- **Upserts:** ``conflict_keys`` maps a table to the columns of a unique
  constraint. Rows for that table update the non-key columns of existing rows
  instead of failing. Without conflict keys, a duplicate row raises the
  database's integrity error.
- **Batching:** rows are inserted with ``execute_many`` in batches of
  ``batch_size`` (default 500).
- **Transactions:** the loader does not commit. Commit or roll back on the session
  yourself, or run the load inside a transaction.
- **Sequences:** with ``resync_sequences=True`` on a PostgreSQL-family driver, the
  sequence behind each serial or identity column is set so the next generated
  value follows the highest loaded id; an empty table starts again at 1. Other
  databases skip the option and log a debug message. On CockroachDB, identity
  columns are resynced, while ``SERIAL`` columns that default to
  ``unique_rowid()`` have no sequence and are left unchanged.
- **Exporting:** every row of each table is written to ``<table>.json``, or
  ``<table>.jsonl`` with ``jsonl=True``, gzipped by default (``compress=True``).
  The directory is created when missing. Export removes the table's other fixture
  files in the directory, such as an older ``users.json`` next to a new
  ``users.json.gz``, so the next load reads the exported file. Exported files load
  back with the same values.

Related Guides
--------------

- :doc:`configuration` for adapter configuration options.
- :doc:`drivers_and_querying` for the full query API.
