Testing
=======

SQLSpec provides comprehensive support for both fast in-memory unit tests and containerized
integration testing against real database engines.

Unit Testing with SQLite
------------------------

For fast unit tests without external service dependencies, SQLite in-memory (``:memory:``)
or temporary file databases via pytest's ``tmp_path`` fixture are recommended.

Fast In-Memory Fixture
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    import pytest
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    @pytest.fixture
    def db_session():
        spec = SQLSpec()
        config = spec.add_config(
            SqliteConfig(connection_config={"database": ":memory:"})
        )
        with spec.provide_session(config) as session:
            session.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            yield session
        config.close_pool()

Temporary File Fixture
~~~~~~~~~~~~~~~~~~~~~~

Use ``tmp_path`` when testing multi-connection scenarios, migrations, or file-backed storage:

.. code-block:: python

    from pathlib import Path
    import pytest
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    @pytest.fixture
    def db_config(tmp_path: Path) -> SqliteConfig:
        spec = SQLSpec()
        config = spec.add_config(
            SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
        )
        with spec.provide_session(config) as session:
            session.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            session.execute_many(
                "INSERT INTO users (name) VALUES (?)",
                [("Alice",), ("Bob",)],
            )
        yield config
        config.close_pool()

    def test_get_users(db_config: SqliteConfig) -> None:
        with db_config.provide_session() as session:
            count = session.select_value("SELECT COUNT(*) FROM users")
            assert count == 2

Integration Testing with ``pytest-databases``
---------------------------------------------

For integration testing against production-grade database systems, SQLSpec integrates
directly with `pytest-databases <https://github.com/litestar-org/pytest-databases>`_.
``pytest-databases`` provisions and manages Docker containers for database engines and
handles database isolation across parallel ``pytest-xdist`` workers.

Supported Database Services
~~~~~~~~~~~~~~~~~~~~~~~~~~~

``pytest-databases`` provides managed Docker fixtures for:

- ``PostgresService`` (PostgreSQL, pgvector, ParadeDB)
- ``MySQLService`` (MySQL, MariaDB)
- ``OracleService`` (Oracle Free / Express)
- ``MSSQLService`` (Microsoft SQL Server)
- ``BigQueryService`` (Google Cloud BigQuery emulator)
- ``SpannerService`` (Google Cloud Spanner emulator)
- ``CockroachDBService`` (CockroachDB)
- ``RustfsService`` (S3 / cloud blob storage emulator)

PostgreSQL Integration Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Install ``pytest-databases`` with PostgreSQL support:

.. code-block:: console

    uv add --dev "pytest-databases[postgres]"

Configure your pytest fixtures using the provided service fixture:

.. code-block:: python

    from collections.abc import AsyncGenerator
    import pytest
    from pytest_databases.docker.postgres import PostgresService
    from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver

    @pytest.fixture
    def asyncpg_config(postgres_service: PostgresService) -> AsyncpgConfig:
        """Create an AsyncpgConfig connected to the containerized test database."""
        return AsyncpgConfig(
            connection_config={
                "host": postgres_service.host,
                "port": postgres_service.port,
                "user": postgres_service.user,
                "password": postgres_service.password,
                "database": postgres_service.database,
            }
        )

    @pytest.fixture
    async def db_session(asyncpg_config: AsyncpgConfig) -> AsyncGenerator[AsyncpgDriver, None]:
        """Provide an active session with automatic pool teardown."""
        async with asyncpg_config.provide_session() as session:
            await session.execute(
                "CREATE TABLE IF NOT EXISTS items (id SERIAL PRIMARY KEY, title TEXT NOT NULL)"
            )
            yield session
        await asyncpg_config.close_pool()

    async def test_insert_and_fetch_item(db_session: AsyncpgDriver) -> None:
        result = await db_session.execute(
            "INSERT INTO items (title) VALUES (:title) RETURNING id",
            title="Widget",
        )
        item_id = result.last_inserted_id
        assert item_id is not None

        item = await db_session.select_one("SELECT title FROM items WHERE id = :id", id=item_id)
        assert item["title"] == "Widget"

Running Migrations in Test Fixtures
-----------------------------------

When testing against real databases, use ``migrate_up()`` to initialize the schema before tests:

.. code-block:: python

    @pytest.fixture
    async def migrated_config(postgres_service: PostgresService) -> AsyncGenerator[AsyncpgConfig, None]:
        config = AsyncpgConfig(
            connection_config={
                "host": postgres_service.host,
                "port": postgres_service.port,
                "user": postgres_service.user,
                "password": postgres_service.password,
                "database": postgres_service.database,
            },
            migration_config={
                "script_location": "migrations/postgres",
            },
        )
        # Apply all migrations up to head
        await config.migrate_up()

        yield config

        # Teardown connection pool
        await config.close_pool()

Parallel Testing with ``pytest-xdist``
--------------------------------------

``pytest-databases`` automatically allocates unique database names or schemas per xdist
worker process, preventing tests from colliding when running in parallel:

.. code-block:: console

    # Run tests in parallel across CPU cores
    uv run pytest -n auto

Testing Best Practices
----------------------

1. **Always clean up connection pools**: Call ``config.close_pool()`` or ``await config.close_pool()`` in fixture teardown to prevent hanging database connections.
2. **Use function-based tests**: Write asynchronous tests using ``async def test_...`` without wrapping them in test classes.
3. **Assert with ``select_value``**: For aggregate queries like counts, use ``session.select_value("SELECT COUNT(*)...")`` to retrieve scalar values directly.
4. **Bulk test seeding**: Seed test datasets efficiently with ``session.execute_many()`` or native ``driver.load_from_records()``.

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

        export_table_fixtures_sync(session, fixtures, tables=["users", "posts"], compress=True)

``load_table_fixtures_async`` and ``export_table_fixtures_async`` provide identical
functionality for asynchronous drivers:

.. code-block:: python

    from pathlib import Path
    import pytest
    from sqlspec.adapters.asyncpg import AsyncpgDriver
    from sqlspec.utils.fixtures import export_table_fixtures_async, load_table_fixtures_async

    fixtures = Path("tests/fixtures/tables")

    @pytest.fixture
    async def seeded_session(db_session: AsyncpgDriver) -> AsyncpgDriver:
        await load_table_fixtures_async(
            db_session,
            fixtures,
            table_order=["users", "posts"],
            conflict_keys={"users": ["id"]},
            resync_sequences=True,
        )
        await db_session.commit()
        return db_session

    async def test_export_fixtures(db_session: AsyncpgDriver) -> None:
        await export_table_fixtures_async(
            db_session,
            fixtures,
            tables=["users", "posts"],
            compress=True,
        )

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
  - ISO 8601 durations without year or month parts and numbers of seconds in
    ``interval`` columns to ``timedelta``, and DuckDB
    ``[months, days, nanoseconds]`` interval lists to interval text. Durations
    with year or month parts, such as ``P1Y`` or ``P1M``, are passed through as
    text, which some drivers, such as asyncpg, reject. Exported files never
    contain them;
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
- **Generated columns:** on PostgreSQL-family, MySQL, DuckDB, and SQLite
  drivers, columns the database computes (``GENERATED ALWAYS AS``) are left out
  of exported files, and their values in a loaded file are ignored, so files that
  contain them still load. Other databases are not checked for them, so SQL Server
  computed columns and Oracle virtual columns are exported and loaded like any
  other column.
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

- :doc:`configuration` for full adapter configuration options.
- :doc:`drivers_and_querying` for querying and transaction APIs.
- :doc:`migrations` for migration execution and schema tracking.
- :doc:`bulk_ingest` for high-volume data loading in tests.
- :doc:`/reference/utils` for fixture utility function signatures and API reference.
