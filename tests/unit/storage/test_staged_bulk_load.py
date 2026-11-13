"""Unit tests for staged bulk load, dialect-aware upsert/merge, type casts, and load_from_records."""

from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, Literal, cast
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.aiosqlite._typing import AiosqliteConnection
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.spanner._typing import SpannerTransaction
from sqlspec.adapters.spanner.driver import SpannerSyncDriver
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sybase import SybaseDriver
from sqlspec.adapters.sybase._typing import SybaseConnection
from sqlspec.adapters.sybase.core import default_statement_config as sybase_statement_config
from sqlspec.exceptions import SQLSpecError
from sqlspec.storage import StorageBridgeJob
from sqlspec.storage._staging_sql import (
    build_checkpoint_insert_sql,
    build_checkpoint_select_sql,
    build_checkpoint_table_ddl,
    build_create_table_from_arrow_schema,
    build_drop_stage_table_sql,
    build_stage_merge_sql,
    build_stage_table_ddl,
    format_storage_identifier,
    quote_dialect_column,
    resolve_stage_table_name,
    validate_sql_cast_type,
)


def test_resolve_stage_table_name_auto_and_custom() -> None:
    """resolve_stage_table_name generates a unique stage identifier or preserves an explicit string."""
    auto_name = resolve_stage_table_name("public.users", True)
    assert auto_name is not None
    assert auto_name.startswith("public.users_stage_")
    assert resolve_stage_table_name("public.users", "custom_stage_tbl") == "custom_stage_tbl"
    assert resolve_stage_table_name("public.users", False) is None


@pytest.mark.parametrize(
    ("dialect", "expected_fragment"),
    [
        ("postgres", "CREATE UNLOGGED TABLE"),
        ("duckdb", "CREATE TEMP TABLE"),
        ("sqlite", "CREATE TEMP TABLE"),
        ("mysql", "CREATE TEMPORARY TABLE"),
        ("mssql", "SELECT TOP 0"),
        ("oracle", "NOLOGGING"),
        ("bigquery", "CREATE TABLE IF NOT EXISTS"),
        ("spanner", "CREATE TABLE"),
    ],
)
def test_build_stage_table_ddl_across_dialects(dialect: str, expected_fragment: str) -> None:
    """Stage table DDL builder produces dialect-appropriate temporary/staging DDL."""
    ddl_list = build_stage_table_ddl(dialect, "target_items", "_stage_items")
    assert any(expected_fragment in stmt for stmt in ddl_list)


@pytest.mark.parametrize(
    ("dialect", "on_conflict", "expected_fragment"),
    [
        ("postgres", "update", "ON CONFLICT"),
        ("postgres", "nothing", "DO NOTHING"),
        ("cockroach", "update", "ON CONFLICT"),
        ("sqlite", "update", "ON CONFLICT"),
        ("duckdb", "nothing", "DO NOTHING"),
        ("mysql", "update", "ON DUPLICATE KEY UPDATE"),
        ("mysql", "nothing", "INSERT IGNORE INTO"),
        ("mssql", "update", "MERGE INTO"),
        ("oracle", "update", "MERGE INTO"),
        ("bigquery", "update", "MERGE INTO"),
    ],
)
def test_build_stage_merge_sql_across_dialects(
    dialect: str, on_conflict: Literal["update", "nothing"], expected_fragment: str
) -> None:
    """Stage merge SQL builder produces dialect-appropriate upsert/ignore statements."""
    sql = build_stage_merge_sql(
        dialect,
        "target_items",
        "_stage_items",
        ["id", "name", "metadata"],
        conflict_keys=["id"],
        on_conflict=on_conflict,
        exclude_update_columns=["created_at"],
        type_casts={"metadata": "JSONB"} if dialect == "postgres" else None,
    )
    assert expected_fragment in sql
    if dialect == "postgres":
        assert 'CAST("metadata" AS JSONB)' in sql

    drop_sql = build_drop_stage_table_sql(dialect, "_stage_items")
    assert "DROP TABLE" in drop_sql


def test_sqlite_staged_bulk_load_upsert_nothing_and_records(tmp_path: Path) -> None:
    """SqliteDriver load_from_arrow and load_from_records support staged upsert, DO NOTHING, and cleanup."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "staged.db")})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
        initial = pa.Table.from_pydict({"id": [1, 2], "name": ["alice", "bob"], "score": [10, 20]})
        job1 = driver.load_from_arrow("users", initial, stage_table=True, conflict_keys=["id"], on_conflict="update")
        assert job1.telemetry["rows_processed"] == 2

        delta = pa.Table.from_pydict({"id": [2, 3], "name": ["bob_updated", "carol"], "score": [99, 30]})
        driver.load_from_arrow(
            "users",
            delta,
            stage_table=True,
            conflict_keys=["id"],
            on_conflict="update",
            exclude_update_columns=["name"],
        )
        rows = driver.select("SELECT id, name, score FROM users ORDER BY id")
        assert rows == [
            {"id": 1, "name": "alice", "score": 10},
            {"id": 2, "name": "bob", "score": 99},
            {"id": 3, "name": "carol", "score": 30},
        ]

        ignored = [{"id": 3, "name": "carol_ignored", "score": 999}, {"id": 4, "name": "dave", "score": 40}]
        driver.load_from_records(
            "users",
            ignored,
            stage_table="kept_stage_users",
            conflict_keys=["id"],
            on_conflict="nothing",
            drop_stage_table=False,
        )
        rows_after = driver.select("SELECT id, name, score FROM users ORDER BY id")
        assert len(rows_after) == 4
        assert rows_after[2]["score"] == 30
        stage_rows = driver.select("SELECT id FROM kept_stage_users ORDER BY id")
        assert len(stage_rows) == 2


def test_staged_bulk_load_drops_stage_table_on_merge_failure(tmp_path: Path) -> None:
    """Stage table is dropped in finally even if merge execution raises an error."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "staged_fail.db")})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")
        table = pa.Table.from_pydict({"id": [1], "val": ["ok"]})
        with pytest.raises(Exception):
            driver.load_from_arrow(
                "items", table, stage_table="transient_stage", conflict_keys=["nonexistent_key"], on_conflict="update"
            )
        remaining = driver.select(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name = 'transient_stage'"
        )
        assert remaining == []


@pytest.mark.anyio
async def test_aiosqlite_and_duckdb_staged_bulk_load_with_type_casts(tmp_path: Path) -> None:
    """Async Aiosqlite and DuckDB staged loads support type_casts and conflict resolution."""
    duck_cfg = DuckDBConfig(connection_config={"database": ":memory:"})
    with duck_cfg.provide_session() as duck_driver:
        duck_driver.execute("CREATE TABLE metrics (id INTEGER PRIMARY KEY, amount INTEGER)")
        duck_driver.load_from_records(
            "metrics",
            [{"id": 1, "amount": "100"}, {"id": 2, "amount": "200"}],
            stage_table=True,
            conflict_keys=["id"],
            on_conflict="update",
            type_casts={"amount": "INTEGER"},
        )
        rows = duck_driver.select("SELECT id, amount FROM metrics ORDER BY id")
        assert rows == [{"id": 1, "amount": 100}, {"id": 2, "amount": 200}]

    aio_cfg = AiosqliteConfig(connection_config={"database": str(tmp_path / "async_staged.db")})
    try:
        async with aio_cfg.provide_session() as aio_driver:
            await aio_driver.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, label TEXT)")
            await aio_driver.load_from_records(
                "events",
                [{"id": 1, "label": "v1"}, {"id": 2, "label": "v2"}],
                stage_table=True,
                conflict_keys=["id"],
                on_conflict="update",
            )
            await aio_driver.load_from_records(
                "events",
                [{"id": 2, "label": "v2_updated"}, {"id": 3, "label": "v3"}],
                stage_table=True,
                conflict_keys=["id"],
                on_conflict="update",
            )
            async_rows = await aio_driver.select("SELECT id, label FROM events ORDER BY id")
            assert async_rows == [{"id": 1, "label": "v1"}, {"id": 2, "label": "v2_updated"}, {"id": 3, "label": "v3"}]
    finally:
        await aio_cfg.close_pool()


def test_spanner_load_from_arrow_uses_insert_or_ignore_when_on_conflict_nothing() -> None:
    """SpannerSyncDriver uses writer.insert_or_ignore when conflict_keys + on_conflict='nothing' is set."""
    mock_tx = MagicMock(spec=SpannerTransaction)
    mock_tx.insert_or_ignore = MagicMock()
    mock_tx.insert_or_update = MagicMock()
    driver = SpannerSyncDriver(
        connection=mock_tx, driver_features={"storage_capabilities": {"arrow_import_enabled": True}}
    )
    table = pa.Table.from_pydict({"id": [1, 2], "name": ["a", "b"]})
    driver.load_from_arrow("users", table, conflict_keys=["id"], on_conflict="nothing")
    assert mock_tx.insert_or_ignore.called
    assert not mock_tx.insert_or_update.called


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        ("sybase", "SELECT TOP 0 * INTO audit.staged FROM audit.items"),
        ("sqlanywhere", "SELECT * INTO audit.staged FROM audit.items WHERE 1=0"),
    ],
)
def test_sap_stage_table_ddl_uses_select_into(dialect: str, expected: str) -> None:
    assert build_stage_table_ddl(dialect, "audit.items", "audit.staged") == [expected]


def test_sybase_stage_upsert_updates_then_inserts_with_composite_keys_and_casts() -> None:
    statement = build_stage_merge_sql(
        "sybase",
        "audit.items",
        "audit.staged",
        ["tenant_id", "id", "value", "created_at"],
        conflict_keys=["tenant_id", "id"],
        exclude_update_columns=["created_at"],
        type_casts={"value": "INTEGER"},
    )
    assert statement == (
        "UPDATE audit.items SET [value] = CAST(stg.[value] AS INTEGER) "
        "FROM audit.items tgt, audit.staged stg "
        "WHERE tgt.[tenant_id] = stg.[tenant_id] AND tgt.[id] = stg.[id]; "
        "INSERT INTO audit.items ([tenant_id], [id], [value], [created_at]) "
        "SELECT stg.[tenant_id], stg.[id], CAST(stg.[value] AS INTEGER), stg.[created_at] FROM audit.staged stg "
        "WHERE NOT EXISTS (SELECT 1 FROM audit.items tgt "
        "WHERE tgt.[tenant_id] = stg.[tenant_id] AND tgt.[id] = stg.[id])"
    )


@pytest.mark.parametrize(("on_conflict", "exclude"), [("nothing", []), ("update", ["value"])])
def test_sybase_stage_insert_only_when_updates_are_disabled(
    on_conflict: Literal["update", "nothing"], exclude: list[str]
) -> None:
    assert build_stage_merge_sql(
        "sybase",
        "items",
        "staged",
        ["id", "value"],
        conflict_keys=["id"],
        on_conflict=on_conflict,
        exclude_update_columns=exclude,
    ) == (
        "INSERT INTO items ([id], [value]) SELECT stg.[id], stg.[value] FROM staged stg "
        "WHERE NOT EXISTS (SELECT 1 FROM items tgt WHERE tgt.[id] = stg.[id])"
    )
    assert build_drop_stage_table_sql("sybase", "audit.staged") == "DROP TABLE audit.staged"


def test_sqlanywhere_stage_merge_preserves_non_primary_conflict_keys() -> None:
    assert build_stage_merge_sql(
        "sqlanywhere",
        "items",
        "staged",
        ["id", "external_key", "value"],
        conflict_keys=["external_key"],
        exclude_update_columns=["id"],
    ) == (
        'MERGE INTO items tgt USING staged stg ON (tgt."external_key" = stg."external_key")'
        ' WHEN MATCHED THEN UPDATE SET tgt."value" = stg."value"'
        ' WHEN NOT MATCHED THEN INSERT ("id", "external_key", "value") VALUES (stg."id", stg."external_key", stg."value")'
    )


class _StorageNativeCursor:
    description = None
    lastrowid = None

    def __init__(self, connection: "_StorageNativeConnection") -> None:
        self.connection = connection
        self.rowcount = 1
        self.closed = False
        connection.open_cursors += 1

    def execute(self, statement: str, parameters: Any = None) -> None:
        self.connection.statements.append(statement)
        if self.connection.fail_merge and statement.startswith("UPDATE"):
            raise RuntimeError("native merge failure")

    def executemany(self, statement: str, parameters: Sequence[Sequence[Any]]) -> None:
        self.connection.statements.append(statement)
        self.connection.inserted_rows.extend(tuple(row) for row in parameters)
        self.rowcount = len(parameters)

    def fetchall(self) -> list[Any]:
        return []

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self.connection.open_cursors -= 1


class _StorageNativeConnection:
    in_transaction = True

    def __init__(self, *, fail_merge: bool) -> None:
        self.fail_merge = fail_merge
        self.statements: list[str] = []
        self.inserted_rows: list[tuple[Any, ...]] = []
        self.open_cursors = 0

    def cursor(self) -> _StorageNativeCursor:
        return _StorageNativeCursor(self)

    def execute(self, statement: str, parameters: Any = None) -> _StorageNativeCursor:
        cursor = self.cursor()
        try:
            cursor.execute(statement, parameters)
        except BaseException:
            cursor.close()
            raise
        return cursor


class _StorageAsyncNativeCursor:
    description = None
    lastrowid = None

    def __init__(self, cursor: _StorageNativeCursor) -> None:
        self.cursor = cursor

    @property
    def rowcount(self) -> int:
        return self.cursor.rowcount

    async def execute(self, statement: str, parameters: Any = None) -> None:
        self.cursor.execute(statement, parameters)

    async def executemany(self, statement: str, parameters: Sequence[Sequence[Any]]) -> None:
        self.cursor.executemany(statement, parameters)

    async def close(self) -> None:
        self.cursor.close()


class _StorageAsyncNativeConnection:
    in_transaction = True

    def __init__(self, connection: _StorageNativeConnection) -> None:
        self._conn = connection

    async def cursor(self) -> _StorageAsyncNativeCursor:
        return _StorageAsyncNativeCursor(self._conn.cursor())

    async def _execute(self, function: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        return function(*args, **kwargs)


class _SybaseAsyncStorageDriver(AiosqliteDriver):
    __slots__ = ()

    dialect = "sybase"


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("from_storage", [False, True], ids=["arrow", "storage"])
@pytest.mark.parametrize("fail_merge", [False, True], ids=["success", "failure"])
async def test_sybase_staged_upserts_execute_native_inserts_and_always_cleanup(
    tmp_path: Path, asynchronous: bool, from_storage: bool, fail_merge: bool
) -> None:
    import pyarrow.parquet as pq

    source = pa.table({"id": [1], "value": ["value"]})
    native = _StorageNativeConnection(fail_merge=fail_merge)
    config = sybase_statement_config.replace(enable_caching=False)
    features = {"storage_capabilities": {"arrow_import_enabled": True}}
    driver = (
        _SybaseAsyncStorageDriver(
            cast("AiosqliteConnection", _StorageAsyncNativeConnection(native)),
            statement_config=config,
            driver_features=features,
        )
        if asynchronous
        else SybaseDriver(cast("SybaseConnection", native), statement_config=config, driver_features=features)
    )
    source_path = tmp_path / "input.parquet"
    pq.write_table(source, source_path)

    async def run_load() -> StorageBridgeJob:
        if isinstance(driver, _SybaseAsyncStorageDriver):
            if from_storage:
                return await driver.load_from_storage(
                    "items", source_path, file_format="parquet", stage_table="staged", conflict_keys=["id"]
                )
            return await driver.load_from_arrow("items", source, stage_table="staged", conflict_keys=["id"])
        if from_storage:
            return driver.load_from_storage(
                "items", source_path, file_format="parquet", stage_table="staged", conflict_keys=["id"]
            )
        return driver.load_from_arrow("items", source, stage_table="staged", conflict_keys=["id"])

    if fail_merge:
        error_type = RuntimeError if asynchronous else SQLSpecError
        with pytest.raises(error_type, match="native merge failure"):
            await run_load()
    else:
        result = await run_load()
        assert result.telemetry["rows_processed"] == 1

    bulk_insert = (
        'INSERT INTO "staged" ("id", "value") VALUES (?, ?)'
        if asynchronous
        else "INSERT INTO [staged] ([id], [value]) VALUES (%s, %s)"
    )
    expected = [
        "SELECT TOP 0 * INTO staged FROM items",
        bulk_insert,
        "UPDATE items SET [value] = stg.[value] FROM items tgt, staged stg WHERE tgt.[id] = stg.[id]",
    ]
    if not fail_merge:
        expected.append(
            "INSERT INTO items ([id], [value]) SELECT stg.[id], stg.[value] FROM staged stg "
            "WHERE NOT EXISTS (SELECT 1 FROM items tgt WHERE tgt.[id] = stg.[id])"
        )
    expected.append("DROP TABLE staged")
    assert native.statements == expected
    assert native.inserted_rows == [(1, "value")]
    assert native.open_cursors == 0


@pytest.mark.parametrize("bad_identifier", ["", "   ", "bad\x00name", "schema.  .table"])
def test_format_storage_identifier_rejects_empty_and_nul(bad_identifier: str) -> None:
    """format_storage_identifier rejects empty, whitespace, NUL, and malformed qualified segments."""
    with pytest.raises(ValueError, match="Storage identifier"):
        format_storage_identifier(bad_identifier, allow_qualified=True)


def test_snowflake_staging_sql_identifiers_arrow_ddl_and_merge_quoting() -> None:
    """Snowflake staging SQL helpers validate identifiers, Arrow DDL, and quoted merge columns."""
    assert format_storage_identifier("analytics.events", allow_qualified=True) == "analytics.events"
    assert format_storage_identifier('"Mixed""Schema"."Order Items"', allow_qualified=True) == (
        '"Mixed""Schema"."Order Items"'
    )
    assert format_storage_identifier("order-items") == '"order-items"'

    arrow_table = pa.table({"id": [1], "order total": [9.5]})
    assert build_create_table_from_arrow_schema("sales.orders", arrow_table) == (
        'CREATE TABLE IF NOT EXISTS sales.orders (id BIGINT, "order total" DOUBLE PRECISION)'
    )
    assert build_create_table_from_arrow_schema("sales.orders", arrow_table.schema, if_not_exists=False) == (
        'CREATE TABLE sales.orders (id BIGINT, "order total" DOUBLE PRECISION)'
    )
    with pytest.raises(ValueError, match="at least one field"):
        build_create_table_from_arrow_schema("sales.orders", pa.schema([]))

    snowflake_merge = build_stage_merge_sql(
        "snowflake",
        "sales.orders",
        "sales.orders_stage",
        ["id", "payload", "created_at"],
        conflict_keys=["id"],
        exclude_update_columns=["created_at"],
        type_casts={"payload": "VARIANT"},
    )
    assert 'ON (tgt."id" = stg."id")' in snowflake_merge
    assert 'UPDATE SET tgt."payload" = CAST(stg."payload" AS VARIANT)' in snowflake_merge
    assert "created_at = stg" not in snowflake_merge
    assert 'CAST(stg."payload" AS VARIANT)' in snowflake_merge


@pytest.mark.parametrize(
    ("dialect", "raw_col", "expected_col", "raw_ident", "expected_ident"),
    [
        ("postgres", 'col"name', '"col""name"', "public.order", 'public."order"'),
        ("mysql", "col`name", "`col``name`", "analytics.order-items", "analytics.`order-items`"),
        ("bigquery", "col`name", "`col``name`", "dataset.select", "dataset.`select`"),
        ("spanner", "col`name", "`col``name`", "order", "`order`"),
        ("mssql", "col]name", "[col]]name]", "dbo.order items", "dbo.[order items]"),
        ("sybase", "col]name", "[col]]name]", "dbo.select", "dbo.[select]"),
    ],
)
def test_dialect_native_storage_quoting_and_checkpoint_sql(
    dialect: str, raw_col: str, expected_col: str, raw_ident: str, expected_ident: str
) -> None:
    """quote_dialect_column, format_storage_identifier, and checkpoint SQL builders quote per dialect."""
    assert quote_dialect_column(raw_col, dialect=dialect) == expected_col
    assert format_storage_identifier(raw_ident, dialect=dialect, allow_qualified=True) == expected_ident
    assert expected_ident in build_checkpoint_table_ddl(dialect, raw_ident)
    assert expected_ident in build_checkpoint_select_sql(raw_ident, dialect=dialect)
    assert expected_ident in build_checkpoint_insert_sql(dialect, raw_ident)


@pytest.mark.parametrize("valid_type", ["INTEGER", "DECIMAL(18, 4)", "VARCHAR(255)", "JSONB"])
def test_validate_sql_cast_type_accepts_standard_types(valid_type: str) -> None:
    """validate_sql_cast_type normalizes valid scalar and parameterized SQL data types."""
    rendered = validate_sql_cast_type(valid_type, dialect="postgres")
    assert rendered


@pytest.mark.parametrize(
    "invalid_type",
    ["", "   ", "INT\x00", "INT; SELECT 1", "INT) FROM users --", "INT /* comment */", "VARCHAR('x')", "DECIMAL((18)"],
)
def test_validate_sql_cast_type_rejects_malformed_or_compound_expressions(invalid_type: str) -> None:
    """validate_sql_cast_type rejects empty, comment-bearing, unbalanced, or multi-clause cast strings."""
    with pytest.raises(ValueError, match="Invalid SQL cast type"):
        validate_sql_cast_type(invalid_type, dialect="postgres")


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
async def test_multi_shard_storage_ingest_skips_empty_shards_quotes_delete_and_validates_columns(
    tmp_path: Path, asynchronous: bool
) -> None:
    """Multi-shard ingestion skips leading 0-row shards, records checkpoints, quotes DELETE FROM, and checks column parity."""
    import pyarrow.parquet as pq

    empty_shard = tmp_path / "00_empty.parquet"
    valid_shard_1 = tmp_path / "01_part.parquet"
    valid_shard_2 = tmp_path / "02_part.parquet"
    drift_shard = tmp_path / "03_drift.parquet"

    pq.write_table(pa.table({"orphan_col": pa.array([], type=pa.int64())}), empty_shard)
    pq.write_table(pa.table({"id": [1, 2], "label": ["a", "b"]}), valid_shard_1)
    pq.write_table(pa.table({"id": [2, 3], "label": ["b_new", "c"]}), valid_shard_2)
    pq.write_table(pa.table({"id": [4], "extra": ["bad"]}), drift_shard)

    db_file = tmp_path / f"shards_{'async' if asynchronous else 'sync'}.db"
    if asynchronous:
        aio_cfg = AiosqliteConfig(connection_config={"database": str(db_file)})
        try:
            async with aio_cfg.provide_session() as aio_drv:
                await aio_drv.execute('CREATE TABLE "order items" (id INTEGER PRIMARY KEY, label TEXT)')
                await aio_drv.execute("INSERT INTO \"order items\" (id, label) VALUES (99, 'stale')")
                job = await aio_drv.load_from_storage(
                    '"order items"',
                    [empty_shard, valid_shard_1],
                    file_format="parquet",
                    overwrite=True,
                    stage_table=True,
                    checkpoint_table="ingest_ckpt",
                )
                assert job.telemetry["rows_processed"] == 2
                rows = await aio_drv.select('SELECT id, label FROM "order items" ORDER BY id')
                assert rows == [{"id": 1, "label": "a"}, {"id": 2, "label": "b"}]
                ckpt = await aio_drv.select("SELECT shard_uri, rows_loaded FROM ingest_ckpt ORDER BY shard_uri")
                assert len(ckpt) == 2
                assert ckpt[0]["rows_loaded"] == 0

                with pytest.raises(SQLSpecError, match="do not match expected columns"):
                    await aio_drv.load_from_storage(
                        '"order items"',
                        [valid_shard_2, drift_shard],
                        file_format="parquet",
                        stage_table=True,
                        conflict_keys=["id"],
                    )
        finally:
            await aio_cfg.close_pool()
    else:
        sync_cfg = SqliteConfig(connection_config={"database": str(db_file)})
        try:
            with sync_cfg.provide_session() as sync_drv:
                sync_drv.execute('CREATE TABLE "order items" (id INTEGER PRIMARY KEY, label TEXT)')
                sync_drv.execute("INSERT INTO \"order items\" (id, label) VALUES (99, 'stale')")
                job = sync_drv.load_from_storage(
                    '"order items"',
                    [empty_shard, valid_shard_1],
                    file_format="parquet",
                    overwrite=True,
                    stage_table=True,
                    checkpoint_table="ingest_ckpt",
                )
                assert job.telemetry["rows_processed"] == 2
                rows = sync_drv.select('SELECT id, label FROM "order items" ORDER BY id')
                assert rows == [{"id": 1, "label": "a"}, {"id": 2, "label": "b"}]
                ckpt = sync_drv.select("SELECT shard_uri, rows_loaded FROM ingest_ckpt ORDER BY shard_uri")
                assert len(ckpt) == 2
                assert ckpt[0]["rows_loaded"] == 0

                with pytest.raises(SQLSpecError, match="do not match expected columns"):
                    sync_drv.load_from_storage(
                        '"order items"',
                        [valid_shard_2, drift_shard],
                        file_format="parquet",
                        stage_table=True,
                        conflict_keys=["id"],
                    )
        finally:
            sync_cfg.close_pool()
