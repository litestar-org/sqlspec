"""Unit tests for staged bulk load, dialect-aware upsert/merge, type casts, and load_from_records."""

from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal, cast
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.spanner._typing import SpannerTransaction
from sqlspec.adapters.spanner.driver import SpannerSyncDriver
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sybase import SybaseDriver
from sqlspec.adapters.sybase._typing import SybaseConnection
from sqlspec.driver._storage_helpers import (
    build_drop_stage_table_sql,
    build_stage_merge_sql,
    build_stage_table_ddl,
    execute_load_from_storage_async,
    execute_load_from_storage_sync,
    execute_staged_load_async,
    execute_staged_load_sync,
    resolve_stage_table_name,
)
from sqlspec.storage import StorageBridgeJob, SyncStoragePipeline, create_storage_bridge_job


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
        assert "CAST(metadata AS JSONB)" in sql

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
        "UPDATE audit.items SET value = CAST(stg.value AS INTEGER) "
        "FROM audit.items tgt, audit.staged stg "
        "WHERE tgt.tenant_id = stg.tenant_id AND tgt.id = stg.id; "
        "INSERT INTO audit.items (tenant_id, id, value, created_at) "
        "SELECT stg.tenant_id, stg.id, CAST(stg.value AS INTEGER), stg.created_at FROM audit.staged stg "
        "WHERE NOT EXISTS (SELECT 1 FROM audit.items tgt "
        "WHERE tgt.tenant_id = stg.tenant_id AND tgt.id = stg.id)"
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
        "INSERT INTO items (id, value) SELECT stg.id, stg.value FROM staged stg "
        "WHERE NOT EXISTS (SELECT 1 FROM items tgt WHERE tgt.id = stg.id)"
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
        "MERGE INTO items tgt USING staged stg ON (tgt.external_key = stg.external_key)"
        " WHEN MATCHED THEN UPDATE SET tgt.value = stg.value"
        " WHEN NOT MATCHED THEN INSERT (id, external_key, value) VALUES (stg.id, stg.external_key, stg.value)"
    )


class _SapStorageRecorder:
    dialect = "sybase"

    def __init__(self, *, fail_merge: bool = False) -> None:
        self.statements: list[str] = []
        self.scripts: list[str] = []
        self.fail_merge = fail_merge

    def execute(self, statement: str) -> None:
        self.statements.append(statement)
        if statement.startswith(("UPDATE", "INSERT", "MERGE")):
            raise AssertionError("ASE staged upserts must execute through the script boundary")

    def execute_script(self, script: str) -> None:
        self.scripts.append(script)
        if self.fail_merge and script.startswith(("UPDATE", "INSERT")):
            raise RuntimeError("native merge failure")
        self.statements.extend(part.strip() for part in script.split(";") if part.strip())

    def load_from_arrow(self, table: str, source: pa.Table, **kwargs: Any) -> StorageBridgeJob:
        return create_storage_bridge_job(
            "completed", {"rows_processed": source.num_rows, "bytes_processed": source.nbytes}
        )

    def _storage_pipeline(self) -> SyncStoragePipeline:
        return SyncStoragePipeline()


class _AsyncSapStorageRecorder:
    dialect = "sybase"

    def __init__(self, recorder: _SapStorageRecorder) -> None:
        self.recorder = recorder

    async def execute(self, statement: str) -> None:
        self.recorder.execute(statement)

    async def execute_script(self, script: str) -> None:
        self.recorder.execute_script(script)

    async def load_from_arrow(self, table: str, source: pa.Table, **kwargs: Any) -> StorageBridgeJob:
        return self.recorder.load_from_arrow(table, source, **kwargs)

    def _storage_pipeline(self) -> SyncStoragePipeline:
        return SyncStoragePipeline()


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("from_storage", [False, True], ids=["arrow", "storage"])
@pytest.mark.parametrize("fail_merge", [False, True], ids=["success", "failure"])
async def test_sybase_staged_upserts_use_scripts_and_always_cleanup(
    tmp_path: Path, asynchronous: bool, from_storage: bool, fail_merge: bool
) -> None:
    import pyarrow.parquet as pq

    source = pa.table({"id": [1], "value": ["value"]})
    recorder = _SapStorageRecorder(fail_merge=fail_merge)
    driver = _AsyncSapStorageRecorder(recorder) if asynchronous else recorder
    source_path = tmp_path / "input.parquet"
    pq.write_table(source, source_path)

    async def run_load() -> StorageBridgeJob:
        if from_storage:
            if isinstance(driver, _AsyncSapStorageRecorder):
                return await execute_load_from_storage_async(
                    driver, "items", [source_path], file_format="parquet", stage_table="staged", conflict_keys=["id"]
                )
            return execute_load_from_storage_sync(
                driver, "items", [source_path], file_format="parquet", stage_table="staged", conflict_keys=["id"]
            )
        if isinstance(driver, _AsyncSapStorageRecorder):
            return await execute_staged_load_async(
                driver, "items", source, raw_loader=driver.load_from_arrow, stage_table="staged", conflict_keys=["id"]
            )
        return execute_staged_load_sync(
            driver, "items", source, raw_loader=driver.load_from_arrow, stage_table="staged", conflict_keys=["id"]
        )

    if fail_merge:
        with pytest.raises(RuntimeError, match="native merge failure"):
            await run_load()
    else:
        result = await run_load()
        assert result.telemetry["rows_processed"] == 1
        assert [statement.split()[0] for statement in recorder.statements] == ["SELECT", "UPDATE", "INSERT", "DROP"]
    assert len(recorder.scripts) == 2
    assert recorder.scripts[0] == "SELECT TOP 0 * INTO staged FROM items"
    assert recorder.statements[-1] == "DROP TABLE staged"


def test_sybase_driver_executes_both_staged_upsert_statements() -> None:
    statements: list[str] = []
    cursor = SimpleNamespace(
        description=None,
        rowcount=1,
        execute=lambda statement, parameters=None: statements.append(statement),
        close=lambda: None,
    )
    connection = SimpleNamespace(cursor=lambda: cursor)
    driver = SybaseDriver(cast("SybaseConnection", connection))
    result = execute_staged_load_sync(
        driver,
        "items",
        pa.table({"id": [1], "value": ["value"]}),
        raw_loader=lambda table, source, **kwargs: create_storage_bridge_job(
            "completed", {"rows_processed": source.num_rows}
        ),
        stage_table="staged",
        conflict_keys=["id"],
    )

    assert statements == [
        "SELECT TOP 0 * INTO staged FROM items",
        "UPDATE items SET value = stg.value FROM items tgt, staged stg WHERE tgt.id = stg.id",
        (
            "INSERT INTO items (id, value) SELECT stg.id, stg.value FROM staged stg "
            "WHERE NOT EXISTS (SELECT 1 FROM items tgt WHERE tgt.id = stg.id)"
        ),
        "DROP TABLE staged",
    ]
    assert result.telemetry["rows_processed"] == 1
