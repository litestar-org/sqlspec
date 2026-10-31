"""Unit tests for staged bulk load, dialect-aware upsert/merge, type casts, and load_from_records."""

from pathlib import Path
from typing import Literal
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.spanner._typing import SpannerTransaction
from sqlspec.adapters.spanner.driver import SpannerSyncDriver
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.driver._storage_helpers import (
    build_drop_stage_table_sql,
    build_stage_merge_sql,
    build_stage_table_ddl,
    resolve_stage_table_name,
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
