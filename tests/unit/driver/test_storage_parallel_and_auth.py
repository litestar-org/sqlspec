"""Unit tests for parallel storage ingestion, checkpointing, thread budgets, and dynamic auth providers."""

import os
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlspec.adapters.adbc.config import AdbcConfig
from sqlspec.adapters.adbc.core import build_connection_config as build_adbc_connection_config
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.adapters.duckdb.core import _build_storage_read_sql, _s3_storage_options_match
from sqlspec.adapters.duckdb.pool import DuckDBConnectionPool
from sqlspec.adapters.psqlpy.config import PsqlpyConfig
from sqlspec.adapters.psqlpy.core import build_connection_config as build_psqlpy_connection_config
from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.driver._storage_helpers import (
    build_checkpoint_insert_sql,
    build_checkpoint_select_sql,
    build_checkpoint_table_ddl,
    compute_worker_thread_budget,
    init_spawn_worker_thread_budget,
)


def test_compute_worker_thread_budget_divides_cores_fairly() -> None:
    """Thread budget divides total cores across workers with a floor of 1."""
    budget4 = compute_worker_thread_budget(4, cpu_count=16)
    assert budget4["OMP_NUM_THREADS"] == "4"
    assert budget4["RAYON_NUM_THREADS"] == "4"

    budget8 = compute_worker_thread_budget(8, cpu_count=4)
    assert budget8["OMP_NUM_THREADS"] == "1"

    budget0 = compute_worker_thread_budget(0, cpu_count=8)
    assert budget0["OMP_NUM_THREADS"] == "8"


def test_init_spawn_worker_thread_budget_sets_thread_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    """Spawn worker initializer sets all native thread pool environment variables."""
    expected_keys = compute_worker_thread_budget(1, cpu_count=1).keys()
    for var in expected_keys:
        monkeypatch.delenv(var, raising=False)

    init_spawn_worker_thread_budget(4, cpu_count=12)
    for var in expected_keys:
        assert os.environ.get(var) == "3"


def test_checkpoint_sql_builders_generate_dialect_valid_sql() -> None:
    """Checkpoint DDL, SELECT, and INSERT builders emit valid SQL across dialects."""
    ddl_sql = build_checkpoint_table_ddl("sqlite", "_ingest_ckpt")
    assert "CREATE TABLE IF NOT EXISTS" in ddl_sql
    assert "phase" in ddl_sql
    assert "shard_uri" in ddl_sql

    select_sql = build_checkpoint_select_sql("_ingest_ckpt")
    assert "SELECT shard_uri FROM _ingest_ckpt" in select_sql

    insert_sql = build_checkpoint_insert_sql("sqlite", "_ingest_ckpt")
    assert "INSERT INTO _ingest_ckpt" in insert_sql
    assert "ON CONFLICT" in insert_sql


def test_sync_load_from_storage_multi_uri_checkpoint_and_schema(tmp_path: Path) -> None:
    """Sync load_from_storage ingests multiple URIs concurrently and skips checkpointed URIs on rerun."""
    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    file1 = tmp_path / "part1.parquet"
    file2 = tmp_path / "part2.parquet"
    pq.write_table(pa.Table.from_pydict({"id": [1, 2], "name": ["a", "b"]}, schema=schema), file1)
    pq.write_table(pa.Table.from_pydict({"id": [3, 4], "name": ["c", "d"]}, schema=schema), file2)

    db_path = tmp_path / "checkpoint_sync.db"
    config = SqliteConfig(connection_config={"database": str(db_path)})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        job1 = driver.load_from_storage(
            "items",
            [str(file1), str(file2)],
            file_format="parquet",
            max_workers=2,
            checkpoint_table="_load_checkpoints",
            checkpoint_phase="daily_sync",
            arrow_schema=schema,
        )
        assert job1.telemetry["rows_processed"] == 4
        assert job1.telemetry["shards_processed"] == 2
        assert job1.telemetry["shards_skipped"] == 0
        rows = driver.select("SELECT id, name FROM items ORDER BY id")
        assert len(rows) == 4

        job2 = driver.load_from_storage(
            "items",
            [str(file1), str(file2)],
            file_format="parquet",
            max_workers=2,
            checkpoint_table="_load_checkpoints",
            checkpoint_phase="daily_sync",
            arrow_schema=schema,
        )
        assert job2.telemetry["rows_processed"] == 0
        assert job2.telemetry["shards_skipped"] == 2


@pytest.mark.anyio
async def test_async_load_from_storage_multi_uri_checkpoint_and_schema(tmp_path: Path) -> None:
    """Async load_from_storage ingests multiple URIs with bounded concurrency and checkpointing."""
    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    file1 = tmp_path / "async_part1.parquet"
    file2 = tmp_path / "async_part2.parquet"
    pq.write_table(pa.Table.from_pydict({"id": [10, 20], "name": ["x", "y"]}, schema=schema), file1)
    pq.write_table(pa.Table.from_pydict({"id": [30, 40], "name": ["z", "w"]}, schema=schema), file2)

    db_path = tmp_path / "checkpoint_async.db"
    config = AiosqliteConfig(connection_config={"database": str(db_path)})
    try:
        async with config.provide_session() as driver:
            await driver.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
            job1 = await driver.load_from_storage(
                "items",
                [str(file1), str(file2)],
                file_format="parquet",
                max_workers=2,
                checkpoint_table="_load_checkpoints",
                checkpoint_phase="async_daily",
                arrow_schema=schema,
            )
            assert job1.telemetry["rows_processed"] == 4
            assert job1.telemetry["shards_processed"] == 2

            job2 = await driver.load_from_storage(
                "items",
                [str(file1), str(file2)],
                file_format="parquet",
                max_workers=2,
                checkpoint_table="_load_checkpoints",
                checkpoint_phase="async_daily",
                arrow_schema=schema,
            )
            assert job2.telemetry["rows_processed"] == 0
            assert job2.telemetry["shards_skipped"] == 2
    finally:
        await config.close_pool()


def test_psycopg_sync_auth_token_provider_injects_token_and_shortens_lifetime() -> None:
    """PsycopgSyncConfig invokes auth_token_provider on connect and defaults max_lifetime to 1800s."""
    tokens = iter(["token-1", "token-2"])
    config = PsycopgSyncConfig(
        connection_config={
            "host": "localhost",
            "user": "iam_user",
            "dbname": "analytics",
            "auth_token_provider": lambda: next(tokens),
        }
    )
    _, _, kwargs = config._connection_kwargs()
    assert kwargs["password"] == "token-1"

    captured_pool_kwargs: dict[str, Any] = {}

    def fake_pool(conninfo: str, **pool_kwargs: Any) -> MagicMock:
        captured_pool_kwargs["conninfo"] = conninfo
        captured_pool_kwargs.update(pool_kwargs)
        return MagicMock()

    with patch("sqlspec.adapters.psycopg.config.ConnectionPool", side_effect=fake_pool):
        config._create_pool()

    assert captured_pool_kwargs["max_lifetime"] == 1800.0
    assert "auth_token_provider" not in captured_pool_kwargs["kwargs"]


@pytest.mark.anyio
async def test_psycopg_async_and_asyncpg_and_psqlpy_auth_token_provider() -> None:
    """Async PostgreSQL configs support sync and async auth_token_provider callables."""

    async def provide_token() -> str:
        return "async-iam-token"

    psycopg_async = PsycopgAsyncConfig(
        connection_config={
            "host": "localhost",
            "user": "iam_user",
            "dbname": "analytics",
            "open": False,
            "auth_token_provider": provide_token,
        }
    )
    captured_psycopg: dict[str, Any] = {}

    def fake_async_pool(conninfo: str, **pool_kwargs: Any) -> MagicMock:
        captured_psycopg.update(pool_kwargs)
        return MagicMock()

    with patch("sqlspec.adapters.psycopg.config.AsyncConnectionPool", side_effect=fake_async_pool):
        await psycopg_async._create_pool()
    assert captured_psycopg["max_lifetime"] == 1800.0

    asyncpg_cfg = AsyncpgConfig(
        connection_config={
            "host": "localhost",
            "user": "iam_user",
            "database": "analytics",
            "auth_token_provider": provide_token,
        }
    )
    captured_asyncpg: dict[str, Any] = {}

    async def fake_asyncpg_pool(**pool_kwargs: Any) -> MagicMock:
        captured_asyncpg.update(pool_kwargs)
        return MagicMock()

    with patch("sqlspec.adapters.asyncpg.config.asyncpg_create_pool", side_effect=fake_asyncpg_pool):
        await asyncpg_cfg._create_pool()
    assert captured_asyncpg["password"] is provide_token
    assert captured_asyncpg["max_inactive_connection_lifetime"] == 1800.0

    psqlpy_cfg = PsqlpyConfig(
        connection_config={
            "host": "localhost",
            "username": "iam_user",
            "db_name": "analytics",
            "auth_token_provider": provide_token,
        }
    )
    captured_psqlpy: dict[str, Any] = {}

    def fake_psqlpy_pool(**pool_kwargs: Any) -> MagicMock:
        captured_psqlpy.update(pool_kwargs)
        return MagicMock()

    with patch("sqlspec.adapters.psqlpy.config.ConnectionPool", side_effect=fake_psqlpy_pool):
        await psqlpy_cfg._create_pool()
    assert captured_psqlpy["password"] == "async-iam-token"
    assert (
        build_psqlpy_connection_config({"username": "u", "auth_token_provider": lambda: "sync-tok"})["password"]
        == "sync-tok"
    )


def test_adbc_auth_token_provider_resolves_fresh_credentials() -> None:
    """ADBC build_connection_config invokes auth_token_provider on each call."""
    counter = 0

    def next_token() -> str:
        nonlocal counter
        counter += 1
        return f"adbc-token-{counter}"

    cfg1 = build_adbc_connection_config({
        "driver": "postgresql",
        "uri": "postgresql://localhost/db",
        "auth_token_provider": next_token,
    })
    cfg2 = build_adbc_connection_config({
        "driver": "flightsql",
        "uri": "grpc+tls://localhost:31337",
        "auth_token_provider": next_token,
    })
    assert cfg1["password"] == "adbc-token-1"
    assert cfg2.get("db_kwargs") is not None
    _ = AdbcConfig(connection_config={"driver": "sqlite", "uri": ":memory:", "auth_token_provider": next_token})


def test_duckdb_secret_value_provider_and_gcs_bearer_token_and_globs() -> None:
    """DuckDB secret value_provider refreshes secrets and supports GCS bearer_token and native globs."""
    token_seq = iter(["gcs-oauth-1", "gcs-oauth-2"])
    pool = DuckDBConnectionPool(
        {"database": ":memory:"},
        secrets=[
            {
                "name": "gcs_dynamic",
                "secret_type": "http",
                "value_provider": lambda: {"bearer_token": next(token_seq)},
                "refresh_interval_seconds": 0.0,
            }
        ],
    )
    try:
        conn = pool.acquire()
        secrets_initial = pool._storage_settings(conn)["_duckdb_storage_secrets"]
        assert secrets_initial[0]["value"]["bearer_token"] == "gcs-oauth-1"

        conn = pool.acquire()
        secrets_refreshed = pool._storage_settings(conn)["_duckdb_storage_secrets"]
        assert secrets_refreshed[0]["value"]["bearer_token"] == "gcs-oauth-2"
    finally:
        pool.close()

    assert _s3_storage_options_match({"google_bearer_token": "tok-123"}, {"bearer_token": "tok-123"}, "gcs")
    assert _build_storage_read_sql("items", "gs://bucket/path/*.parquet", "parquet", allow_globs=False) is None
    parquet_glob_sql = _build_storage_read_sql("items", "gs://bucket/path/*.parquet", "parquet", allow_globs=True)
    csv_glob_sql = _build_storage_read_sql("items", "gs://bucket/path/*.csv", "csv", allow_globs=True)
    jsonl_glob_sql = _build_storage_read_sql("items", "gs://bucket/path/*.jsonl", "jsonl", allow_globs=True)
    assert parquet_glob_sql is not None and "read_parquet" in parquet_glob_sql.lower()
    assert csv_glob_sql is not None and "read_csv" in csv_glob_sql.lower()
    assert jsonl_glob_sql is not None and "read_json" in jsonl_glob_sql.lower()
