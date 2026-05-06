"""Cross-adapter storage bridge contract tests."""

import inspect
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.psqlpy import PsqlpyConfig
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.storage.registry import storage_registry
from sqlspec.typing import FSSPEC_INSTALLED, PYARROW_INSTALLED
from tests.integration.adapters._storage_bridge_helpers import register_minio_alias
from tests.integration.adapters.contracts._mysql_async import (
    MYSQL_ASYNC_ADAPTERS,
    close_mysql_async_config,
    mysql_async_config,
)

LOCAL_LOAD_ADAPTERS = [
    pytest.param("sqlite", marks=pytest.mark.xdist_group("sqlite"), id="sqlite"),
    pytest.param("aiosqlite", marks=pytest.mark.xdist_group("sqlite"), id="aiosqlite"),
    *MYSQL_ASYNC_ADAPTERS,
]

ROUND_TRIP_ADAPTERS = [
    pytest.param("adbc-postgres", marks=[pytest.mark.postgres, pytest.mark.xdist_group("storage")], id="adbc-postgres"),
    pytest.param(
        "asyncpg",
        marks=[
            pytest.mark.asyncpg,
            pytest.mark.xdist_group("postgres"),
            pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec not installed"),
            pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed"),
        ],
        id="asyncpg",
    ),
    pytest.param(
        "duckdb",
        marks=[
            pytest.mark.xdist_group("storage"),
            pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec not installed"),
            pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed"),
        ],
        id="duckdb",
    ),
    pytest.param(
        "psqlpy",
        marks=[
            pytest.mark.xdist_group("postgres"),
            pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec not installed"),
            pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed"),
        ],
        id="psqlpy",
    ),
    pytest.param(
        "psycopg-sync",
        marks=[
            pytest.mark.xdist_group("postgres"),
            pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec not installed"),
            pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed"),
        ],
        id="psycopg-sync",
    ),
    pytest.param(
        "psycopg-async",
        marks=[
            pytest.mark.xdist_group("postgres"),
            pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec not installed"),
            pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed"),
        ],
        id="psycopg-async",
    ),
]


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _close_config(config: Any) -> None:
    close_pool = getattr(config, "close_pool", None)
    if close_pool is not None:
        await _maybe_await(close_pool())
    elif (connection := getattr(config, "connection_instance", None)) is not None:
        close = getattr(connection, "close", None)
        if close is not None:
            await _maybe_await(close())

    if hasattr(config, "connection_instance"):
        config.connection_instance = None


def _postgres_conninfo(request: pytest.FixtureRequest) -> str:
    postgres_service = request.getfixturevalue("postgres_service")
    return (
        f"postgresql://{postgres_service.user}:{postgres_service.password}@"
        f"{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
    )


def _postgres_dsn(request: pytest.FixtureRequest) -> str:
    postgres_service = request.getfixturevalue("postgres_service")
    return (
        f"postgres://{postgres_service.user}:{postgres_service.password}@"
        f"{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
    )


def _make_local_load_config(adapter: str, request: pytest.FixtureRequest) -> Any:
    if adapter == "sqlite":
        return SqliteConfig(connection_config={"database": ":memory:"})
    if adapter == "aiosqlite":
        return AiosqliteConfig()

    mysql_service = request.getfixturevalue("mysql_service")
    return mysql_async_config(adapter, mysql_service)


def _make_round_trip_config(adapter: str, request: pytest.FixtureRequest) -> Any:
    if adapter == "adbc-postgres":
        return AdbcConfig(connection_config={"uri": _postgres_dsn(request), "driver_name": "adbc_driver_postgresql"})
    if adapter == "asyncpg":
        postgres_service = request.getfixturevalue("postgres_service")
        return AsyncpgConfig(
            connection_config={
                "host": postgres_service.host,
                "port": postgres_service.port,
                "user": postgres_service.user,
                "password": postgres_service.password,
                "database": postgres_service.database,
            }
        )
    if adapter == "duckdb":
        return DuckDBConfig(connection_config={"database": ":memory:"})
    if adapter == "psqlpy":
        return PsqlpyConfig(connection_config={"dsn": _postgres_dsn(request), "max_db_pool_size": 5})
    if adapter == "psycopg-sync":
        return PsycopgSyncConfig(connection_config={"conninfo": _postgres_conninfo(request)})
    return PsycopgAsyncConfig(connection_config={"conninfo": _postgres_conninfo(request)}, pool_config={"min_size": 1})


def _is_async_adapter(adapter: str) -> bool:
    return adapter in {"aiosqlite", "aiomysql", "asyncmy", "asyncpg", "psqlpy", "psycopg-async"}


@asynccontextmanager
async def _provide_driver(adapter: str, config: Any) -> AsyncGenerator[Any, None]:
    try:
        if _is_async_adapter(adapter):
            async with config.provide_session() as driver:
                yield driver
        else:
            with config.provide_session() as driver:
                yield driver
    finally:
        if adapter in {"aiomysql", "asyncmy"}:
            await close_mysql_async_config(config)
        else:
            await _close_config(config)


async def _execute(driver: Any, statement: str, *parameters: Any) -> Any:
    result = driver.execute(statement, *parameters) if parameters else driver.execute(statement)
    return await _maybe_await(result)


async def _commit(driver: Any) -> None:
    commit = getattr(driver, "commit", None)
    if commit is not None:
        await _maybe_await(commit())


async def _rows(driver: Any, table_name: str) -> list[dict[str, Any]]:
    result = await _execute(driver, f"SELECT id, label FROM {table_name} ORDER BY id")
    if hasattr(result, "get_data"):
        return list(result.get_data())
    return list(result)


async def _drop_table(driver: Any, table_name: str, *, cascade: bool = False) -> None:
    suffix = " CASCADE" if cascade else ""
    await _execute(driver, f"DROP TABLE IF EXISTS {table_name}{suffix}")
    await _commit(driver)


async def _create_table(driver: Any, table_name: str) -> None:
    await _execute(driver, f"CREATE TABLE {table_name} (id INT PRIMARY KEY, label TEXT NOT NULL)")
    await _commit(driver)


async def _seed_table(driver: Any, table_name: str, labels: tuple[str, str, str]) -> None:
    for row_id, label in enumerate(labels, start=1):
        await _execute(driver, f"INSERT INTO {table_name} (id, label) VALUES ({row_id}, '{label}')")
    await _commit(driver)


@pytest.mark.parametrize("adapter", LOCAL_LOAD_ADAPTERS)
async def test_storage_bridge_load_from_arrow(adapter: str, request: pytest.FixtureRequest) -> None:
    """Adapters with direct Arrow loading support load Arrow tables into SQL tables."""
    table_name = f"storage_bridge_load_arrow_{adapter.replace('-', '_')}"
    arrow_table = pa.table({"id": [1, 2], "label": ["alpha", "beta"]})
    config = _make_local_load_config(adapter, request)

    async with _provide_driver(adapter, config) as driver:
        await _drop_table(driver, table_name)
        await _create_table(driver, table_name)

        job = await _maybe_await(driver.load_from_arrow(table_name, arrow_table, overwrite=True))

        assert job.telemetry["rows_processed"] == arrow_table.num_rows
        assert job.telemetry["destination"] == table_name
        assert await _rows(driver, table_name) == [{"id": 1, "label": "alpha"}, {"id": 2, "label": "beta"}]

        await _drop_table(driver, table_name)


@pytest.mark.parametrize("adapter", LOCAL_LOAD_ADAPTERS)
async def test_storage_bridge_load_from_storage(tmp_path: Path, adapter: str, request: pytest.FixtureRequest) -> None:
    """Adapters with direct storage loading support load Parquet files into SQL tables."""
    table_name = f"storage_bridge_load_storage_{adapter.replace('-', '_')}"
    filename = f"{adapter}-bridge.parquet"
    arrow_table = pa.table({"id": [3, 4], "label": ["gamma", "delta"]})
    destination = tmp_path / filename
    pq.write_table(arrow_table, destination)
    config = _make_local_load_config(adapter, request)

    async with _provide_driver(adapter, config) as driver:
        await _drop_table(driver, table_name)
        await _create_table(driver, table_name)

        job = await _maybe_await(
            driver.load_from_storage(table_name, str(destination), file_format="parquet", overwrite=True)
        )

        assert job.telemetry["extra"]["source"]["destination"].endswith(filename)  # type: ignore[index]
        assert job.telemetry["extra"]["source"]["backend"]  # type: ignore[index]
        assert await _rows(driver, table_name) == [{"id": 3, "label": "gamma"}, {"id": 4, "label": "delta"}]

        await _drop_table(driver, table_name)


@pytest.mark.parametrize("adapter", ROUND_TRIP_ADAPTERS)
async def test_storage_bridge_select_to_storage_round_trip(
    tmp_path: Path,
    adapter: str,
    request: pytest.FixtureRequest,
    minio_service: Any,
    minio_client: Any,
    minio_default_bucket_name: str,
) -> None:
    """Adapters with export support round-trip SQL rows through registered storage backends."""
    labels = ("north", "south", "east")
    source_table = f"storage_bridge_{adapter.replace('-', '_')}_source"
    target_table = f"storage_bridge_{adapter.replace('-', '_')}_target"
    alias = f"storage_bridge_{adapter.replace('-', '_')}"
    export_name = f"{adapter.replace('-', '_')}.parquet"
    cascade = adapter in {"asyncpg", "psycopg-sync", "psycopg-async"}
    config = _make_round_trip_config(adapter, request)

    storage_registry.clear()
    try:
        if adapter == "adbc-postgres":
            storage_registry.register_alias(alias, f"file://{tmp_path}", backend="local")
            destination = f"alias://{alias}/{export_name}"
            expected_local_path = tmp_path / export_name
            object_name = None
        else:
            prefix = register_minio_alias(alias, minio_service, minio_default_bucket_name)
            destination = f"alias://{alias}/{adapter}/export.parquet"
            expected_local_path = None
            object_name = f"{prefix}/{adapter}/export.parquet"

        async with _provide_driver(adapter, config) as driver:
            await _drop_table(driver, target_table, cascade=cascade)
            await _drop_table(driver, source_table, cascade=cascade)
            await _create_table(driver, source_table)
            await _seed_table(driver, source_table, labels)

            export_job = await _maybe_await(
                driver.select_to_storage(
                    f"SELECT id, label FROM {source_table} ORDER BY id", destination, format_hint="parquet"
                )
            )
            assert export_job.telemetry["rows_processed"] == 3

            await _create_table(driver, target_table)
            load_job = await _maybe_await(
                driver.load_from_storage(target_table, destination, file_format="parquet", overwrite=True)
            )
            assert load_job.telemetry["rows_processed"] == 3

            assert await _rows(driver, target_table) == [
                {"id": 1, "label": "north"},
                {"id": 2, "label": "south"},
                {"id": 3, "label": "east"},
            ]

            await _drop_table(driver, target_table, cascade=cascade)
            await _drop_table(driver, source_table, cascade=cascade)

        if expected_local_path is not None:
            assert expected_local_path.exists()
        else:
            stat = minio_client.stat_object(bucket_name=minio_default_bucket_name, object_name=object_name)
            object_size = stat.size if stat.size is not None else 0
            assert object_size > 0
    finally:
        storage_registry.clear()
