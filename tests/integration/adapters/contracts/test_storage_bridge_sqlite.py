"""SQLite-family storage bridge contract tests."""

import inspect
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig

pytestmark = [pytest.mark.xdist_group("sqlite")]


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


@pytest.mark.parametrize(
    ("adapter", "table_name", "rows", "labels"),
    [
        pytest.param("sqlite", "storage_bridge_sqlite", [1, 2], ["alpha", "beta"], id="sqlite"),
        pytest.param("aiosqlite", "storage_bridge_aiosqlite", [1, 2], ["north", "south"], id="aiosqlite"),
    ],
)
async def test_sqlite_family_load_from_arrow(adapter: str, table_name: str, rows: list[int], labels: list[str]) -> None:
    """SQLite-family drivers load Arrow tables into destination tables."""
    arrow_table = pa.table({"id": rows, "label": labels})
    config: Any = SqliteConfig(connection_config={"database": ":memory:"}) if adapter == "sqlite" else AiosqliteConfig()
    try:
        session_context = config.provide_session()
        if adapter == "sqlite":
            with session_context as driver:
                await _maybe_await(driver.execute(f"DROP TABLE IF EXISTS {table_name}"))
                await _maybe_await(driver.execute(f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, label TEXT)"))
                job = await _maybe_await(driver.load_from_arrow(table_name, arrow_table, overwrite=True))
                result = await _maybe_await(driver.execute(f"SELECT id, label FROM {table_name} ORDER BY id"))
        else:
            async with session_context as driver:
                await _maybe_await(driver.execute(f"DROP TABLE IF EXISTS {table_name}"))
                await _maybe_await(driver.execute(f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, label TEXT)"))
                job = await _maybe_await(driver.load_from_arrow(table_name, arrow_table, overwrite=True))
                result = await _maybe_await(driver.execute(f"SELECT id, label FROM {table_name} ORDER BY id"))
    finally:
        await _maybe_await(config.close_pool())

    assert job.telemetry["rows_processed"] == arrow_table.num_rows
    assert result.get_data() == [{"id": row_id, "label": label} for row_id, label in zip(rows, labels)]


@pytest.mark.parametrize(
    ("adapter", "table_name", "filename", "rows", "labels"),
    [
        pytest.param(
            "sqlite", "storage_bridge_sqlite", "sqlite-bridge.parquet", [10, 11], ["gamma", "delta"], id="sqlite"
        ),
        pytest.param(
            "aiosqlite",
            "storage_bridge_aiosqlite",
            "aiosqlite-bridge.parquet",
            [3, 4],
            ["east", "west"],
            id="aiosqlite",
        ),
    ],
)
async def test_sqlite_family_load_from_storage(
    tmp_path: Path, adapter: str, table_name: str, filename: str, rows: list[int], labels: list[str]
) -> None:
    """SQLite-family drivers load Parquet files through the storage bridge."""
    arrow_table = pa.table({"id": rows, "label": labels})
    destination = tmp_path / filename
    pq.write_table(arrow_table, destination)

    config: Any = SqliteConfig(connection_config={"database": ":memory:"}) if adapter == "sqlite" else AiosqliteConfig()
    try:
        session_context = config.provide_session()
        if adapter == "sqlite":
            with session_context as driver:
                await _maybe_await(driver.execute(f"DROP TABLE IF EXISTS {table_name}"))
                await _maybe_await(driver.execute(f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, label TEXT)"))
                job = await _maybe_await(
                    driver.load_from_storage(table_name, str(destination), file_format="parquet", overwrite=True)
                )
                result = await _maybe_await(driver.execute(f"SELECT id, label FROM {table_name} ORDER BY id"))
        else:
            async with session_context as driver:
                await _maybe_await(driver.execute(f"DROP TABLE IF EXISTS {table_name}"))
                await _maybe_await(driver.execute(f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, label TEXT)"))
                job = await _maybe_await(
                    driver.load_from_storage(table_name, str(destination), file_format="parquet", overwrite=True)
                )
                result = await _maybe_await(driver.execute(f"SELECT id, label FROM {table_name} ORDER BY id"))
    finally:
        await _maybe_await(config.close_pool())

    assert job.telemetry["extra"]["source"]["destination"].endswith(filename)  # type: ignore[index]
    assert job.telemetry["extra"]["source"]["backend"]  # type: ignore[index]
    assert result.get_data() == [{"id": row_id, "label": label} for row_id, label in zip(rows, labels)]
