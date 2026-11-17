"""aiosqlite load_from_arrow transactional atomicity."""

import asyncio
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.exceptions import SQLSpecError, UniqueViolationError

pytestmark = pytest.mark.xdist_group("sqlite")


async def test_load_from_arrow_rolls_back_on_failure() -> None:
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    try:
        async with config.provide_session() as driver:
            await driver.execute("CREATE TABLE atomic_t (id INTEGER PRIMARY KEY, v TEXT)")
            await driver.execute("INSERT INTO atomic_t (id, v) VALUES (1, 'seed')")
            await driver.commit()

            arrow_table = pa.table({"id": [2, 1, 3], "v": ["a", "dup", "c"]})
            with pytest.raises(SQLSpecError):
                await driver.load_from_arrow("atomic_t", arrow_table)

            result = await driver.execute("SELECT COUNT(*) AS c FROM atomic_t")
            assert result.get_data()[0]["c"] == 1
    finally:
        await config.close_pool()


async def test_load_from_arrow_respects_caller_transaction() -> None:
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    try:
        async with config.provide_session() as driver:
            await driver.execute("CREATE TABLE caller_txn (id INTEGER PRIMARY KEY, v TEXT)")
            await driver.commit()

            await driver.begin()
            await driver.load_from_arrow("caller_txn", pa.table({"id": [1, 2], "v": ["a", "b"]}))
            await driver.rollback()

            result = await driver.execute("SELECT COUNT(*) AS c FROM caller_txn")
            assert result.get_data()[0]["c"] == 0
    finally:
        await config.close_pool()


@pytest.mark.parametrize("loader", ["arrow", "arrow_drop_indexes", "storage"])
async def test_failed_load_raises_unique_violation_and_session_stays_usable(tmp_path: Path, loader: str) -> None:
    duplicate_keys = pa.table({"id": [1, 1], "v": ["a", "b"]})
    parquet_path = tmp_path / "duplicate.parquet"
    pq.write_table(duplicate_keys, parquet_path)
    config = AiosqliteConfig(connection_config={"database": str(tmp_path / "failed_load.db")})
    try:
        async with config.provide_session() as driver:
            await driver.execute_script("CREATE TABLE failed_load (id INTEGER PRIMARY KEY, v TEXT)")
            if loader == "storage":
                load = driver.load_from_storage("failed_load", str(parquet_path), file_format="parquet")
            else:
                load = driver.load_from_arrow(
                    "failed_load", duplicate_keys, drop_indexes=loader == "arrow_drop_indexes"
                )

            with pytest.raises(UniqueViolationError):
                await asyncio.wait_for(load, timeout=10)

            assert not driver.connection.in_transaction
            assert await driver.select_value("SELECT COUNT(*) FROM failed_load") == 0
            await driver.load_from_arrow("failed_load", pa.table({"id": [1, 2], "v": ["a", "b"]}))
            assert await driver.select_value("SELECT COUNT(*) FROM failed_load") == 2
    finally:
        await config.close_pool()


async def test_cancelled_load_rolls_back_and_session_stays_usable(tmp_path: Path) -> None:
    row_count = 50_000
    loop = asyncio.get_running_loop()
    insert_started = asyncio.Event()

    def signal_first_insert(statement: str) -> None:
        if statement.startswith("INSERT") and not insert_started.is_set():
            loop.call_soon_threadsafe(insert_started.set)

    config = AiosqliteConfig(connection_config={"database": str(tmp_path / "cancelled_load.db")})
    try:
        async with config.provide_session() as driver:
            await driver.execute_script("CREATE TABLE cancelled_load (id INTEGER PRIMARY KEY, v TEXT)")
            arrow_table = pa.table({"id": list(range(row_count)), "v": ["x"] * row_count})
            await driver.connection.set_trace_callback(signal_first_insert)
            load = asyncio.ensure_future(driver.load_from_arrow("cancelled_load", arrow_table))
            await asyncio.wait_for(insert_started.wait(), timeout=10)
            load.cancel()
            await asyncio.wait({load}, timeout=10)
            await driver.connection.set_trace_callback(None)

            assert load.cancelled()
            assert not driver.connection.in_transaction
            assert await driver.select_value("SELECT COUNT(*) FROM cancelled_load") == 0
            await driver.load_from_arrow("cancelled_load", pa.table({"id": [1, 2], "v": ["a", "b"]}))
            assert await driver.select_value("SELECT COUNT(*) FROM cancelled_load") == 2
    finally:
        await config.close_pool()
