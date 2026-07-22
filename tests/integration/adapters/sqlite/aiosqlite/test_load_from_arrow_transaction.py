"""aiosqlite load_from_arrow transactional atomicity."""

import pyarrow as pa
import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.exceptions import SQLSpecError

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
