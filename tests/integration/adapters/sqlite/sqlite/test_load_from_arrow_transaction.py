"""SQLite load_from_arrow transactional atomicity."""

import pyarrow as pa
import pytest

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import SQLSpecError


def test_load_from_arrow_rolls_back_on_failure() -> None:
    config = SqliteConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as driver:
            driver.execute("CREATE TABLE atomic_t (id INTEGER PRIMARY KEY, v TEXT)")
            driver.execute("INSERT INTO atomic_t (id, v) VALUES (1, 'seed')")
            driver.commit()

            arrow_table = pa.table({"id": [2, 1, 3], "v": ["a", "dup", "c"]})
            with pytest.raises(SQLSpecError):
                driver.load_from_arrow("atomic_t", arrow_table)

            count = driver.execute("SELECT COUNT(*) AS c FROM atomic_t").get_data()[0]["c"]
            assert count == 1
    finally:
        config.close_pool()


def test_load_from_arrow_overwrite_rolls_back_delete_on_failure() -> None:
    config = SqliteConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as driver:
            driver.execute("CREATE TABLE atomic_ov (id INTEGER PRIMARY KEY, v TEXT)")
            driver.execute("INSERT INTO atomic_ov (id, v) VALUES (1, 'seed')")
            driver.commit()

            arrow_table = pa.table({"id": [2, 2], "v": ["a", "b"]})
            with pytest.raises(SQLSpecError):
                driver.load_from_arrow("atomic_ov", arrow_table, overwrite=True)

            rows = driver.execute("SELECT id, v FROM atomic_ov").get_data()
            assert rows == [{"id": 1, "v": "seed"}]
    finally:
        config.close_pool()


def test_load_from_arrow_respects_caller_transaction() -> None:
    config = SqliteConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as driver:
            driver.execute("CREATE TABLE caller_txn (id INTEGER PRIMARY KEY, v TEXT)")
            driver.commit()

            driver.begin()
            driver.load_from_arrow("caller_txn", pa.table({"id": [1, 2], "v": ["a", "b"]}))
            driver.rollback()

            count = driver.execute("SELECT COUNT(*) AS c FROM caller_txn").get_data()[0]["c"]
            assert count == 0
    finally:
        config.close_pool()
