"""SQLite end-to-end coverage for the generic load_from_records method."""

import pytest

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import ImproperConfigurationError


def test_load_from_records_dict_and_positional() -> None:
    config = SqliteConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as driver:
            driver.execute("CREATE TABLE rec (id INTEGER, name TEXT)")
            driver.commit()

            job = driver.load_from_records("rec", [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
            assert job.telemetry["rows_processed"] == 2
            assert driver.select_value("SELECT COUNT(*) FROM rec") == 2

            driver.load_from_records("rec", [(3, "c"), (4, "d")], columns=["id", "name"])
            assert driver.select_value("SELECT COUNT(*) FROM rec") == 4

            rows = driver.select("SELECT id, name FROM rec ORDER BY id")
            assert rows[2] == {"id": 3, "name": "c"}
    finally:
        config.close_pool()


def test_load_from_records_empty_raises() -> None:
    config = SqliteConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as driver:
            driver.execute("CREATE TABLE rec (id INTEGER, name TEXT)")
            with pytest.raises(ImproperConfigurationError):
                driver.load_from_records("rec", [])
    finally:
        config.close_pool()
