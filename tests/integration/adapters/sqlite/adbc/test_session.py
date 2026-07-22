"""ADBC SQLite integration smoke (regression for #472).

Two upstream bugs are exercised here:

1. ``detect_dialect`` previously ignored ``connection_config["driver_name"]``
   and defaulted to ``postgres``; the user got a misleading warning and a
   wrong fallback when ``adbc_get_info()`` did not yield a pattern match.
2. ``AdbcDriver.begin()`` ran ``cursor.execute("BEGIN")`` unconditionally;
   ADBC SQLite holds an implicit transaction and rejected the explicit
   ``BEGIN`` with "cannot start a transaction within a transaction."
"""

from typing import TYPE_CHECKING

import pytest

from sqlspec.adapters.adbc import AdbcConfig

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.adbc


def test_adbc_sqlite_dialect_and_begin(tmp_path: "Path") -> None:
    db = tmp_path / "demo.db"
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": str(db)})

    with config.provide_session() as driver:
        assert driver._dialect_name == "sqlite"
        driver.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)")
        driver.begin()
        driver.execute("INSERT INTO t (body) VALUES (?)", ("hello",))
        driver.commit()
        row = driver.execute("SELECT body FROM t").one()
        assert row["body"] == "hello"
