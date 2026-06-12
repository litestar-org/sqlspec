import inspect
import sqlite3
import sys
from typing import Any, cast

import pytest

from sqlspec.adapters.sqlite.driver import SqliteDriver
from sqlspec.exceptions import ImproperConfigurationError, SQLSpecError


def test_driver_cache_stmt_cache_execute_direct_has_no_unreachable_returns_rows_guard() -> None:
    source = inspect.getsource(SqliteDriver._stmt_cache_execute_direct)
    assert "if returns_rows:" not in source


def test_pool_no_duplicate_typedef_sqlite_connection_params_not_exported_from_pool() -> None:
    import sqlspec.adapters.sqlite.pool as pool_mod

    assert not hasattr(pool_mod, "SqliteConnectionParams")


def test_pool_no_duplicate_typedef_sqlite_connection_pool_still_importable() -> None:
    from sqlspec.adapters.sqlite.pool import SqliteConnectionPool

    assert SqliteConnectionPool is not None


def test_pool_no_duplicate_typedef_pool_module_all_unchanged() -> None:
    import sqlspec.adapters.sqlite.pool as pool_mod

    assert pool_mod.__all__ == ("SqliteConnectionPool",)


def test_pool_no_duplicate_typedef_canonical_typedef_still_importable_from_config() -> None:
    from sqlspec.adapters.sqlite.config import SqliteConnectionParams

    assert hasattr(SqliteConnectionParams, "__annotations__") or hasattr(SqliteConnectionParams, "__required_keys__")


def test_pool_no_duplicate_typedef_canonical_typedef_importable_from_package() -> None:
    from sqlspec.adapters.sqlite import SqliteConnectionParams

    assert SqliteConnectionParams is not None


def test_pool_no_duplicate_typedef_pool_creates_connection_after_cleanup() -> None:
    from sqlspec.adapters.sqlite.pool import SqliteConnectionPool

    pool = SqliteConnectionPool(connection_parameters={"database": ":memory:"})
    conn = pool.acquire()
    cursor = conn.execute("SELECT 1 AS n")
    row = cursor.fetchone()
    pool.close()
    assert row is not None
    assert row[0] == 1


@pytest.mark.skipif(sys.version_info >= (3, 11), reason="gate only raises below 3.11")
def test_serialize_gate_below_311() -> None:
    driver = SqliteDriver(sqlite3.connect(":memory:"))

    try:
        with pytest.raises(ImproperConfigurationError, match=r"3\.11"):
            cast(Any, driver).serialize()
    finally:
        driver.connection.close()


@pytest.mark.skipif(sys.version_info >= (3, 11), reason="gate only raises below 3.11")
def test_deserialize_gate_below_311() -> None:
    driver = SqliteDriver(sqlite3.connect(":memory:"))

    try:
        with pytest.raises(ImproperConfigurationError, match=r"3\.11"):
            cast(Any, driver).deserialize(b"")
    finally:
        driver.connection.close()


@pytest.mark.skipif(sys.version_info >= (3, 11), reason="gate only raises below 3.11")
def test_blob_open_gate_below_311() -> None:
    driver = SqliteDriver(sqlite3.connect(":memory:"))

    try:
        with pytest.raises(ImproperConfigurationError, match=r"3\.11"):
            cast(Any, driver).blob_open("t", "c", 1)
    finally:
        driver.connection.close()


@pytest.mark.skipif(sys.version_info >= (3, 13), reason="gate only raises below 3.13")
def test_iterdump_filter_gate_below_313() -> None:
    driver = SqliteDriver(sqlite3.connect(":memory:"))

    try:
        with pytest.raises(ImproperConfigurationError, match=r"3\.13"):
            cast(Any, driver).iterdump(filter_pattern="t%")
    finally:
        driver.connection.close()


def test_wal_checkpoint_invalid_mode_raises() -> None:
    driver = SqliteDriver(sqlite3.connect(":memory:"))

    try:
        with pytest.raises(SQLSpecError, match="Invalid WAL checkpoint mode"):
            cast(Any, driver).wal_checkpoint(cast(Any, "BOGUS"))
    finally:
        driver.connection.close()
